from __future__ import annotations

"""
Connection pooling for AWS services.

This module provides connection pooling capabilities to improve performance
and resource utilization for AWS service clients.
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any

from .exceptions import ResourceError
from .interfaces import AuthManagerInterface, ConfigManagerInterface

logger = logging.getLogger(__name__)


class PooledConnection:
    """Wrapper for pooled connections with metadata."""

    def __init__(self, connection: Any, service_name: str):
        self.connection = connection
        self.service_name = service_name
        self.created_at = time.time()
        self.last_used = time.time()
        self.use_count = 0
        self.in_use = False

    def mark_used(self):
        """Mark connection as used."""
        self.last_used = time.time()
        self.use_count += 1
        self.in_use = True

    def mark_returned(self):
        """Mark connection as returned to pool."""
        self.in_use = False

    def is_expired(self, max_age_seconds: float = 3600) -> bool:
        """Check if connection is expired."""
        return (time.time() - self.created_at) > max_age_seconds

    def is_stale(self, max_idle_seconds: float = 300) -> bool:
        """Check if connection is stale (idle too long)."""
        return not self.in_use and (time.time() - self.last_used) > max_idle_seconds


class AWSConnectionPool:
    """Connection pool for AWS service clients."""

    def __init__(
        self,
        auth_manager: AuthManagerInterface,
        config_manager: ConfigManagerInterface,
        max_connections_per_service: int = 10,
        max_idle_time: float = 300,  # 5 minutes
        max_connection_age: float = 3600,  # 1 hour
        cleanup_interval: float = 60,  # 1 minute
    ):
        self.auth_manager = auth_manager
        self.config_manager = config_manager
        self.max_connections_per_service = max_connections_per_service
        self.max_idle_time = max_idle_time
        self.max_connection_age = max_connection_age
        self.cleanup_interval = cleanup_interval

        # Connection pools by service
        self._pools: dict[str, deque[PooledConnection]] = defaultdict(deque)
        self._active_connections: dict[str, set[PooledConnection]] = defaultdict(set)
        self._pool_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._stats = defaultdict(
            lambda: {"created": 0, "reused": 0, "expired": 0, "errors": 0, "active": 0, "idle": 0}
        )

        # Cleanup task
        self._cleanup_task: asyncio.Task | None = None
        self._closed = False

        # Cleanup task (will be started when needed)
        self._cleanup_task_started = False

    def _start_cleanup_task(self):
        """Start the connection cleanup task."""
        if self._cleanup_task_started:
            return
        try:
            # Only create task if we have a running event loop
            if not self._cleanup_task or self._cleanup_task.done():
                self._cleanup_task = asyncio.create_task(self._cleanup_loop())
                self._cleanup_task_started = True
        except RuntimeError:
            # No running event loop, cleanup task will be started when needed
            pass

    async def _cleanup_loop(self):
        """Periodic cleanup of expired/stale connections."""
        while not self._closed:
            try:
                await asyncio.sleep(self.cleanup_interval)
                if not self._closed:
                    await self._cleanup_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in connection pool cleanup: {e}")

    async def _cleanup_connections(self):
        """Clean up expired and stale connections."""
        for service_name in list(self._pools.keys()):
            async with self._pool_locks[service_name]:
                pool = self._pools[service_name]
                cleaned = 0

                # Remove expired/stale connections
                remaining = deque()
                while pool:
                    conn = pool.popleft()
                    if conn.is_expired(self.max_connection_age) or conn.is_stale(
                        self.max_idle_time
                    ):
                        try:
                            await self._close_connection(conn.connection)
                            cleaned += 1
                            self._stats[service_name]["expired"] += 1
                        except Exception as e:
                            logger.warning(
                                f"Error closing expired connection for {service_name}: {e}"
                            )
                    else:
                        remaining.append(conn)

                self._pools[service_name] = remaining

                if cleaned > 0:
                    logger.debug(f"Cleaned up {cleaned} connections for {service_name}")

    async def get_connection(self, service_name: str) -> Any:
        """
        Get a connection from the pool.

        Args:
            service_name: AWS service name (s3, bedrock-runtime, etc.)

        Returns:
            AWS service client

        Raises:
            ResourceError: If connection cannot be obtained
        """
        if self._closed:
            raise ResourceError("Connection pool is closed")

        # Start cleanup task if not already started
        if not self._cleanup_task_started:
            self._start_cleanup_task()

        async with self._pool_locks[service_name]:
            # Try to reuse existing connection
            pool = self._pools[service_name]
            while pool:
                conn = pool.popleft()
                if not conn.is_expired(self.max_connection_age) and not conn.is_stale(
                    self.max_idle_time
                ):
                    conn.mark_used()
                    self._active_connections[service_name].add(conn)
                    self._stats[service_name]["reused"] += 1
                    logger.debug(f"Reusing connection for {service_name}")
                    return conn.connection
                else:
                    # Connection is expired/stale, close it
                    try:
                        await self._close_connection(conn.connection)
                        self._stats[service_name]["expired"] += 1
                    except Exception as e:
                        logger.warning(f"Error closing expired connection: {e}")

            # Check if we can create a new connection
            active_count = len(self._active_connections[service_name])
            if active_count >= self.max_connections_per_service:
                raise ResourceError(
                    f"Maximum connections reached for {service_name} "
                    f"({active_count}/{self.max_connections_per_service})"
                )

            # Create new connection
            try:
                connection = await self._create_connection(service_name)
                pooled_conn = PooledConnection(connection, service_name)
                pooled_conn.mark_used()
                self._active_connections[service_name].add(pooled_conn)
                self._stats[service_name]["created"] += 1
                logger.debug(f"Created new connection for {service_name}")
                return connection
            except Exception as e:
                self._stats[service_name]["errors"] += 1
                raise ResourceError(f"Failed to create connection for {service_name}: {e}") from e

    async def return_connection(self, service_name: str, connection: Any) -> None:
        """
        Return a connection to the pool.

        Args:
            service_name: AWS service name
            connection: Connection to return
        """
        if self._closed:
            return

        async with self._pool_locks[service_name]:
            # Find the pooled connection
            pooled_conn = None
            active_connections = self._active_connections[service_name].copy()

            for conn in active_connections:
                if conn.connection is connection:
                    pooled_conn = conn
                    break

            if pooled_conn is None:
                logger.warning(f"Trying to return unknown connection for {service_name}")
                return

            # Remove from active set
            self._active_connections[service_name].discard(pooled_conn)

            # Check if connection is still usable
            if pooled_conn.is_expired(self.max_connection_age):
                try:
                    await self._close_connection(connection)
                    self._stats[service_name]["expired"] += 1
                except Exception as e:
                    logger.warning(f"Error closing expired connection: {e}")
                return

            # Return to pool
            pooled_conn.mark_returned()
            self._pools[service_name].append(pooled_conn)
            logger.debug(f"Returned connection to pool for {service_name}")

    async def _create_connection(self, service_name: str) -> Any:
        """Create a new AWS service connection."""
        try:
            session = await self.auth_manager.get_session()
            region = self.config_manager.get_aws_region()

            # Create service client
            if service_name == "s3":
                return session.client("s3", region_name=region)
            elif service_name == "bedrock-runtime":
                return session.client("bedrock-runtime", region_name=region)
            elif service_name == "bedrock-agent-runtime":
                return session.client("bedrock-agent-runtime", region_name=region)
            elif service_name == "bedrock-agent":
                return session.client("bedrock-agent", region_name=region)
            elif service_name == "sts":
                return session.client("sts", region_name=region)
            else:
                # Generic service
                return session.client(service_name, region_name=region)

        except Exception as e:
            logger.error(f"Failed to create {service_name} client: {e}")
            raise

    async def _close_connection(self, connection: Any) -> None:
        """Close a connection safely."""
        try:
            if hasattr(connection, "close"):
                if asyncio.iscoroutinefunction(connection.close):
                    await connection.close()
                else:
                    connection.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

    async def close_all(self) -> None:
        """Close all connections and shut down the pool."""
        if self._closed:
            return

        self._closed = True

        # Cancel cleanup task
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Close all connections
        for service_name in list(self._pools.keys()):
            async with self._pool_locks[service_name]:
                # Close pooled connections
                pool = self._pools[service_name]
                while pool:
                    conn = pool.popleft()
                    try:
                        await self._close_connection(conn.connection)
                    except Exception as e:
                        logger.warning(f"Error closing pooled connection: {e}")

                # Close active connections
                active_connections = self._active_connections[service_name].copy()
                for conn in active_connections:
                    try:
                        await self._close_connection(conn.connection)
                    except Exception as e:
                        logger.warning(f"Error closing active connection: {e}")

                self._pools[service_name].clear()
                self._active_connections[service_name].clear()

        logger.info("Connection pool closed")

    def get_stats(self) -> dict[str, Any]:
        """Get connection pool statistics."""
        stats = {}
        for service_name, service_stats in self._stats.items():
            pool_size = len(self._pools[service_name])
            active_size = len(self._active_connections[service_name])

            stats[service_name] = {
                **service_stats,
                "active": active_size,
                "idle": pool_size,
                "total": active_size + pool_size,
            }

        return {
            "services": stats,
            "total_active": sum(len(conns) for conns in self._active_connections.values()),
            "total_idle": sum(len(pool) for pool in self._pools.values()),
            "max_per_service": self.max_connections_per_service,
            "closed": self._closed,
        }


class ConnectionContextManager:
    """Context manager for automatic connection return."""

    def __init__(self, pool: AWSConnectionPool, service_name: str):
        self.pool = pool
        self.service_name = service_name
        self.connection: Any | None = None

    async def __aenter__(self) -> Any:
        """Acquire connection."""
        self.connection = await self.pool.get_connection(self.service_name)
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Return connection to pool."""
        if self.connection is not None:
            await self.pool.return_connection(self.service_name, self.connection)


# Convenience function for creating connection pool
def create_connection_pool(
    auth_manager: AuthManagerInterface, config_manager: ConfigManagerInterface, **kwargs
) -> AWSConnectionPool:
    """
    Create a connection pool with default settings.

    Args:
        auth_manager: Authentication manager
        config_manager: Configuration manager
        **kwargs: Additional pool configuration

    Returns:
        Connection pool instance
    """
    return AWSConnectionPool(auth_manager, config_manager, **kwargs)
