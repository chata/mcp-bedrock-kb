from __future__ import annotations

"""
Tests for AWS connection pooling functionality.
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.bedrock_kb_mcp.connection_pool import (
    AWSConnectionPool,
    ConnectionContextManager,
    PooledConnection,
    create_connection_pool,
)
from src.bedrock_kb_mcp.exceptions import ResourceError
from tests.test_interfaces import MockAuthManager, MockConfigManager


class TestPooledConnection:
    """Test PooledConnection wrapper."""

    def test_creation(self):
        """Test connection creation."""
        mock_conn = MagicMock()
        pooled = PooledConnection(mock_conn, "s3")

        assert pooled.connection is mock_conn
        assert pooled.service_name == "s3"
        assert pooled.use_count == 0
        assert not pooled.in_use
        assert pooled.created_at <= time.time()

    def test_mark_used(self):
        """Test marking connection as used."""
        mock_conn = MagicMock()
        pooled = PooledConnection(mock_conn, "s3")

        pooled.mark_used()

        assert pooled.use_count == 1
        assert pooled.in_use
        assert pooled.last_used <= time.time()

    def test_mark_returned(self):
        """Test marking connection as returned."""
        mock_conn = MagicMock()
        pooled = PooledConnection(mock_conn, "s3")
        pooled.mark_used()

        pooled.mark_returned()

        assert not pooled.in_use

    def test_is_expired(self):
        """Test expiration check."""
        mock_conn = MagicMock()
        pooled = PooledConnection(mock_conn, "s3")

        # Fresh connection should not be expired
        assert not pooled.is_expired(3600)

        # Simulate old connection
        pooled.created_at = time.time() - 7200  # 2 hours ago
        assert pooled.is_expired(3600)  # 1 hour max age

    def test_is_stale(self):
        """Test staleness check."""
        mock_conn = MagicMock()
        pooled = PooledConnection(mock_conn, "s3")

        # Active connection should not be stale
        pooled.mark_used()
        assert not pooled.is_stale(300)

        # Returned connection
        pooled.mark_returned()

        # Fresh returned connection should not be stale
        assert not pooled.is_stale(300)

        # Old returned connection should be stale
        pooled.last_used = time.time() - 600  # 10 minutes ago
        assert pooled.is_stale(300)  # 5 minutes max idle


class TestAWSConnectionPool:
    """Test AWS connection pool."""

    @pytest.fixture
    def auth_manager(self):
        """Create mock auth manager."""
        return MockAuthManager()

    @pytest.fixture
    def config_manager(self):
        """Create mock config manager."""
        return MockConfigManager()

    @pytest.fixture
    def pool(self, auth_manager, config_manager):
        """Create connection pool."""
        return AWSConnectionPool(
            auth_manager,
            config_manager,
            max_connections_per_service=2,
            cleanup_interval=0.1,  # Fast cleanup for testing
        )

    @pytest.mark.asyncio
    async def test_pool_creation(self, pool):
        """Test pool creation."""
        assert pool.max_connections_per_service == 2
        assert not pool._closed
        # Cleanup task should be lazily initialized
        assert pool._cleanup_task_started is False

    @pytest.mark.asyncio
    async def test_get_new_connection(self, pool):
        """Test getting a new connection."""
        with patch.object(pool, "_create_connection", new_callable=AsyncMock) as mock_create:
            mock_client = MagicMock()
            mock_create.return_value = mock_client

            conn = await pool.get_connection("s3")

            assert conn is mock_client
            mock_create.assert_called_once_with("s3")

            stats = pool.get_stats()
            assert stats["services"]["s3"]["created"] == 1
            assert stats["services"]["s3"]["active"] == 1

    @pytest.mark.asyncio
    async def test_reuse_connection(self, pool):
        """Test connection reuse."""
        with patch.object(pool, "_create_connection", new_callable=AsyncMock) as mock_create:
            mock_client = MagicMock()
            mock_create.return_value = mock_client

            # Get and return connection
            conn1 = await pool.get_connection("s3")
            await pool.return_connection("s3", conn1)

            # Get connection again (should reuse)
            conn2 = await pool.get_connection("s3")

            assert conn1 is conn2
            mock_create.assert_called_once()  # Only created once

            stats = pool.get_stats()
            assert stats["services"]["s3"]["created"] == 1
            assert stats["services"]["s3"]["reused"] == 1

    @pytest.mark.asyncio
    async def test_max_connections_limit(self, pool):
        """Test maximum connections limit."""
        with patch.object(pool, "_create_connection", new_callable=AsyncMock) as mock_create:
            mock_create.side_effect = [MagicMock(), MagicMock()]

            # Get maximum connections
            await pool.get_connection("s3")
            await pool.get_connection("s3")

            # Try to get one more (should fail)
            with pytest.raises(ResourceError, match="Maximum connections reached"):
                await pool.get_connection("s3")

    @pytest.mark.asyncio
    async def test_connection_cleanup(self, pool):
        """Test connection cleanup."""
        with patch.object(pool, "_create_connection", new_callable=AsyncMock) as mock_create:
            with patch.object(pool, "_close_connection", new_callable=AsyncMock) as mock_close:
                mock_client = MagicMock()
                mock_create.return_value = mock_client

                # Get and return connection
                conn = await pool.get_connection("s3")
                await pool.return_connection("s3", conn)

                # Manually trigger cleanup with expired connection
                pooled_conn = pool._pools["s3"][0]
                pooled_conn.created_at = time.time() - 7200  # 2 hours ago

                await pool._cleanup_connections()

                mock_close.assert_called_once_with(mock_client)
                assert len(pool._pools["s3"]) == 0

    @pytest.mark.asyncio
    async def test_create_s3_connection(self, pool):
        """Test S3 client creation."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client

        with patch.object(
            pool.auth_manager, "get_session", new_callable=AsyncMock
        ) as mock_get_session:
            mock_get_session.return_value = mock_session

            client = await pool._create_connection("s3")

            assert client is mock_client
            mock_session.client.assert_called_once_with("s3", region_name="us-east-1")

    @pytest.mark.asyncio
    async def test_create_bedrock_connection(self, pool):
        """Test Bedrock client creation."""
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_session.client.return_value = mock_client

        with patch.object(
            pool.auth_manager, "get_session", new_callable=AsyncMock
        ) as mock_get_session:
            mock_get_session.return_value = mock_session

            client = await pool._create_connection("bedrock-runtime")

            assert client is mock_client
            mock_session.client.assert_called_once_with("bedrock-runtime", region_name="us-east-1")

    @pytest.mark.asyncio
    async def test_create_connection_failure(self, pool):
        """Test connection creation failure."""
        with patch.object(
            pool.auth_manager, "get_session", new_callable=AsyncMock
        ) as mock_get_session:
            mock_get_session.side_effect = Exception("Auth failed")

            with pytest.raises(ResourceError, match="Failed to create connection"):
                await pool.get_connection("s3")

            stats = pool.get_stats()
            assert stats["services"]["s3"]["errors"] == 1

    @pytest.mark.asyncio
    async def test_return_unknown_connection(self, pool):
        """Test returning unknown connection."""
        mock_conn = MagicMock()

        # Should not raise, but should log warning
        await pool.return_connection("s3", mock_conn)

    @pytest.mark.asyncio
    async def test_close_pool(self, pool):
        """Test closing the pool."""
        with patch.object(pool, "_create_connection", new_callable=AsyncMock):
            with patch.object(pool, "_close_connection", new_callable=AsyncMock) as mock_close:
                # Get some connections
                conn1 = await pool.get_connection("s3")
                await pool.get_connection("bedrock-runtime")
                await pool.return_connection("s3", conn1)

                # Close pool
                await pool.close_all()

                assert pool._closed
                # If cleanup task was started, it should be cancelled
                if pool._cleanup_task_started and pool._cleanup_task:
                    assert pool._cleanup_task.cancelled()
                assert mock_close.call_count >= 2  # Both connections closed

    @pytest.mark.asyncio
    async def test_get_connection_after_close(self, pool):
        """Test getting connection after pool is closed."""
        await pool.close_all()

        with pytest.raises(ResourceError, match="Connection pool is closed"):
            await pool.get_connection("s3")

    def test_get_stats(self, pool):
        """Test getting pool statistics."""
        stats = pool.get_stats()

        assert "services" in stats
        assert "total_active" in stats
        assert "total_idle" in stats
        assert "max_per_service" in stats
        assert "closed" in stats

        assert stats["max_per_service"] == 2
        assert not stats["closed"]


class TestConnectionContextManager:
    """Test connection context manager."""

    @pytest.fixture
    def pool(self):
        """Create mock pool."""
        pool = MagicMock()
        pool.get_connection = AsyncMock()
        pool.return_connection = AsyncMock()
        return pool

    @pytest.mark.asyncio
    async def test_context_manager_success(self, pool):
        """Test successful context manager usage."""
        mock_conn = MagicMock()
        pool.get_connection.return_value = mock_conn

        async with ConnectionContextManager(pool, "s3") as conn:
            assert conn is mock_conn
            pool.get_connection.assert_called_once_with("s3")

        pool.return_connection.assert_called_once_with("s3", mock_conn)

    @pytest.mark.asyncio
    async def test_context_manager_exception(self, pool):
        """Test context manager with exception."""
        mock_conn = MagicMock()
        pool.get_connection.return_value = mock_conn

        with pytest.raises(ValueError):
            async with ConnectionContextManager(pool, "s3") as conn:
                assert conn is mock_conn
                raise ValueError("Test error")

        # Should still return connection even on exception
        pool.return_connection.assert_called_once_with("s3", mock_conn)


class TestCreateConnectionPool:
    """Test connection pool factory function."""

    def test_create_connection_pool(self):
        """Test creating connection pool with factory function."""
        auth_manager = MockAuthManager()
        config_manager = MockConfigManager()

        pool = create_connection_pool(
            auth_manager, config_manager, max_connections_per_service=5, max_idle_time=600
        )

        assert isinstance(pool, AWSConnectionPool)
        assert pool.auth_manager is auth_manager
        assert pool.config_manager is config_manager
        assert pool.max_connections_per_service == 5
        assert pool.max_idle_time == 600
