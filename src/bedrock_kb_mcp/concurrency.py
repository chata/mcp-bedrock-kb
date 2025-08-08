from __future__ import annotations

"""
Concurrency utilities for Bedrock Knowledge Base MCP Server.

This module provides utilities for managing concurrent operations,
including semaphore-based processing and batch operations.
"""

import asyncio
import builtins
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from .exceptions import ConcurrencyError
from .exceptions import TimeoutError as CustomTimeoutError

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class ProcessingResult:
    """Result of a processing operation."""

    success: bool
    result: Any = None
    error: Exception | None = None
    duration: float = 0.0
    item_id: str | None = None


@dataclass
class BatchProcessingStats:
    """Statistics for batch processing."""

    total_items: int
    successful: int
    failed: int
    total_duration: float
    average_duration: float
    max_duration: float
    min_duration: float
    errors: list[Exception]


class ConcurrentProcessor:
    """Processor for handling concurrent operations with semaphore control."""

    def __init__(
        self,
        max_concurrent: int = 10,
        timeout_seconds: float | None = None,
        retry_attempts: int = 0,
        retry_delay: float = 1.0,
    ):
        self.max_concurrent = max_concurrent
        self.timeout_seconds = timeout_seconds
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self._semaphore = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        """Get or create semaphore in current event loop."""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)
        return self._semaphore

    async def process_single(
        self, func: Callable[[T], Awaitable[R]], item: T, item_id: str | None = None
    ) -> ProcessingResult:
        """
        Process a single item with semaphore control.

        Args:
            func: Async function to process the item
            item: Item to process
            item_id: Optional identifier for the item

        Returns:
            Processing result
        """
        start_time = time.time()

        async with self.semaphore:
            for attempt in range(self.retry_attempts + 1):
                try:
                    if self.timeout_seconds:
                        result = await asyncio.wait_for(func(item), timeout=self.timeout_seconds)
                    else:
                        result = await func(item)

                    duration = time.time() - start_time
                    return ProcessingResult(
                        success=True, result=result, duration=duration, item_id=item_id
                    )

                except (asyncio.TimeoutError, builtins.TimeoutError):  # noqa: UP041
                    duration = time.time() - start_time
                    error = CustomTimeoutError(
                        f"Operation timed out after {self.timeout_seconds}s", self.timeout_seconds
                    )
                    logger.warning(f"Item {item_id} timed out on attempt {attempt + 1}")

                    if attempt == self.retry_attempts:
                        return ProcessingResult(
                            success=False, error=error, duration=duration, item_id=item_id
                        )

                    await asyncio.sleep(self.retry_delay * (attempt + 1))

                except Exception as e:
                    duration = time.time() - start_time
                    logger.warning(f"Item {item_id} failed on attempt {attempt + 1}: {e}")

                    if attempt == self.retry_attempts:
                        return ProcessingResult(
                            success=False, error=e, duration=duration, item_id=item_id
                        )

                    await asyncio.sleep(self.retry_delay * (attempt + 1))

        # Should never reach here
        duration = time.time() - start_time
        return ProcessingResult(
            success=False,
            error=ConcurrencyError("Unexpected processing failure"),
            duration=duration,
            item_id=item_id,
        )

    async def process_batch(
        self, func: Callable[[T], Awaitable[R]], items: list[T], item_ids: list[str | None] = None
    ) -> tuple[list[ProcessingResult], BatchProcessingStats]:
        """
        Process a batch of items concurrently.

        Args:
            func: Async function to process each item
            items: List of items to process
            item_ids: Optional list of item identifiers

        Returns:
            Tuple of (results, statistics)
        """
        if not items:
            return [], BatchProcessingStats(
                total_items=0,
                successful=0,
                failed=0,
                total_duration=0.0,
                average_duration=0.0,
                max_duration=0.0,
                min_duration=0.0,
                errors=[],
            )

        start_time = time.time()

        # Create tasks for all items
        if item_ids and len(item_ids) == len(items):
            tasks = [
                self.process_single(func, item, item_id)
                for item, item_id in zip(items, item_ids)  # noqa: B905
            ]
        else:
            tasks = [self.process_single(func, item, f"item_{i}") for i, item in enumerate(items)]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Calculate statistics
        total_duration = time.time() - start_time
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        durations = [r.duration for r in results]
        errors = [r.error for r in results if r.error is not None]

        stats = BatchProcessingStats(
            total_items=len(items),
            successful=successful,
            failed=failed,
            total_duration=total_duration,
            average_duration=sum(durations) / len(durations) if durations else 0.0,
            max_duration=max(durations) if durations else 0.0,
            min_duration=min(durations) if durations else 0.0,
            errors=errors,
        )

        logger.info(
            f"Batch processing completed: {successful}/{len(items)} successful, "
            f"took {total_duration:.2f}s"
        )

        return results, stats


class DocumentProcessor:
    """Specialized processor for document operations."""

    def __init__(
        self,
        max_concurrent: int = 5,  # Conservative for document processing
        timeout_seconds: float = 60.0,  # 1 minute per document
        retry_attempts: int = 2,
    ):
        self.processor = ConcurrentProcessor(
            max_concurrent=max_concurrent,
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
        )

    async def process_documents_for_pii(
        self,
        documents: list[dict[str, Any]],
        pii_processor_func: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> tuple[list[ProcessingResult], BatchProcessingStats]:
        """
        Process documents for PII detection concurrently.

        Args:
            documents: List of documents to process
            pii_processor_func: Function to process each document for PII

        Returns:
            Tuple of (results, statistics)
        """
        document_ids = [
            doc.get("key", doc.get("name", f"doc_{i}")) for i, doc in enumerate(documents)
        ]

        return await self.processor.process_batch(pii_processor_func, documents, document_ids)

    async def process_documents_for_deletion(
        self, document_keys: list[str], deletion_func: Callable[[str], Awaitable[bool]]
    ) -> tuple[list[ProcessingResult], BatchProcessingStats]:
        """
        Process document deletions concurrently.

        Args:
            document_keys: List of document keys to delete
            deletion_func: Function to delete each document

        Returns:
            Tuple of (results, statistics)
        """
        return await self.processor.process_batch(deletion_func, document_keys, document_keys)


class SearchProcessor:
    """Specialized processor for search operations."""

    def __init__(
        self,
        max_concurrent: int = 15,  # Higher for search operations
        timeout_seconds: float = 30.0,  # 30 seconds per search
        retry_attempts: int = 1,
    ):
        self.processor = ConcurrentProcessor(
            max_concurrent=max_concurrent,
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
        )

    async def search_multiple_knowledge_bases(
        self,
        search_requests: list[dict[str, Any]],
        search_func: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> tuple[list[ProcessingResult], BatchProcessingStats]:
        """
        Search multiple knowledge bases concurrently.

        Args:
            search_requests: List of search request dictionaries
            search_func: Function to perform each search

        Returns:
            Tuple of (results, statistics)
        """
        request_ids = [
            f"kb_{req.get('knowledge_base_id', i)}_{req.get('query', 'query')[:20]}"
            for i, req in enumerate(search_requests)
        ]

        return await self.processor.process_batch(search_func, search_requests, request_ids)


class RateLimitedProcessor:
    """Processor with rate limiting capabilities."""

    def __init__(
        self,
        max_concurrent: int = 10,
        rate_limit_per_second: float = 5.0,
        burst_size: int = 10,
    ):
        self.max_concurrent = max_concurrent
        self.rate_limit_per_second = rate_limit_per_second
        self.burst_size = burst_size

        # Rate limiting state
        self.tokens = burst_size
        self.last_update = time.time()
        self._rate_lock = None

        self.processor = ConcurrentProcessor(max_concurrent=max_concurrent)

    @property
    def rate_lock(self) -> asyncio.Lock:
        """Get or create rate lock in current event loop."""
        if self._rate_lock is None:
            self._rate_lock = asyncio.Lock()
        return self._rate_lock

    async def _acquire_token(self) -> None:
        """Acquire a token for rate limiting."""
        async with self.rate_lock:
            now = time.time()
            time_passed = now - self.last_update

            # Add tokens based on time passed
            new_tokens = time_passed * self.rate_limit_per_second
            self.tokens = min(self.burst_size, self.tokens + new_tokens)
            self.last_update = now

            # Wait if no tokens available
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate_limit_per_second
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1

    async def process_with_rate_limit(
        self, func: Callable[[T], Awaitable[R]], item: T, item_id: str | None = None
    ) -> ProcessingResult:
        """
        Process an item with rate limiting.

        Args:
            func: Async function to process the item
            item: Item to process
            item_id: Optional identifier for the item

        Returns:
            Processing result
        """
        await self._acquire_token()
        return await self.processor.process_single(func, item, item_id)

    async def process_batch_with_rate_limit(
        self, func: Callable[[T], Awaitable[R]], items: list[T], item_ids: list[str | None] = None
    ) -> tuple[list[ProcessingResult], BatchProcessingStats]:
        """
        Process a batch with rate limiting.

        Args:
            func: Async function to process each item
            items: List of items to process
            item_ids: Optional list of item identifiers

        Returns:
            Tuple of (results, statistics)
        """

        async def rate_limited_func(item: T) -> R:
            await self._acquire_token()
            return await func(item)

        return await self.processor.process_batch(rate_limited_func, items, item_ids)


# Convenience functions


async def process_concurrently(
    func: Callable[[T], Awaitable[R]],
    items: list[T],
    max_concurrent: int = 10,
    timeout_seconds: float | None = None,
    retry_attempts: int = 0,
) -> tuple[list[ProcessingResult], BatchProcessingStats]:
    """
    Convenient function for concurrent processing.

    Args:
        func: Async function to process each item
        items: List of items to process
        max_concurrent: Maximum concurrent operations
        timeout_seconds: Timeout per operation
        retry_attempts: Number of retry attempts

    Returns:
        Tuple of (results, statistics)
    """
    processor = ConcurrentProcessor(
        max_concurrent=max_concurrent,
        timeout_seconds=timeout_seconds,
        retry_attempts=retry_attempts,
    )

    return await processor.process_batch(func, items)


async def process_with_semaphore(
    func: Callable[[T], Awaitable[R]], items: list[T], semaphore: asyncio.Semaphore
) -> list[R]:
    """
    Process items with an existing semaphore.

    Args:
        func: Async function to process each item
        items: List of items to process
        semaphore: Semaphore for limiting concurrency

    Returns:
        List of results
    """

    async def semaphore_wrapped_func(item: T) -> R:
        async with semaphore:
            return await func(item)

    tasks = [semaphore_wrapped_func(item) for item in items]
    return await asyncio.gather(*tasks)
