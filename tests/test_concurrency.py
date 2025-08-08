from __future__ import annotations

"""
Tests for concurrency utilities.
"""

import asyncio
import time

import pytest

from src.bedrock_kb_mcp.concurrency import (
    BatchProcessingStats,
    ConcurrentProcessor,
    DocumentProcessor,
    ProcessingResult,
    RateLimitedProcessor,
    SearchProcessor,
    process_concurrently,
    process_with_semaphore,
)
from src.bedrock_kb_mcp.exceptions import TimeoutError as CustomTimeoutError


class TestProcessingResult:
    """Test ProcessingResult dataclass."""

    def test_successful_result(self):
        """Test successful processing result."""
        result = ProcessingResult(
            success=True, result="processed_data", duration=1.5, item_id="item_1"
        )

        assert result.success
        assert result.result == "processed_data"
        assert result.error is None
        assert result.duration == 1.5
        assert result.item_id == "item_1"

    def test_failed_result(self):
        """Test failed processing result."""
        error = Exception("Processing failed")
        result = ProcessingResult(success=False, error=error, duration=0.5, item_id="item_2")

        assert not result.success
        assert result.result is None
        assert result.error is error
        assert result.duration == 0.5
        assert result.item_id == "item_2"


class TestBatchProcessingStats:
    """Test BatchProcessingStats dataclass."""

    def test_stats_creation(self):
        """Test stats creation."""
        errors = [Exception("Error 1"), ValueError("Error 2")]
        stats = BatchProcessingStats(
            total_items=10,
            successful=8,
            failed=2,
            total_duration=5.0,
            average_duration=0.5,
            max_duration=1.2,
            min_duration=0.1,
            errors=errors,
        )

        assert stats.total_items == 10
        assert stats.successful == 8
        assert stats.failed == 2
        assert stats.total_duration == 5.0
        assert stats.average_duration == 0.5
        assert stats.max_duration == 1.2
        assert stats.min_duration == 0.1
        assert len(stats.errors) == 2


class TestConcurrentProcessor:
    """Test concurrent processor."""

    @pytest.fixture
    def processor(self):
        """Create processor instance."""
        return ConcurrentProcessor(
            max_concurrent=3, timeout_seconds=1.0, retry_attempts=1, retry_delay=0.1
        )

    @pytest.mark.asyncio
    async def test_process_single_success(self, processor):
        """Test successful single item processing."""

        async def mock_func(item):
            await asyncio.sleep(0.1)
            return f"processed_{item}"

        result = await processor.process_single(mock_func, "test_item", "item_1")

        assert result.success
        assert result.result == "processed_test_item"
        assert result.item_id == "item_1"
        assert result.duration > 0.1
        assert result.error is None

    @pytest.mark.asyncio
    async def test_process_single_failure(self, processor):
        """Test failed single item processing."""

        async def mock_func(item):
            raise ValueError("Processing failed")

        result = await processor.process_single(mock_func, "test_item", "item_1")

        assert not result.success
        assert result.result is None
        assert result.item_id == "item_1"
        assert isinstance(result.error, ValueError)

    @pytest.mark.asyncio
    async def test_process_single_timeout(self, processor):
        """Test timeout handling."""

        async def mock_func(item):
            await asyncio.sleep(2.0)  # Longer than timeout
            return "done"

        result = await processor.process_single(mock_func, "test_item")

        assert not result.success
        assert isinstance(result.error, CustomTimeoutError)
        assert "timed out" in result.error.message.lower()

    @pytest.mark.asyncio
    async def test_process_single_with_retry(self, processor):
        """Test retry mechanism."""
        call_count = 0

        async def mock_func(item):
            nonlocal call_count
            call_count += 1
            if call_count < 2:  # Fail first time
                raise ValueError("Temporary failure")
            return f"processed_{item}"

        result = await processor.process_single(mock_func, "test_item")

        assert result.success
        assert result.result == "processed_test_item"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_process_batch_success(self, processor):
        """Test successful batch processing."""

        async def mock_func(item):
            await asyncio.sleep(0.1)
            return f"processed_{item}"

        items = ["item1", "item2", "item3"]
        results, stats = await processor.process_batch(mock_func, items)

        assert len(results) == 3
        assert all(r.success for r in results)
        assert stats.total_items == 3
        assert stats.successful == 3
        assert stats.failed == 0
        assert stats.total_duration > 0
        assert len(stats.errors) == 0

    @pytest.mark.asyncio
    async def test_process_batch_mixed_results(self, processor):
        """Test batch processing with mixed success/failure."""

        async def mock_func(item):
            if item == "fail":
                raise ValueError("Item failed")
            return f"processed_{item}"

        items = ["item1", "fail", "item3"]
        results, stats = await processor.process_batch(mock_func, items)

        assert len(results) == 3
        assert results[0].success
        assert not results[1].success
        assert results[2].success
        assert stats.successful == 2
        assert stats.failed == 1
        assert len(stats.errors) == 1
        assert isinstance(stats.errors[0], ValueError)

    @pytest.mark.asyncio
    async def test_process_batch_empty(self, processor):
        """Test processing empty batch."""

        async def mock_func(item):
            return item

        results, stats = await processor.process_batch(mock_func, [])

        assert len(results) == 0
        assert stats.total_items == 0
        assert stats.successful == 0
        assert stats.failed == 0
        assert stats.total_duration == 0

    @pytest.mark.asyncio
    async def test_process_batch_with_item_ids(self, processor):
        """Test batch processing with custom item IDs."""

        async def mock_func(item):
            return f"processed_{item}"

        items = ["item1", "item2"]
        item_ids = ["custom_1", "custom_2"]
        results, stats = await processor.process_batch(mock_func, items, item_ids)

        assert results[0].item_id == "custom_1"
        assert results[1].item_id == "custom_2"

    @pytest.mark.asyncio
    async def test_concurrency_limit(self, processor):
        """Test that concurrency is properly limited."""
        active_count = 0
        max_active = 0

        async def mock_func(item):
            nonlocal active_count, max_active
            active_count += 1
            max_active = max(max_active, active_count)
            await asyncio.sleep(0.2)
            active_count -= 1
            return f"processed_{item}"

        items = ["item1", "item2", "item3", "item4", "item5"]
        await processor.process_batch(mock_func, items)

        assert max_active <= processor.max_concurrent


class TestDocumentProcessor:
    """Test document processor."""

    @pytest.fixture
    def processor(self):
        """Create document processor."""
        return DocumentProcessor(max_concurrent=2, timeout_seconds=1.0)

    @pytest.mark.asyncio
    async def test_process_documents_for_pii(self, processor):
        """Test PII processing for documents."""
        documents = [
            {"key": "doc1.txt", "content": "Document 1"},
            {"key": "doc2.txt", "content": "Document 2"},
        ]

        async def pii_processor(doc):
            await asyncio.sleep(0.1)
            return {"document": doc["key"], "pii_found": False}

        results, stats = await processor.process_documents_for_pii(documents, pii_processor)

        assert len(results) == 2
        assert all(r.success for r in results)
        assert results[0].item_id == "doc1.txt"
        assert results[1].item_id == "doc2.txt"

    @pytest.mark.asyncio
    async def test_process_documents_for_deletion(self, processor):
        """Test document deletion processing."""
        document_keys = ["doc1.txt", "doc2.txt", "doc3.txt"]

        async def deletion_func(key):
            await asyncio.sleep(0.1)
            if key == "doc2.txt":
                raise Exception(f"Deletion failed for {key}")
            return True

        results, stats = await processor.process_documents_for_deletion(
            document_keys, deletion_func
        )

        assert len(results) == 3
        assert results[0].success  # doc1
        assert not results[1].success  # doc2 (simulated failure)
        assert results[2].success  # doc3
        assert stats.successful == 2
        assert stats.failed == 1


class TestSearchProcessor:
    """Test search processor."""

    @pytest.fixture
    def processor(self):
        """Create search processor."""
        return SearchProcessor(max_concurrent=5, timeout_seconds=0.5)

    @pytest.mark.asyncio
    async def test_search_multiple_knowledge_bases(self, processor):
        """Test searching multiple knowledge bases."""
        search_requests = [
            {"knowledge_base_id": "kb1", "query": "test query 1"},
            {"knowledge_base_id": "kb2", "query": "test query 2"},
        ]

        async def search_func(request):
            await asyncio.sleep(0.1)
            return {
                "results": [f"result for {request['knowledge_base_id']}"],
                "query": request["query"],
            }

        results, stats = await processor.search_multiple_knowledge_bases(
            search_requests, search_func
        )

        assert len(results) == 2
        assert all(r.success for r in results)
        assert "kb_kb1_test query 1" in results[0].item_id
        assert "kb_kb2_test query 2" in results[1].item_id


class TestRateLimitedProcessor:
    """Test rate-limited processor."""

    @pytest.fixture
    def processor(self):
        """Create rate-limited processor."""
        return RateLimitedProcessor(max_concurrent=2, rate_limit_per_second=5.0, burst_size=3)

    @pytest.mark.asyncio
    async def test_process_with_rate_limit(self, processor):
        """Test rate-limited processing."""

        async def mock_func(item):
            return f"processed_{item}"

        start_time = time.time()
        result = await processor.process_with_rate_limit(mock_func, "test_item")
        end_time = time.time()

        assert result.success
        assert result.result == "processed_test_item"
        # Should be fast for first request (within burst limit)
        assert (end_time - start_time) < 0.5

    @pytest.mark.asyncio
    async def test_rate_limiting_behavior(self, processor):
        """Test that rate limiting actually works."""

        async def mock_func(item):
            return f"processed_{item}"

        # Process more items than burst size to trigger rate limiting
        items = ["item" + str(i) for i in range(15)]  # 15 items, burst_size=10

        start_time = time.time()
        results, stats = await processor.process_batch_with_rate_limit(mock_func, items)
        end_time = time.time()

        assert len(results) == 15
        assert all(r.success for r in results)

        # Should take some time due to rate limiting
        # With 5 req/sec and 15 requests with burst_size=10,
        # remaining 5 items should be rate limited (at least 1 second)
        assert (end_time - start_time) > 0.8


class TestConvenienceFunctions:
    """Test convenience functions."""

    @pytest.mark.asyncio
    async def test_process_concurrently(self):
        """Test process_concurrently function."""

        async def mock_func(item):
            await asyncio.sleep(0.1)
            return f"processed_{item}"

        items = ["item1", "item2", "item3"]
        results, stats = await process_concurrently(
            mock_func, items, max_concurrent=2, timeout_seconds=1.0
        )

        assert len(results) == 3
        assert all(r.success for r in results)
        assert stats.total_items == 3
        assert stats.successful == 3

    @pytest.mark.asyncio
    async def test_process_with_semaphore(self):
        """Test process_with_semaphore function."""
        semaphore = asyncio.Semaphore(2)
        active_count = 0
        max_active = 0

        async def mock_func(item):
            nonlocal active_count, max_active
            active_count += 1
            max_active = max(max_active, active_count)
            await asyncio.sleep(0.1)
            active_count -= 1
            return f"processed_{item}"

        items = ["item1", "item2", "item3", "item4"]
        results = await process_with_semaphore(mock_func, items, semaphore)

        assert len(results) == 4
        assert all(result.startswith("processed_") for result in results)
        assert max_active <= 2  # Semaphore limit
