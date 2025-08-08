from __future__ import annotations

"""Tests for PII detector module."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from security.pii_detector import PIIDetector, PIIFinding


class TestPIIDetector:
    """Test cases for PII detector functionality."""

    def test_masking_enabled_default(self):
        """Test that masking is enabled by default."""
        with patch.dict(os.environ, {}, clear=True):
            detector = PIIDetector()
            assert detector._is_masking_enabled() is True

    def test_masking_enabled_true(self):
        """Test masking enabled with environment variable."""
        with patch.dict(os.environ, {"BEDROCK_KB_MASK_PII": "true"}):
            detector = PIIDetector()
            assert detector._is_masking_enabled() is True

    def test_masking_disabled_false(self):
        """Test masking disabled with environment variable."""
        with patch.dict(os.environ, {"BEDROCK_KB_MASK_PII": "false"}):
            detector = PIIDetector()
            assert detector._is_masking_enabled() is False

    @patch("security.pii_detector.AnalyzerEngine")
    @patch("security.pii_detector.AnonymizerEngine")
    @patch("security.pii_detector.NlpEngineProvider")
    @pytest.mark.asyncio
    async def test_initialization_success(self, mock_nlp_provider, mock_anonymizer, mock_analyzer):
        """Test successful initialization with Presidio."""
        mock_nlp_engine = MagicMock()
        mock_nlp_provider_instance = MagicMock()
        mock_nlp_provider_instance.create_engine.return_value = mock_nlp_engine
        mock_nlp_provider.return_value = mock_nlp_provider_instance

        mock_analyzer_instance = MagicMock()
        mock_analyzer.return_value = mock_analyzer_instance

        mock_anonymizer_instance = MagicMock()
        mock_anonymizer.return_value = mock_anonymizer_instance

        with patch.dict(os.environ, {"BEDROCK_KB_MASK_PII": "true"}):
            detector = PIIDetector()
            await detector.ensure_initialized()

            assert detector.detection_enabled is True
            assert detector.masking_enabled is True
            assert detector.analyzer == mock_analyzer_instance
            assert detector.anonymizer == mock_anonymizer_instance

    @pytest.mark.asyncio
    async def test_initialization_presidio_unavailable(self):
        """Test initialization when Presidio is not available."""
        with patch("security.pii_detector.AnalyzerEngine", None):
            detector = PIIDetector()
            try:
                await detector.ensure_initialized()
            except ImportError:
                pass  # Expected when Presidio not available
            assert detector.detection_enabled is False

    async def test_detect_pii_email(self):
        """Test PII detection for email addresses."""
        detector = PIIDetector()

        # Mock analyzer
        mock_result = MagicMock()
        mock_result.entity_type = "EMAIL_ADDRESS"
        mock_result.start = 0
        mock_result.end = 16  # Correction: text@example.com is 16 characters
        mock_result.score = 0.9

        detector.analyzer = MagicMock()
        detector.analyzer.analyze.return_value = [mock_result]
        detector.detection_enabled = True

        text = "test@example.com is my email"
        findings = await detector.detect_pii(text)

        assert len(findings) == 1
        assert findings[0].entity_type == "EMAIL_ADDRESS"
        assert findings[0].text == "test@example.com"

    async def test_mask_pii_with_masking_enabled(self):
        """Test PII masking when masking is enabled."""
        detector = PIIDetector()
        detector.detection_enabled = True
        detector.masking_enabled = True

        # Mock detect_pii method
        finding = PIIFinding(
            entity_type="EMAIL_ADDRESS", start=0, end=16, score=0.9, text="test@example.com"
        )

        async def mock_detect_pii(text):
            return [finding]

        detector.detect_pii = mock_detect_pii

        text = "test@example.com is my email"
        masked_text, findings = await detector.mask_pii(text)

        assert len(findings) == 1
        assert masked_text == "[EMAIL_REDACTED] is my email"

    async def test_mask_pii_with_masking_disabled(self):
        """Test PII detection when masking is disabled (detection only)."""
        detector = PIIDetector()
        detector.detection_enabled = True
        detector.masking_enabled = False

        # Mock detect_pii method
        finding = PIIFinding(
            entity_type="EMAIL_ADDRESS", start=0, end=16, score=0.9, text="test@example.com"
        )

        async def mock_detect_pii(text):
            return [finding]

        detector.detect_pii = mock_detect_pii

        text = "test@example.com is my email"
        masked_text, findings = await detector.mask_pii(text)

        assert len(findings) == 1
        assert masked_text == text  # Original text unchanged
        assert findings[0].entity_type == "EMAIL_ADDRESS"

    def test_full_mask_various_entities(self):
        """Test full masking for various PII entity types."""
        detector = PIIDetector()

        findings = [
            PIIFinding("EMAIL_ADDRESS", 0, 16, 0.9, "test@example.com"),
            PIIFinding("PHONE_NUMBER", 17, 29, 0.8, "555-123-4567"),
            PIIFinding("CREDIT_CARD", 30, 49, 0.9, "4111-1111-1111-1111"),
            PIIFinding("US_SSN", 50, 61, 0.95, "123-45-6789"),
            PIIFinding("PERSON", 62, 72, 0.7, "John Smith"),
        ]

        text = "test@example.com 555-123-4567 4111-1111-1111-1111 123-45-6789 John Smith"
        masked_text = detector._full_mask(text, findings)

        expected = "[EMAIL_REDACTED] [PHONE_REDACTED] [CREDIT_CARD_REDACTED] [SSN_REDACTED] [NAME_REDACTED]"
        assert masked_text == expected

    def test_get_pii_warning(self):
        """Test PII warning message generation."""
        detector = PIIDetector()
        detector.masking_enabled = True

        findings = [
            PIIFinding("EMAIL_ADDRESS", 0, 16, 0.9, "test@example.com"),
            PIIFinding("EMAIL_ADDRESS", 17, 35, 0.9, "test2@example.com"),
            PIIFinding("PHONE_NUMBER", 36, 48, 0.8, "555-123-4567"),
        ]

        warning = detector.get_pii_warning(findings)
        assert "⚠️ PII detected and masked:" in warning
        assert "EMAIL_ADDRESS: 2" in warning
        assert "PHONE_NUMBER: 1" in warning

    async def test_process_metadata_safely(self):
        """Test safe metadata processing with PII detection."""
        detector = PIIDetector()
        detector.detection_enabled = True
        detector.masking_enabled = True

        # Mock mask_pii method
        async def mock_mask_pii(text):
            if "test@example.com" in text:
                return text.replace("test@example.com", "[EMAIL_REDACTED]"), [
                    PIIFinding("EMAIL_ADDRESS", 0, 16, 0.9, "test@example.com")
                ]
            return text, []

        detector.mask_pii = mock_mask_pii

        metadata = {
            "title": "Document about test@example.com",
            "author": "John Doe",
            "count": 42,
        }

        processed_metadata, warnings = await detector.process_metadata_safely(metadata)

        assert processed_metadata["title"] == "Document about [EMAIL_REDACTED]"
        assert processed_metadata["author"] == "John Doe"
        assert processed_metadata["count"] == 42
        assert len(warnings) == 1
        assert "title" in warnings[0]

    async def test_no_pii_detected(self):
        """Test behavior when no PII is detected."""
        detector = PIIDetector()
        detector.detection_enabled = True
        detector.masking_enabled = True

        # Mock detect_pii to return no findings
        async def mock_detect_pii(text):
            return []

        detector.detect_pii = mock_detect_pii

        text = "This is a normal text without PII"
        masked_text, findings = await detector.mask_pii(text)

        assert len(findings) == 0
        assert masked_text == text
        assert detector.get_pii_warning(findings) == ""

    async def test_detection_disabled(self):
        """Test behavior when detection is disabled."""
        detector = PIIDetector()
        detector.detection_enabled = False

        text = "test@example.com"
        findings = await detector.detect_pii(text)

        assert len(findings) == 0

    def test_memory_limit_calculation(self):
        """Test memory limit calculation."""
        detector = PIIDetector()

        # Test default calculation (should be positive)
        limit_mb = detector._get_memory_limit_mb()
        assert limit_mb >= 100  # Minimum is 100MB

        # Test with environment variable
        with patch.dict(os.environ, {"BEDROCK_KB_MEMORY_LIMIT_MB": "256"}):
            detector = PIIDetector()
            assert detector._get_memory_limit_mb() == 256

    def test_chunk_size_calculation(self):
        """Test chunk size calculation based on text length."""
        detector = PIIDetector()

        # Small text shouldn't be chunked
        small_text_length = 1000
        chunk_size = detector._get_chunk_size_chars(small_text_length)
        assert chunk_size == small_text_length

        # Large text should be chunked
        large_text_length = 100000000  # 100M chars - definitely large
        chunk_size = detector._get_chunk_size_chars(large_text_length)
        # For large text, chunk size should be reasonable (text_length // 10 or memory limited)
        expected_chunk_size = max(10000, large_text_length // 10)  # 10M chars for 100M
        memory_limit = detector._get_memory_limit_mb()
        max_chars = (memory_limit * 1024 * 1024) // (4 * 2)
        expected_chunk_size = min(max_chars, expected_chunk_size)
        assert chunk_size == expected_chunk_size
        assert chunk_size >= 10000  # Minimum chunk size

    def test_text_chunking(self):
        """Test text chunking with overlap."""
        detector = PIIDetector()
        text = "This is a test text that will be split into chunks for processing."
        chunk_size = 20
        overlap = 5

        chunks = list(detector._chunk_text(text, chunk_size, overlap))

        # Should have multiple chunks
        assert len(chunks) > 1

        # Each chunk should have position information
        for chunk_text, start, end in chunks:
            assert isinstance(chunk_text, str)
            assert isinstance(start, int)
            assert isinstance(end, int)
            assert start >= 0
            assert end <= len(text)
            assert start < end

    @patch("security.pii_detector.psutil")
    def test_analyze_text_chunk(self, mock_psutil):
        """Test text chunk analysis."""
        detector = PIIDetector()
        detector.detection_enabled = True

        # Mock analyzer
        mock_result = MagicMock()
        mock_result.entity_type = "EMAIL_ADDRESS"
        mock_result.start = 0
        mock_result.end = 16
        mock_result.score = 0.9

        detector.analyzer = MagicMock()
        detector.analyzer.analyze.return_value = [mock_result]

        text_chunk = "test@example.com is here"
        start_offset = 100
        end_offset = 124

        findings = detector._analyze_text_chunk(text_chunk, start_offset, end_offset)

        assert len(findings) == 1
        assert findings[0].entity_type == "EMAIL_ADDRESS"
        assert findings[0].start == start_offset + mock_result.start  # Offset adjusted
        assert findings[0].end == start_offset + mock_result.end  # Offset adjusted
        assert findings[0].text == "test@example.com"

    @patch("security.pii_detector.psutil")
    async def test_memory_monitoring_in_detection(self, mock_psutil):
        """Test memory monitoring during PII detection."""
        # Mock memory usage
        mock_memory = MagicMock()
        mock_memory.used = 500 * 1024 * 1024  # 500MB
        mock_psutil.virtual_memory.return_value = mock_memory

        detector = PIIDetector()
        detector.detection_enabled = True
        detector.analyzer = MagicMock()
        detector.analyzer.analyze.return_value = []

        # Mock _get_memory_limit_mb to return a small limit for testing
        detector._get_memory_limit_mb = MagicMock(return_value=100)

        text = "test@example.com" * 1000  # Large text to trigger chunking
        findings = await detector.detect_pii(text)

        # Should complete without crashing
        assert isinstance(findings, list)
