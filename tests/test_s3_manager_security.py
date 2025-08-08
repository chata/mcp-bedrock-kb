from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from security.pii_detector import PIIFinding
from src.bedrock_kb_mcp.s3_manager import S3Manager


class TestS3ManagerSecurity:
    """Test cases for S3Manager security features."""

    @pytest.fixture
    def mock_config(self):
        """Mock configuration."""
        config = MagicMock()
        config.get.return_value = "test-bucket"
        return config

    @pytest.fixture
    def mock_auth(self):
        """Mock auth manager."""
        auth = AsyncMock()
        auth.get_session = AsyncMock()
        return auth

    @pytest.fixture
    def s3_manager(self, mock_auth, mock_config):
        """Create S3Manager instance with mocked dependencies."""
        # Mock config to return supported formats
        mock_config.get.side_effect = lambda key, default=None: {
            "s3.default_bucket": "test-bucket",
            "document_processing.supported_formats": ["txt", "md", "html", "pdf", "docx"],
            "s3.max_concurrent_uploads": 5,
            "s3.max_file_size_mb": 500,
            "s3.encoding": "utf-8",
            "s3.upload_prefix": "documents/",
        }.get(key, default)

        manager = S3Manager(auth_manager=mock_auth, config_manager=mock_config)
        # Set proper defaults
        manager.encoding = "utf-8"
        manager.max_file_size_mb = 500
        manager.default_bucket = "test-bucket"
        manager.supported_formats = ["txt", "md", "html", "pdf", "docx"]
        manager.upload_prefix = "documents/"
        return manager

    @pytest.mark.asyncio
    async def test_upload_document_with_pii_detection(self, s3_manager):
        """Test document upload with PII detection and masking."""
        # Mock PII detector
        mock_pii_detector = MagicMock()
        pii_finding = PIIFinding("EMAIL_ADDRESS", 0, 16, 0.9, "test@example.com")
        mock_pii_detector.mask_pii = AsyncMock(
            return_value=("[EMAIL_REDACTED] is my contact", [pii_finding])
        )
        mock_pii_detector.process_metadata_safely = AsyncMock(
            return_value=({"author": "John Doe"}, [])
        )
        mock_pii_detector.log_pii_detection = AsyncMock()
        mock_pii_detector.get_pii_warning.return_value = (
            "⚠️ PII detected and masked: EMAIL_ADDRESS: 1"
        )
        mock_pii_detector.masking_enabled = True

        # Inject PII detector
        s3_manager.pii_detector = mock_pii_detector

        # Mock S3 operations
        s3_manager.get_bucket_for_kb = AsyncMock(return_value="test-bucket")

        # Mock S3 client
        mock_s3_client = AsyncMock()
        mock_s3_client.put_object = MagicMock()
        s3_manager._get_s3_client = AsyncMock(return_value=mock_s3_client)

        result = await s3_manager.upload_document(
            knowledge_base_id="kb-123",
            document_content="test@example.com is my contact",
            document_name="test.txt",
            metadata={"author": "John Doe"},
        )

        # Verify PII detection was called
        mock_pii_detector.mask_pii.assert_called_once_with("test@example.com is my contact")
        mock_pii_detector.process_metadata_safely.assert_called_once_with({"author": "John Doe"})

        # Verify S3 operations
        s3_manager._get_s3_client.assert_called_once()
        mock_s3_client.put_object.assert_called_once()

        # Verify result includes security warnings
        assert result["success"] is True
        assert "security_warnings" in result
        assert len(result["security_warnings"]) == 1
        assert "EMAIL_ADDRESS: 1" in result["security_warnings"][0]

    @pytest.mark.asyncio
    async def test_upload_document_masking_disabled(self, s3_manager):
        """Test document upload with PII detection but masking disabled."""
        # Mock PII detector with masking disabled
        mock_pii_detector = MagicMock()
        pii_finding = PIIFinding("EMAIL_ADDRESS", 0, 16, 0.9, "test@example.com")
        mock_pii_detector.mask_pii = AsyncMock(
            return_value=("test@example.com is my contact", [pii_finding])
        )
        mock_pii_detector.process_metadata_safely = AsyncMock(
            return_value=({"author": "John Doe"}, [])
        )
        mock_pii_detector.log_pii_detection = AsyncMock()
        mock_pii_detector.get_pii_warning.return_value = "⚠️ PII detected: EMAIL_ADDRESS: 1"
        mock_pii_detector.masking_enabled = False

        # Inject PII detector
        s3_manager.pii_detector = mock_pii_detector

        # Mock S3 operations
        s3_manager.get_bucket_for_kb = AsyncMock(return_value="test-bucket")

        # Mock S3 client
        mock_s3_client = AsyncMock()
        mock_s3_client.put_object = MagicMock()
        s3_manager._get_s3_client = AsyncMock(return_value=mock_s3_client)

        result = await s3_manager.upload_document(
            knowledge_base_id="kb-123",
            document_content="test@example.com is my contact",
            document_name="test.txt",
            metadata={"author": "John Doe"},
        )

        # Verify original content was uploaded (not masked)
        call_args = mock_s3_client.put_object.call_args[1]
        assert call_args["Body"] == b"test@example.com is my contact"

        # Verify result includes security warnings
        assert result["success"] is True
        assert "security_warnings" in result

    @pytest.mark.asyncio
    async def test_upload_file_with_metadata_pii(self, s3_manager):
        """Test file upload with PII in metadata."""
        import base64

        # Mock PII detector
        mock_pii_detector = MagicMock()
        mock_pii_detector.process_metadata_safely = AsyncMock(
            return_value=(
                {"author": "[NAME_REDACTED]", "email": "[EMAIL_REDACTED]"},
                ["⚠️ Metadata PII detected: NAME: 1, EMAIL_ADDRESS: 1"],
            )
        )

        # Inject PII detector
        s3_manager.pii_detector = mock_pii_detector

        test_content = "Test file content"
        file_content_b64 = base64.b64encode(test_content.encode()).decode()

        s3_manager.get_bucket_for_kb = AsyncMock(return_value="test-bucket")

        # Mock S3 client
        mock_s3_client = AsyncMock()
        mock_s3_client.put_object = MagicMock()
        s3_manager._get_s3_client = AsyncMock(return_value=mock_s3_client)

        result = await s3_manager.upload_file(
            knowledge_base_id="kb-123",
            file_content=file_content_b64,
            file_name="test.txt",
            content_type="text/plain",
            metadata={"author": "John Doe", "email": "john@example.com"},
        )

        # Verify PII detection was called for metadata
        mock_pii_detector.process_metadata_safely.assert_called_once()

        # Verify result includes security warnings
        assert result["success"] is True
        assert "security_warnings" in result

    @pytest.mark.asyncio
    async def test_update_document_with_pii(self, s3_manager):
        """Test document update with PII detection."""
        # Mock PII detector
        mock_pii_detector = MagicMock()
        pii_finding = PIIFinding("PHONE_NUMBER", 0, 12, 0.8, "555-123-4567")
        mock_pii_detector.mask_pii = AsyncMock(
            return_value=("[PHONE_REDACTED] is the number", [pii_finding])
        )
        mock_pii_detector.process_metadata_safely = AsyncMock(
            return_value=({"updated": "true"}, [])
        )
        mock_pii_detector.log_pii_detection = AsyncMock()
        mock_pii_detector.get_pii_warning.return_value = (
            "⚠️ PII detected and masked: PHONE_NUMBER: 1"
        )
        mock_pii_detector.masking_enabled = True

        # Inject PII detector
        s3_manager.pii_detector = mock_pii_detector

        # Mock S3 operations
        s3_manager.get_bucket_for_kb = AsyncMock(return_value="test-bucket")

        # Mock S3 client
        mock_s3_client = AsyncMock()
        mock_s3_client.head_object = MagicMock(
            return_value={"Metadata": {"existing": "metadata"}, "ContentType": "text/plain"}
        )
        mock_s3_client.put_object = MagicMock()
        s3_manager._get_s3_client = AsyncMock(return_value=mock_s3_client)

        result = await s3_manager.update_document(
            knowledge_base_id="kb-123",
            document_s3_key="documents/test.txt",
            new_content="555-123-4567 is the number",
            metadata={"updated": "true"},
        )

        # Verify PII detection was called
        mock_pii_detector.mask_pii.assert_called_once_with("555-123-4567 is the number")

        # Verify result includes security warnings
        assert result["success"] is True
        assert "security_warnings" in result

    @pytest.mark.asyncio
    async def test_upload_document_no_pii_detector(self, s3_manager):
        """Test document upload without PII detector."""
        # No PII detector injection (pii_detector remains None)

        # Mock S3 operations
        s3_manager.get_bucket_for_kb = AsyncMock(return_value="test-bucket")

        # Mock S3 client
        mock_s3_client = AsyncMock()
        mock_s3_client.put_object = MagicMock()
        s3_manager._get_s3_client = AsyncMock(return_value=mock_s3_client)

        result = await s3_manager.upload_document(
            knowledge_base_id="kb-123",
            document_content="test@example.com is my contact",
            document_name="test.txt",
            metadata={"author": "John Doe"},
        )

        # Verify upload succeeded without PII detection
        assert result["success"] is True
        assert "security_warnings" not in result or len(result.get("security_warnings", [])) == 0

    @pytest.mark.asyncio
    async def test_no_pii_detected(self, s3_manager):
        """Test document upload when no PII is detected."""
        # Mock PII detector
        mock_pii_detector = MagicMock()
        mock_pii_detector.mask_pii = AsyncMock(return_value=("Clean content", []))
        mock_pii_detector.process_metadata_safely = AsyncMock(
            return_value=({"author": "John Doe"}, [])
        )

        # Inject PII detector
        s3_manager.pii_detector = mock_pii_detector

        # Mock S3 operations
        s3_manager.get_bucket_for_kb = AsyncMock(return_value="test-bucket")

        # Mock S3 client
        mock_s3_client = AsyncMock()
        mock_s3_client.put_object = MagicMock()
        s3_manager._get_s3_client = AsyncMock(return_value=mock_s3_client)

        result = await s3_manager.upload_document(
            knowledge_base_id="kb-123",
            document_content="Clean content",
            document_name="test.txt",
            metadata={"author": "John Doe"},
        )

        # Verify upload succeeded with no security warnings
        assert result["success"] is True
        assert "security_warnings" not in result or len(result.get("security_warnings", [])) == 0
