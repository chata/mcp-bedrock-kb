from __future__ import annotations

"""
Tests for custom exception hierarchy.
"""

from unittest.mock import MagicMock

from src.bedrock_kb_mcp.exceptions import (
    AlertError,
    AuthenticationError,
    AWSServiceError,
    BedrockKBError,
    BedrockServiceError,
    ConcurrencyError,
    ConfigurationError,
    DocumentError,
    GDPRDeletionError,
    GDPRError,
    GDPRValidationError,
    KnowledgeBaseError,
    PIIDetectionError,
    PIIProcessingError,
    ResourceError,
    S3ServiceError,
    TimeoutError,
    ValidationError,
    handle_aws_error,
)


class TestBedrockKBError:
    """Test base exception class."""

    def test_basic_creation(self):
        """Test basic exception creation."""
        error = BedrockKBError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"
        assert error.details == {}

    def test_creation_with_details(self):
        """Test exception creation with details."""
        details = {"key": "value", "code": 123}
        error = BedrockKBError("Test error", details)
        assert "Details:" in str(error)
        assert error.details == details


class TestSpecificExceptions:
    """Test specific exception classes."""

    def test_configuration_error(self):
        """Test ConfigurationError."""
        error = ConfigurationError("Invalid config")
        assert isinstance(error, BedrockKBError)
        assert error.message == "Invalid config"

    def test_authentication_error(self):
        """Test AuthenticationError."""
        error = AuthenticationError("Auth failed")
        assert isinstance(error, BedrockKBError)
        assert error.message == "Auth failed"

    def test_aws_service_error(self):
        """Test AWSServiceError."""
        error = AWSServiceError("Service failed", "s3", "NoSuchBucket")
        assert isinstance(error, BedrockKBError)
        assert error.service_name == "s3"
        assert error.error_code == "NoSuchBucket"
        assert "AWS s3 Error (NoSuchBucket)" in str(error)

    def test_bedrock_service_error(self):
        """Test BedrockServiceError."""
        error = BedrockServiceError("Model not found", "ValidationException")
        assert isinstance(error, AWSServiceError)
        assert error.service_name == "Bedrock"
        assert error.error_code == "ValidationException"

    def test_s3_service_error(self):
        """Test S3ServiceError."""
        error = S3ServiceError("Bucket not found", "NoSuchBucket")
        assert isinstance(error, AWSServiceError)
        assert error.service_name == "S3"
        assert error.error_code == "NoSuchBucket"

    def test_knowledge_base_error(self):
        """Test KnowledgeBaseError."""
        error = KnowledgeBaseError("KB not found", "kb-123")
        assert isinstance(error, BedrockKBError)
        assert error.knowledge_base_id == "kb-123"
        assert "Knowledge Base ID: kb-123" in str(error)

    def test_document_error(self):
        """Test DocumentError."""
        error = DocumentError("Document too large", "test.pdf")
        assert isinstance(error, BedrockKBError)
        assert error.document_name == "test.pdf"
        assert "Document: test.pdf" in str(error)

    def test_pii_detection_error(self):
        """Test PIIDetectionError."""
        error = PIIDetectionError("Analysis failed", 1000)
        assert isinstance(error, BedrockKBError)
        assert error.text_length == 1000
        assert "Text length: 1000 chars" in str(error)

    def test_pii_processing_error(self):
        """Test PIIProcessingError."""
        error = PIIProcessingError("Masking failed", 500)
        assert isinstance(error, PIIDetectionError)

    def test_gdpr_deletion_error(self):
        """Test GDPRDeletionError."""
        error = GDPRDeletionError("Deletion failed", "req-123")
        assert isinstance(error, GDPRError)
        assert error.request_id == "req-123"
        assert "Request ID: req-123" in str(error)

    def test_gdpr_validation_error(self):
        """Test GDPRValidationError."""
        error = GDPRValidationError("Invalid request")
        assert isinstance(error, GDPRError)

    def test_alert_error(self):
        """Test AlertError."""
        error = AlertError("Alert send failed", "alert-123")
        assert isinstance(error, BedrockKBError)
        assert error.alert_id == "alert-123"
        assert "Alert ID: alert-123" in str(error)

    def test_validation_error(self):
        """Test ValidationError."""
        error = ValidationError("Invalid value", "email", "invalid-email")
        assert isinstance(error, BedrockKBError)
        assert error.field_name == "email"
        assert error.field_value == "invalid-email"
        assert "Field: email = invalid-email" in str(error)

    def test_timeout_error(self):
        """Test TimeoutError."""
        error = TimeoutError("Operation timed out", 30.0)
        assert isinstance(error, BedrockKBError)
        assert error.timeout_seconds == 30.0
        assert "Timeout: 30.0s" in str(error)

    def test_resource_error(self):
        """Test ResourceError."""
        error = ResourceError("Resource exhausted")
        assert isinstance(error, BedrockKBError)

    def test_concurrency_error(self):
        """Test ConcurrencyError."""
        error = ConcurrencyError("Deadlock detected")
        assert isinstance(error, BedrockKBError)


class TestHandleAWSError:
    """Test AWS error handling utility."""

    def test_handle_generic_error(self):
        """Test handling of generic errors."""
        original_error = Exception("Generic error")
        result = handle_aws_error(original_error, "s3")

        assert isinstance(result, S3ServiceError)
        assert result.message == "Generic error"
        assert result.service_name == "S3"
        assert result.error_code is None

    def test_handle_boto3_client_error(self):
        """Test handling of boto3 ClientError."""
        # Mock boto3 ClientError
        mock_error = MagicMock()
        mock_error.response = {
            "Error": {"Code": "NoSuchBucket", "Message": "The specified bucket does not exist"},
            "ResponseMetadata": {"RequestId": "req-123", "HTTPStatusCode": 404},
        }

        result = handle_aws_error(mock_error, "s3")

        assert isinstance(result, S3ServiceError)
        assert result.message == "The specified bucket does not exist"
        assert result.error_code == "NoSuchBucket"
        assert result.details["request_id"] == "req-123"
        assert result.details["http_status_code"] == 404

    def test_handle_bedrock_error(self):
        """Test handling of Bedrock-specific errors."""
        original_error = Exception("Model not found")
        result = handle_aws_error(original_error, "bedrock")

        assert isinstance(result, BedrockServiceError)
        assert result.service_name == "Bedrock"

    def test_handle_unknown_service_error(self):
        """Test handling of unknown service errors."""
        original_error = Exception("Service error")
        result = handle_aws_error(original_error, "unknown-service")

        assert isinstance(result, AWSServiceError)
        assert result.service_name == "unknown-service"
