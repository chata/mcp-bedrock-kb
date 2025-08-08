from __future__ import annotations

"""
Custom exception hierarchy for Bedrock Knowledge Base MCP Server.

This module defines a comprehensive exception hierarchy to provide better
error handling and debugging capabilities throughout the application.
"""

from typing import Any


class BedrockKBError(Exception):
    """Base exception class for all Bedrock KB MCP Server errors."""

    def __init__(self, message: str, details: dict[str, Any | None] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} (Details: {self.details})"
        return self.message


class ConfigurationError(BedrockKBError):
    """Raised when there are configuration-related errors."""

    pass


class AuthenticationError(BedrockKBError):
    """Raised when authentication fails."""

    pass


class AWSServiceError(BedrockKBError):
    """Base class for AWS service-related errors."""

    def __init__(
        self,
        message: str,
        service_name: str,
        error_code: str | None = None,
        details: dict[str, Any | None] = None,
    ):
        super().__init__(message, details)
        self.service_name = service_name
        self.error_code = error_code

    def __str__(self) -> str:
        base_msg = f"AWS {self.service_name} Error"
        if self.error_code:
            base_msg += f" ({self.error_code})"
        base_msg += f": {self.message}"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class BedrockServiceError(AWSServiceError):
    """Raised when Bedrock service operations fail."""

    def __init__(
        self, message: str, error_code: str | None = None, details: dict[str, Any | None] = None
    ):
        super().__init__(message, "Bedrock", error_code, details)


class S3ServiceError(AWSServiceError):
    """Raised when S3 service operations fail."""

    def __init__(
        self, message: str, error_code: str | None = None, details: dict[str, Any | None] = None
    ):
        super().__init__(message, "S3", error_code, details)


class KnowledgeBaseError(BedrockKBError):
    """Raised when knowledge base operations fail."""

    def __init__(
        self,
        message: str,
        knowledge_base_id: str | None = None,
        details: dict[str, Any | None] = None,
    ):
        super().__init__(message, details)
        self.knowledge_base_id = knowledge_base_id

    def __str__(self) -> str:
        base_msg = self.message
        if self.knowledge_base_id:
            base_msg += f" (Knowledge Base ID: {self.knowledge_base_id})"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class DocumentError(BedrockKBError):
    """Raised when document operations fail."""

    def __init__(
        self, message: str, document_name: str | None = None, details: dict[str, Any | None] = None
    ):
        super().__init__(message, details)
        self.document_name = document_name

    def __str__(self) -> str:
        base_msg = self.message
        if self.document_name:
            base_msg += f" (Document: {self.document_name})"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class PIIDetectionError(BedrockKBError):
    """Raised when PII detection operations fail."""

    def __init__(
        self, message: str, text_length: int | None = None, details: dict[str, Any | None] = None
    ):
        super().__init__(message, details)
        self.text_length = text_length

    def __str__(self) -> str:
        base_msg = f"PII Detection Error: {self.message}"
        if self.text_length:
            base_msg += f" (Text length: {self.text_length} chars)"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class PIIProcessingError(PIIDetectionError):
    """Raised when PII processing (masking/anonymization) fails."""

    pass


class GDPRError(BedrockKBError):
    """Base class for GDPR-related errors."""

    pass


class GDPRDeletionError(GDPRError):
    """Raised when GDPR deletion operations fail."""

    def __init__(
        self, message: str, request_id: str | None = None, details: dict[str, Any | None] = None
    ):
        super().__init__(message, details)
        self.request_id = request_id

    def __str__(self) -> str:
        base_msg = f"GDPR Deletion Error: {self.message}"
        if self.request_id:
            base_msg += f" (Request ID: {self.request_id})"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class GDPRValidationError(GDPRError):
    """Raised when GDPR request validation fails."""

    pass


class AlertError(BedrockKBError):
    """Raised when alert system operations fail."""

    def __init__(
        self, message: str, alert_id: str | None = None, details: dict[str, Any | None] = None
    ):
        super().__init__(message, details)
        self.alert_id = alert_id

    def __str__(self) -> str:
        base_msg = f"Alert Error: {self.message}"
        if self.alert_id:
            base_msg += f" (Alert ID: {self.alert_id})"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class ValidationError(BedrockKBError):
    """Raised when input validation fails."""

    def __init__(
        self,
        message: str,
        field_name: str | None = None,
        field_value: Any | None = None,
        details: dict[str, Any | None] = None,
    ):
        super().__init__(message, details)
        self.field_name = field_name
        self.field_value = field_value

    def __str__(self) -> str:
        base_msg = f"Validation Error: {self.message}"
        if self.field_name:
            base_msg += f" (Field: {self.field_name}"
            if self.field_value is not None:
                base_msg += f" = {self.field_value}"
            base_msg += ")"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class ResourceError(BedrockKBError):
    """Raised when resource-related operations fail."""

    pass


class TimeoutError(BedrockKBError):
    """Raised when operations timeout."""

    def __init__(
        self,
        message: str,
        timeout_seconds: float | None = None,
        details: dict[str, Any | None] = None,
    ):
        super().__init__(message, details)
        self.timeout_seconds = timeout_seconds

    def __str__(self) -> str:
        base_msg = f"Timeout Error: {self.message}"
        if self.timeout_seconds:
            base_msg += f" (Timeout: {self.timeout_seconds}s)"
        if self.details:
            base_msg += f" (Details: {self.details})"
        return base_msg


class ConcurrencyError(BedrockKBError):
    """Raised when concurrency-related issues occur."""

    pass


def handle_aws_error(error: Exception, service_name: str) -> BedrockKBError:
    """
    Convert AWS boto3 errors to our custom exception hierarchy.

    Args:
        error: The original AWS error
        service_name: Name of the AWS service (e.g., 'bedrock', 's3')

    Returns:
        Appropriate custom exception
    """
    error_message = str(error)
    error_code = None
    details = {}

    # Extract error details if it's a boto3 ClientError
    if hasattr(error, "response") and "Error" in error.response:
        error_info = error.response["Error"]
        error_code = error_info.get("Code", "Unknown")
        error_message = error_info.get("Message", error_message)
        details.update(
            {
                "request_id": error.response.get("ResponseMetadata", {}).get("RequestId"),
                "http_status_code": error.response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode"
                ),
            }
        )

    # Map to specific service errors
    if service_name.lower() == "bedrock":
        return BedrockServiceError(error_message, error_code, details)
    elif service_name.lower() == "s3":
        return S3ServiceError(error_message, error_code, details)
    else:
        return AWSServiceError(error_message, service_name, error_code, details)
