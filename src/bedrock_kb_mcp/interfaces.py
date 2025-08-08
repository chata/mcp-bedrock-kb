from __future__ import annotations

"""
Interface definitions for Bedrock Knowledge Base MCP Server components.

This module defines protocols (interfaces) to enable loose coupling between
components and facilitate testing and extensibility.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Protocol


# Re-export common types
@dataclass
class PIIFinding:
    """Represents a PII detection finding."""

    entity_type: str
    start: int
    end: int
    score: float
    text: str


class PIIDetectorInterface(Protocol):
    """Protocol for PII detection implementations."""

    async def detect_pii(self, text: str) -> list[PIIFinding]:
        """
        Detect PII in the given text.

        Args:
            text: The text to analyze for PII

        Returns:
            List of PII findings

        Raises:
            PIIDetectionError: If PII detection fails
        """
        ...

    async def mask_pii(self, text: str, findings: list[PIIFinding | None] = None) -> str:
        """
        Mask PII in the given text.

        Args:
            text: The text to mask
            findings: Optional pre-detected PII findings

        Returns:
            Text with PII masked

        Raises:
            PIIProcessingError: If PII masking fails
        """
        ...

    def is_ready(self) -> bool:
        """Check if the PII detector is ready for use."""
        ...

    async def ensure_initialized(self) -> None:
        """Ensure the PII detector is properly initialized."""
        ...


class AlertManagerInterface(Protocol):
    """Protocol for alert management implementations."""

    async def send_alert(
        self,
        level: str,
        failure_mode: str,
        message: str,
        source: str = "system",
        metadata: dict[str, Any | None] = None,
    ) -> str:
        """
        Send an alert.

        Args:
            level: Alert level (info, warning, error, critical)
            failure_mode: Type of failure
            message: Alert message
            source: Alert source
            metadata: Additional metadata

        Returns:
            Alert ID

        Raises:
            AlertError: If alert sending fails
        """
        ...

    def get_active_alerts(self) -> list[dict[str, Any]]:
        """Get list of active alerts."""
        ...

    async def resolve_alert(self, alert_key: str, resolved_by: str = "system") -> bool:
        """Mark an alert as resolved."""
        ...


class AuthManagerInterface(Protocol):
    """Protocol for authentication management implementations."""

    async def get_session(self) -> Any:
        """
        Get an authenticated AWS session.

        Returns:
            Authenticated boto3 session

        Raises:
            AuthenticationError: If authentication fails
        """
        ...

    async def refresh_credentials(self) -> None:
        """Refresh AWS credentials if possible."""
        ...

    def is_authenticated(self) -> bool:
        """Check if currently authenticated."""
        ...


class S3ManagerInterface(Protocol):
    """Protocol for S3 management implementations."""

    async def upload_document(
        self,
        knowledge_base_id: str,
        document_content: str,
        document_name: str,
        document_format: str = "txt",
        metadata: dict[str, Any | None] = None,
        folder_path: str | None = None,
    ) -> dict[str, Any]:
        """
        Upload a document to S3.

        Args:
            knowledge_base_id: Knowledge base ID
            document_content: Document content
            document_name: Document name
            document_format: Document format
            metadata: Document metadata
            folder_path: S3 folder path

        Returns:
            Upload result

        Raises:
            S3ServiceError: If upload fails
            DocumentError: If document processing fails
        """
        ...

    async def delete_document(self, knowledge_base_id: str, document_key: str) -> dict[str, Any]:
        """
        Delete a document from S3.

        Args:
            knowledge_base_id: Knowledge base ID
            document_key: S3 document key

        Returns:
            Deletion result

        Raises:
            S3ServiceError: If deletion fails
        """
        ...

    async def list_documents(
        self, knowledge_base_id: str, prefix: str | None = None, max_items: int | None = None
    ) -> list[dict[str, Any]]:
        """
        List documents in S3.

        Args:
            knowledge_base_id: Knowledge base ID
            prefix: S3 prefix filter
            max_items: Maximum number of items

        Returns:
            List of documents

        Raises:
            S3ServiceError: If listing fails
        """
        ...


class BedrockClientInterface(Protocol):
    """Protocol for Bedrock client implementations."""

    async def search_knowledge_base(
        self,
        knowledge_base_id: str,
        query: str,
        max_results: int = 10,
        filter_criteria: dict[str, Any | None] = None,
    ) -> dict[str, Any]:
        """
        Search a knowledge base.

        Args:
            knowledge_base_id: Knowledge base ID
            query: Search query
            max_results: Maximum results
            filter_criteria: Optional filters

        Returns:
            Search results

        Raises:
            BedrockServiceError: If search fails
            KnowledgeBaseError: If knowledge base access fails
        """
        ...

    async def generate_response(
        self,
        knowledge_base_id: str,
        query: str,
        model_arn: str | None = None,
        max_tokens: int = 2000,
        temperature: float = 0.1,
    ) -> dict[str, Any]:
        """
        Generate a response using RAG.

        Args:
            knowledge_base_id: Knowledge base ID
            query: User query
            model_arn: Model ARN
            max_tokens: Maximum tokens
            temperature: Generation temperature

        Returns:
            Generated response

        Raises:
            BedrockServiceError: If generation fails
        """
        ...

    async def list_knowledge_bases(self) -> list[dict[str, Any]]:
        """
        List available knowledge bases.

        Returns:
            List of knowledge bases

        Raises:
            BedrockServiceError: If listing fails
        """
        ...


class GDPRManagerInterface(Protocol):
    """Protocol for GDPR management implementations."""

    async def create_deletion_request(
        self,
        subject_identifiers: list[str],
        knowledge_base_ids: list[str],
        requestor_info: dict[str, Any | None] = None,
    ) -> str:
        """
        Create a GDPR deletion request.

        Args:
            subject_identifiers: Subject identifiers (emails, etc.)
            knowledge_base_ids: Knowledge bases to delete from
            requestor_info: Information about requestor

        Returns:
            Request ID

        Raises:
            GDPRValidationError: If request validation fails
            GDPRDeletionError: If request creation fails
        """
        ...

    async def execute_deletion(self, request_id: str) -> dict[str, Any]:
        """
        Execute a GDPR deletion request.

        Args:
            request_id: Deletion request ID

        Returns:
            Deletion result

        Raises:
            GDPRDeletionError: If deletion fails
        """
        ...

    async def get_deletion_status(self, request_id: str) -> dict[str, Any]:
        """
        Get status of a deletion request.

        Args:
            request_id: Request ID

        Returns:
            Status information

        Raises:
            GDPRDeletionError: If status check fails
        """
        ...


class ConfigManagerInterface(Protocol):
    """Protocol for configuration management implementations."""

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        ...

    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        ...

    def load_from_file(self, file_path: str) -> None:
        """Load configuration from file."""
        ...

    def get_aws_region(self) -> str:
        """Get AWS region."""
        ...

    def get_aws_profile(self) -> str | None:
        """Get AWS profile."""
        ...


class ConnectionPoolInterface(Protocol):
    """Protocol for connection pooling implementations."""

    async def get_connection(self, service_name: str) -> Any:
        """
        Get a connection from the pool.

        Args:
            service_name: Name of the service (s3, bedrock, etc.)

        Returns:
            Service client connection

        Raises:
            ResourceError: If connection cannot be obtained
        """
        ...

    async def return_connection(self, service_name: str, connection: Any) -> None:
        """
        Return a connection to the pool.

        Args:
            service_name: Name of the service
            connection: Connection to return
        """
        ...

    async def close_all(self) -> None:
        """Close all connections in the pool."""
        ...

    def get_stats(self) -> dict[str, Any]:
        """Get connection pool statistics."""
        ...


# Abstract base classes for implementations requiring inheritance


class BaseS3Manager(ABC):
    """Base class for S3 manager implementations."""

    def __init__(
        self,
        auth_manager: AuthManagerInterface,
        config_manager: ConfigManagerInterface,
        pii_detector: PIIDetectorInterface | None = None,
        alert_manager: AlertManagerInterface | None = None,
        connection_pool: ConnectionPoolInterface | None = None,
    ):
        self.auth_manager = auth_manager
        self.config_manager = config_manager
        self.pii_detector = pii_detector
        self.alert_manager = alert_manager
        self.connection_pool = connection_pool

    @abstractmethod
    async def upload_document(self, *args, **kwargs) -> dict[str, Any]:
        """Upload document implementation."""
        pass

    @abstractmethod
    async def delete_document(self, *args, **kwargs) -> dict[str, Any]:
        """Delete document implementation."""
        pass


class BaseBedrockClient(ABC):
    """Base class for Bedrock client implementations."""

    def __init__(
        self,
        auth_manager: AuthManagerInterface,
        config_manager: ConfigManagerInterface,
        connection_pool: ConnectionPoolInterface | None = None,
    ):
        self.auth_manager = auth_manager
        self.config_manager = config_manager
        self.connection_pool = connection_pool

    @abstractmethod
    async def search_knowledge_base(self, *args, **kwargs) -> dict[str, Any]:
        """Search knowledge base implementation."""
        pass

    @abstractmethod
    async def generate_response(self, *args, **kwargs) -> dict[str, Any]:
        """Generate response implementation."""
        pass
