from __future__ import annotations

"""S3 manager for document operations in Knowledge Base."""

import logging
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from .concurrency import DocumentProcessor
from .interfaces import (
    AlertManagerInterface,
    AuthManagerInterface,
    BaseS3Manager,
    ConfigManagerInterface,
    ConnectionPoolInterface,
    PIIDetectorInterface,
)

# Import PII detector (with fallback)
try:
    import os
    import sys

    sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
    from security.pii_detector import pii_detector as default_pii_detector

    PII_DETECTOR_AVAILABLE = True
except ImportError:
    default_pii_detector = None
    PII_DETECTOR_AVAILABLE = False

logger = logging.getLogger(__name__)


class S3Manager(BaseS3Manager):
    """Manager for S3 operations related to Knowledge Base documents."""

    def __init__(
        self,
        auth_manager: AuthManagerInterface,
        config_manager: ConfigManagerInterface,
        pii_detector: PIIDetectorInterface | None = None,
        alert_manager: AlertManagerInterface | None = None,
        connection_pool: ConnectionPoolInterface | None = None,
    ):
        """Initialize S3 manager.

        Args:
            auth_manager: Authentication manager
            config_manager: Configuration manager
            pii_detector: Optional PII detector
            alert_manager: Optional alert manager
            connection_pool: Optional connection pool
        """
        super().__init__(auth_manager, config_manager, pii_detector, alert_manager, connection_pool)

        # Initialize document processor for concurrent operations
        max_concurrent = config_manager.get("s3.max_concurrent_uploads", 5)
        self.document_processor = DocumentProcessor(max_concurrent=max_concurrent)

        # Configuration
        self.default_bucket = config_manager.get("s3.default_bucket")
        self.upload_prefix = config_manager.get("s3.upload_prefix", "documents/")
        self.max_file_size_mb = config_manager.get("document_processing.max_file_size_mb", 50)
        self.supported_formats = config_manager.get(
            "document_processing.supported_formats", ["txt", "md", "html", "pdf", "docx"]
        )
        self.encoding = config_manager.get("document_processing.encoding", "utf-8")

        # Clients (will be initialized lazily)
        self._s3_client = None
        self._bedrock_agent = None

    async def _get_s3_client(self):
        """Get S3 client (with connection pooling if available)."""
        if self.connection_pool:
            return await self.connection_pool.get_connection("s3")

        if not self._s3_client:
            session = await self.auth_manager.get_session()
            region = self.config_manager.get_aws_region()
            self._s3_client = session.client("s3", region_name=region)

        return self._s3_client

    async def _get_bedrock_agent(self):
        """Get Bedrock agent client (with connection pooling if available)."""
        if self.connection_pool:
            return await self.connection_pool.get_connection("bedrock-agent")

        if not self._bedrock_agent:
            session = await self.auth_manager.get_session()
            region = self.config_manager.get_aws_region()
            self._bedrock_agent = session.client("bedrock-agent", region_name=region)

        return self._bedrock_agent

    async def _return_connection(self, service_name: str, client):
        """Return connection to pool if using connection pooling."""
        if self.connection_pool:
            await self.connection_pool.return_connection(service_name, client)

    async def get_bucket_for_kb(self, knowledge_base_id: str) -> str | None:
        """Get the S3 bucket associated with a Knowledge Base.

        Args:
            knowledge_base_id: The Knowledge Base ID

        Returns:
            S3 bucket name or None
        """
        try:
            bedrock_agent = await self._get_bedrock_agent()
            response = bedrock_agent.get_knowledge_base(knowledgeBaseId=knowledge_base_id)

            storage_config = response.get("knowledgeBase", {}).get("storageConfiguration", {})
            s3_config = (
                storage_config.get("opensearchServerlessConfiguration", {})
                or storage_config.get("pineconeConfiguration", {})
                or storage_config.get("rdsConfiguration", {})
            )

            data_sources = bedrock_agent.list_data_sources(knowledgeBaseId=knowledge_base_id)

            for ds in data_sources.get("dataSourceSummaries", []):
                ds_detail = bedrock_agent.get_data_source(
                    knowledgeBaseId=knowledge_base_id, dataSourceId=ds["dataSourceId"]
                )
                s3_config = (
                    ds_detail.get("dataSource", {})
                    .get("dataSourceConfiguration", {})
                    .get("s3Configuration", {})
                )

                if s3_config.get("bucketArn"):
                    bucket_name = s3_config["bucketArn"].split(":")[-1]
                    return bucket_name

            return self.default_bucket

        except ClientError as e:
            logger.error(f"Error getting bucket for Knowledge Base: {e}")
            return self.default_bucket
        finally:
            if "bedrock_agent" in locals():
                await self._return_connection("bedrock-agent", bedrock_agent)

    async def upload_document(
        self,
        knowledge_base_id: str,
        document_content: str,
        document_name: str,
        document_format: str = "txt",
        metadata: dict[str, Any | None] = None,
        folder_path: str | None = None,
    ) -> dict[str, Any]:
        """Upload a text document to S3 for Knowledge Base.

        Args:
            knowledge_base_id: The Knowledge Base ID
            document_content: Document content
            document_name: Name of the document
            document_format: Document format (txt, md, html)
            metadata: Document metadata
            folder_path: S3 folder path

        Returns:
            Upload result
        """
        try:
            bucket = await self.get_bucket_for_kb(knowledge_base_id)
            if not bucket:
                return {
                    "success": False,
                    "error": "Could not determine S3 bucket for Knowledge Base",
                }

            if document_format not in ["txt", "md", "html"]:
                return {
                    "success": False,
                    "error": f"Unsupported document format: {document_format}",
                }

            if not document_name.endswith(f".{document_format}"):
                document_name = f"{document_name}.{document_format}"

            prefix = folder_path or self.upload_prefix
            if not prefix.endswith("/"):
                prefix += "/"

            s3_key = f"{prefix}{document_name}"

            content_type = {"txt": "text/plain", "md": "text/markdown", "html": "text/html"}.get(
                document_format, "text/plain"
            )

            put_params = {
                "Bucket": bucket,
                "Key": s3_key,
                "Body": document_content.encode(self.encoding),
                "ContentType": content_type,
            }

            # PII detection and metadata processing
            warnings = []
            processed_metadata = metadata

            if metadata and self.pii_detector:
                (
                    processed_metadata,
                    metadata_warnings,
                ) = await self.pii_detector.process_metadata_safely(metadata)
                warnings.extend(metadata_warnings)

            # PII detection in document content
            if self.pii_detector:
                masked_content, content_findings = await self.pii_detector.mask_pii(
                    document_content
                )
                if content_findings:
                    content_warning = self.pii_detector.get_pii_warning(content_findings)
                    warnings.append(f"Document content: {content_warning}")

                    # Log PII detection
                    await self.pii_detector.log_pii_detection(
                        document_content,
                        content_findings,
                        f"upload_document({knowledge_base_id}/{document_name})",
                    )

                    # Use masked content if masking is enabled
                    if self.pii_detector.masking_enabled:
                        document_content = masked_content

            if processed_metadata:
                put_params["Metadata"] = {k: str(v) for k, v in processed_metadata.items()}

            s3_client = await self._get_s3_client()
            s3_client.put_object(**put_params)
            await self._return_connection("s3", s3_client)

            result = {
                "success": True,
                "bucket": bucket,
                "key": s3_key,
                "size": len(document_content.encode(self.encoding)),
                "metadata": processed_metadata,
                "message": f"Document uploaded successfully to s3://{bucket}/{s3_key}",
            }

            # Add security warnings
            if warnings:
                result["security_warnings"] = warnings

            return result

        except ClientError as e:
            logger.error(f"Error uploading document: {e}")
            return {"success": False, "error": str(e)}

    async def upload_file(
        self,
        knowledge_base_id: str,
        file_content: str,
        file_name: str,
        content_type: str,
        s3_key: str | None = None,
        metadata: dict[str, Any | None] = None,
    ) -> dict[str, Any]:
        """Upload a file to S3 for Knowledge Base.

        Args:
            knowledge_base_id: The Knowledge Base ID
            file_content: Base64 encoded file content
            file_name: Name of the file with extension
            content_type: MIME type of the file
            s3_key: S3 object key (optional)
            metadata: Document metadata

        Returns:
            Upload result
        """
        try:
            import base64

            # Decode base64 content
            try:
                file_data = base64.b64decode(file_content)
            except Exception as e:
                return {"success": False, "error": f"Invalid base64 content: {str(e)}"}

            # Check file size
            file_size_mb = len(file_data) / (1024 * 1024)
            if file_size_mb > self.max_file_size_mb:
                return {
                    "success": False,
                    "error": f"File size ({file_size_mb:.2f} MB) exceeds limit ({self.max_file_size_mb} MB)",
                }

            # Check file extension
            file_extension = Path(file_name).suffix[1:].lower()
            if file_extension not in self.supported_formats:
                return {"success": False, "error": f"Unsupported file format: {file_extension}"}

            bucket = await self.get_bucket_for_kb(knowledge_base_id)
            if not bucket:
                return {
                    "success": False,
                    "error": "Could not determine S3 bucket for Knowledge Base",
                }

            if not s3_key:
                s3_key = f"{self.upload_prefix}{file_name}"

            put_params = {
                "Bucket": bucket,
                "Key": s3_key,
                "Body": file_data,
                "ContentType": content_type,
            }

            # PII detection and metadata processing
            warnings = []
            processed_metadata = metadata

            if metadata and self.pii_detector:
                (
                    processed_metadata,
                    metadata_warnings,
                ) = await self.pii_detector.process_metadata_safely(metadata)
                warnings.extend(metadata_warnings)

            if processed_metadata:
                put_params["Metadata"] = {k: str(v) for k, v in processed_metadata.items()}

            s3_client = await self._get_s3_client()
            s3_client.put_object(**put_params)
            await self._return_connection("s3", s3_client)

            result = {
                "success": True,
                "bucket": bucket,
                "key": s3_key,
                "size_mb": file_size_mb,
                "content_type": content_type,
                "metadata": processed_metadata,
                "message": f"File uploaded successfully to s3://{bucket}/{s3_key}",
            }

            # Add security warnings
            if warnings:
                result["security_warnings"] = warnings

            return result

        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            return {"success": False, "error": str(e)}

    async def update_document(
        self,
        knowledge_base_id: str,
        document_s3_key: str,
        new_content: str,
        metadata: dict[str, Any | None] = None,
    ) -> dict[str, Any]:
        """Update an existing document in S3.

        Args:
            knowledge_base_id: The Knowledge Base ID
            document_s3_key: S3 object key of the document
            new_content: New document content
            metadata: Updated metadata

        Returns:
            Update result
        """
        try:
            bucket = await self.get_bucket_for_kb(knowledge_base_id)
            if not bucket:
                return {
                    "success": False,
                    "error": "Could not determine S3 bucket for Knowledge Base",
                }

            s3_client = await self._get_s3_client()
            try:
                existing = s3_client.head_object(Bucket=bucket, Key=document_s3_key)
                existing_metadata = existing.get("Metadata", {})
            except ClientError as e:
                await self._return_connection("s3", s3_client)
                if e.response["Error"]["Code"] == "404":
                    return {
                        "success": False,
                        "error": f"Document not found: s3://{bucket}/{document_s3_key}",
                    }
                raise

            file_extension = Path(document_s3_key).suffix[1:].lower()
            content_type = {
                "txt": "text/plain",
                "md": "text/markdown",
                "html": "text/html",
                "pdf": "application/pdf",
                "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            }.get(file_extension, existing.get("ContentType", "text/plain"))

            if metadata:
                existing_metadata.update(metadata)

            put_params = {
                "Bucket": bucket,
                "Key": document_s3_key,
                "Body": new_content.encode(self.encoding),
                "ContentType": content_type,
            }

            # PII detection and metadata processing
            warnings = []

            if metadata and self.pii_detector:
                (
                    processed_new_metadata,
                    metadata_warnings,
                ) = await self.pii_detector.process_metadata_safely(metadata)
                existing_metadata.update(processed_new_metadata)
                warnings.extend(metadata_warnings)

            # PII detection in document content
            if self.pii_detector:
                masked_content, content_findings = await self.pii_detector.mask_pii(new_content)
                if content_findings:
                    content_warning = self.pii_detector.get_pii_warning(content_findings)
                    warnings.append(f"Document content: {content_warning}")

                    # Log PII detection
                    await self.pii_detector.log_pii_detection(
                        new_content,
                        content_findings,
                        f"update_document({knowledge_base_id}/{document_s3_key})",
                    )

                    # Use masked content if masking is enabled
                    if self.pii_detector.masking_enabled:
                        new_content = masked_content

            if existing_metadata:
                put_params["Metadata"] = {k: str(v) for k, v in existing_metadata.items()}

            s3_client.put_object(**put_params)
            await self._return_connection("s3", s3_client)

            result = {
                "success": True,
                "bucket": bucket,
                "key": document_s3_key,
                "size": len(new_content.encode(self.encoding)),
                "metadata": existing_metadata,
                "message": f"Document updated successfully: s3://{bucket}/{document_s3_key}",
            }

            # Add security warnings
            if warnings:
                result["security_warnings"] = warnings

            return result

        except ClientError as e:
            logger.error(f"Error updating document: {e}")
            return {"success": False, "error": str(e)}

    async def delete_document(self, knowledge_base_id: str, document_s3_key: str) -> dict[str, Any]:
        """Delete a document from S3.

        Args:
            knowledge_base_id: The Knowledge Base ID
            document_s3_key: S3 object key of the document

        Returns:
            Deletion result
        """
        try:
            bucket = await self.get_bucket_for_kb(knowledge_base_id)
            if not bucket:
                return {
                    "success": False,
                    "error": "Could not determine S3 bucket for Knowledge Base",
                }

            s3_client = await self._get_s3_client()
            try:
                s3_client.head_object(Bucket=bucket, Key=document_s3_key)
            except ClientError as e:
                await self._return_connection("s3", s3_client)
                if e.response["Error"]["Code"] == "404":
                    return {
                        "success": False,
                        "error": f"Document not found: s3://{bucket}/{document_s3_key}",
                    }
                raise

            s3_client.delete_object(Bucket=bucket, Key=document_s3_key)
            await self._return_connection("s3", s3_client)

            return {
                "success": True,
                "bucket": bucket,
                "key": document_s3_key,
                "message": f"Document deleted successfully: s3://{bucket}/{document_s3_key}",
            }

        except ClientError as e:
            logger.error(f"Error deleting document: {e}")
            return {"success": False, "error": str(e)}

    async def list_documents(
        self, knowledge_base_id: str, prefix: str | None = None, max_items: int = 100
    ) -> list[dict[str, Any]]:
        """List documents in a Knowledge Base's S3 bucket.

        Args:
            knowledge_base_id: The Knowledge Base ID
            prefix: S3 prefix to filter documents
            max_items: Maximum items to return

        Returns:
            List of documents
        """
        try:
            bucket = await self.get_bucket_for_kb(knowledge_base_id)
            if not bucket:
                return []

            list_params = {"Bucket": bucket, "MaxKeys": max_items}

            if prefix:
                list_params["Prefix"] = prefix

            s3_client = await self._get_s3_client()
            response = s3_client.list_objects_v2(**list_params)

            documents = []
            for obj in response.get("Contents", []):
                try:
                    head_response = s3_client.head_object(Bucket=bucket, Key=obj["Key"])
                    metadata = head_response.get("Metadata", {})
                except Exception:
                    metadata = {}

                documents.append(
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "size_mb": round(obj["Size"] / (1024 * 1024), 2),
                        "last_modified": str(obj["LastModified"]),
                        "etag": obj.get("ETag", "").strip('"'),
                        "metadata": metadata,
                        "url": f"s3://{bucket}/{obj['Key']}",
                    }
                )

            await self._return_connection("s3", s3_client)
            return documents

        except ClientError as e:
            if "s3_client" in locals():
                await self._return_connection("s3", s3_client)
            logger.error(f"Error listing documents: {e}")
            return []
