"""S3 manager for document operations in Knowledge Base."""

import json
import logging
import mimetypes
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Manager:
    """Manager for S3 operations related to Knowledge Base documents."""

    def __init__(self, session: boto3.Session, config: Any):
        """Initialize S3 manager.
        
        Args:
            session: AWS boto3 session
            config: Configuration manager instance
        """
        self.config = config
        self.s3_client = session.client("s3", region_name=config.get("aws.region", "us-east-1"))
        self.bedrock_agent = session.client(
            "bedrock-agent", region_name=config.get("aws.region", "us-east-1")
        )
        
        self.default_bucket = config.get("s3.default_bucket")
        self.upload_prefix = config.get("s3.upload_prefix", "documents/")
        self.max_file_size_mb = config.get("document_processing.max_file_size_mb", 50)
        self.supported_formats = config.get(
            "document_processing.supported_formats",
            ["txt", "md", "html", "pdf", "docx"]
        )
        self.encoding = config.get("document_processing.encoding", "utf-8")

    async def get_bucket_for_kb(self, knowledge_base_id: str) -> Optional[str]:
        """Get the S3 bucket associated with a Knowledge Base.
        
        Args:
            knowledge_base_id: The Knowledge Base ID
            
        Returns:
            S3 bucket name or None
        """
        try:
            response = self.bedrock_agent.get_knowledge_base(
                knowledgeBaseId=knowledge_base_id
            )
            
            storage_config = response.get("knowledgeBase", {}).get("storageConfiguration", {})
            s3_config = storage_config.get("opensearchServerlessConfiguration", {}) or \
                       storage_config.get("pineconeConfiguration", {}) or \
                       storage_config.get("rdsConfiguration", {})
            
            data_sources = self.bedrock_agent.list_data_sources(
                knowledgeBaseId=knowledge_base_id
            )
            
            for ds in data_sources.get("dataSourceSummaries", []):
                ds_detail = self.bedrock_agent.get_data_source(
                    knowledgeBaseId=knowledge_base_id,
                    dataSourceId=ds["dataSourceId"]
                )
                s3_config = ds_detail.get("dataSource", {}).get(
                    "dataSourceConfiguration", {}
                ).get("s3Configuration", {})
                
                if s3_config.get("bucketArn"):
                    bucket_name = s3_config["bucketArn"].split(":")[-1]
                    return bucket_name
            
            return self.default_bucket
            
        except ClientError as e:
            logger.error(f"Error getting bucket for Knowledge Base: {e}")
            return self.default_bucket

    async def upload_document(
        self,
        knowledge_base_id: str,
        document_content: str,
        document_name: str,
        document_format: str = "txt",
        metadata: Optional[Dict[str, Any]] = None,
        folder_path: Optional[str] = None
    ) -> Dict[str, Any]:
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
                    "error": "Could not determine S3 bucket for Knowledge Base"
                }
            
            if document_format not in ["txt", "md", "html"]:
                return {
                    "success": False,
                    "error": f"Unsupported document format: {document_format}"
                }
            
            if not document_name.endswith(f".{document_format}"):
                document_name = f"{document_name}.{document_format}"
            
            prefix = folder_path or self.upload_prefix
            if not prefix.endswith("/"):
                prefix += "/"
            
            s3_key = f"{prefix}{document_name}"
            
            content_type = {
                "txt": "text/plain",
                "md": "text/markdown",
                "html": "text/html"
            }.get(document_format, "text/plain")
            
            put_params = {
                "Bucket": bucket,
                "Key": s3_key,
                "Body": document_content.encode(self.encoding),
                "ContentType": content_type
            }
            
            if metadata:
                put_params["Metadata"] = {
                    k: str(v) for k, v in metadata.items()
                }
            
            self.s3_client.put_object(**put_params)
            
            return {
                "success": True,
                "bucket": bucket,
                "key": s3_key,
                "size": len(document_content.encode(self.encoding)),
                "metadata": metadata,
                "message": f"Document uploaded successfully to s3://{bucket}/{s3_key}"
            }
            
        except ClientError as e:
            logger.error(f"Error uploading document: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def upload_file(
        self,
        knowledge_base_id: str,
        file_content: str,
        file_name: str,
        content_type: str,
        s3_key: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
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
                return {
                    "success": False,
                    "error": f"Invalid base64 content: {str(e)}"
                }
            
            # Check file size
            file_size_mb = len(file_data) / (1024 * 1024)
            if file_size_mb > self.max_file_size_mb:
                return {
                    "success": False,
                    "error": f"File size ({file_size_mb:.2f} MB) exceeds limit ({self.max_file_size_mb} MB)"
                }
            
            # Check file extension
            file_extension = Path(file_name).suffix[1:].lower()
            if file_extension not in self.supported_formats:
                return {
                    "success": False,
                    "error": f"Unsupported file format: {file_extension}"
                }
            
            bucket = await self.get_bucket_for_kb(knowledge_base_id)
            if not bucket:
                return {
                    "success": False,
                    "error": "Could not determine S3 bucket for Knowledge Base"
                }
            
            if not s3_key:
                s3_key = f"{self.upload_prefix}{file_name}"
            
            put_params = {
                "Bucket": bucket,
                "Key": s3_key,
                "Body": file_data,
                "ContentType": content_type
            }
            
            if metadata:
                put_params["Metadata"] = {
                    k: str(v) for k, v in metadata.items()
                }
            
            self.s3_client.put_object(**put_params)
            
            return {
                "success": True,
                "bucket": bucket,
                "key": s3_key,
                "size_mb": file_size_mb,
                "content_type": content_type,
                "metadata": metadata,
                "message": f"File uploaded successfully to s3://{bucket}/{s3_key}"
            }
            
        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def update_document(
        self,
        knowledge_base_id: str,
        document_s3_key: str,
        new_content: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
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
                    "error": "Could not determine S3 bucket for Knowledge Base"
                }
            
            try:
                existing = self.s3_client.head_object(Bucket=bucket, Key=document_s3_key)
                existing_metadata = existing.get("Metadata", {})
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return {
                        "success": False,
                        "error": f"Document not found: s3://{bucket}/{document_s3_key}"
                    }
                raise
            
            file_extension = Path(document_s3_key).suffix[1:].lower()
            content_type = {
                "txt": "text/plain",
                "md": "text/markdown",
                "html": "text/html",
                "pdf": "application/pdf",
                "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            }.get(file_extension, existing.get("ContentType", "text/plain"))
            
            if metadata:
                existing_metadata.update(metadata)
            
            put_params = {
                "Bucket": bucket,
                "Key": document_s3_key,
                "Body": new_content.encode(self.encoding),
                "ContentType": content_type
            }
            
            if existing_metadata:
                put_params["Metadata"] = {
                    k: str(v) for k, v in existing_metadata.items()
                }
            
            self.s3_client.put_object(**put_params)
            
            return {
                "success": True,
                "bucket": bucket,
                "key": document_s3_key,
                "size": len(new_content.encode(self.encoding)),
                "metadata": existing_metadata,
                "message": f"Document updated successfully: s3://{bucket}/{document_s3_key}"
            }
            
        except ClientError as e:
            logger.error(f"Error updating document: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def delete_document(
        self,
        knowledge_base_id: str,
        document_s3_key: str
    ) -> Dict[str, Any]:
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
                    "error": "Could not determine S3 bucket for Knowledge Base"
                }
            
            try:
                self.s3_client.head_object(Bucket=bucket, Key=document_s3_key)
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return {
                        "success": False,
                        "error": f"Document not found: s3://{bucket}/{document_s3_key}"
                    }
                raise
            
            self.s3_client.delete_object(Bucket=bucket, Key=document_s3_key)
            
            return {
                "success": True,
                "bucket": bucket,
                "key": document_s3_key,
                "message": f"Document deleted successfully: s3://{bucket}/{document_s3_key}"
            }
            
        except ClientError as e:
            logger.error(f"Error deleting document: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def list_documents(
        self,
        knowledge_base_id: str,
        prefix: Optional[str] = None,
        max_items: int = 100
    ) -> List[Dict[str, Any]]:
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
            
            list_params = {
                "Bucket": bucket,
                "MaxKeys": max_items
            }
            
            if prefix:
                list_params["Prefix"] = prefix
            
            response = self.s3_client.list_objects_v2(**list_params)
            
            documents = []
            for obj in response.get("Contents", []):
                try:
                    head_response = self.s3_client.head_object(
                        Bucket=bucket,
                        Key=obj["Key"]
                    )
                    metadata = head_response.get("Metadata", {})
                except:
                    metadata = {}
                
                documents.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "size_mb": round(obj["Size"] / (1024 * 1024), 2),
                    "last_modified": str(obj["LastModified"]),
                    "etag": obj.get("ETag", "").strip('"'),
                    "metadata": metadata,
                    "url": f"s3://{bucket}/{obj['Key']}"
                })
            
            return documents
            
        except ClientError as e:
            logger.error(f"Error listing documents: {e}")
            return []