"""Tests for S3Manager."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.bedrock_kb_mcp.s3_manager import S3Manager


@pytest.fixture
def mock_session():
    """Create a mock boto3 session."""
    session = MagicMock()
    session.client = MagicMock()
    return session


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    config = MagicMock()
    config.get = MagicMock(side_effect=lambda key, default=None: {
        "aws.region": "us-east-1",
        "s3.default_bucket": "test-bucket",
        "s3.upload_prefix": "documents/",
        "document_processing.max_file_size_mb": 50,
        "document_processing.supported_formats": ["txt", "md", "html", "pdf", "docx"],
        "document_processing.encoding": "utf-8"
    }.get(key, default))
    return config


@pytest.fixture
def s3_manager(mock_session, mock_config):
    """Create an S3Manager instance with mocks."""
    return S3Manager(mock_session, mock_config)


class TestS3Manager:
    """Test cases for S3Manager."""

    @pytest.mark.asyncio
    async def test_get_bucket_for_kb(self, s3_manager):
        """Test getting S3 bucket for Knowledge Base."""
        s3_manager.bedrock_agent.get_knowledge_base = MagicMock(
            return_value={
                "knowledgeBase": {
                    "storageConfiguration": {}
                }
            }
        )
        
        s3_manager.bedrock_agent.list_data_sources = MagicMock(
            return_value={
                "dataSourceSummaries": [
                    {"dataSourceId": "DS123"}
                ]
            }
        )
        
        s3_manager.bedrock_agent.get_data_source = MagicMock(
            return_value={
                "dataSource": {
                    "dataSourceConfiguration": {
                        "s3Configuration": {
                            "bucketArn": "arn:aws:s3:::kb-bucket"
                        }
                    }
                }
            }
        )
        
        bucket = await s3_manager.get_bucket_for_kb("KB123")
        assert bucket == "kb-bucket"

    @pytest.mark.asyncio
    async def test_get_bucket_for_kb_default(self, s3_manager):
        """Test getting default bucket when KB bucket not found."""
        s3_manager.bedrock_agent.get_knowledge_base = MagicMock(
            side_effect=ClientError(
                {"Error": {"Code": "ResourceNotFoundException"}},
                "get_knowledge_base"
            )
        )
        
        bucket = await s3_manager.get_bucket_for_kb("KB123")
        assert bucket == "test-bucket"

    @pytest.mark.asyncio
    async def test_upload_document_success(self, s3_manager):
        """Test successful document upload."""
        s3_manager.get_bucket_for_kb = MagicMock(return_value="test-bucket")
        s3_manager.s3_client.put_object = MagicMock()
        
        result = await s3_manager.upload_document(
            knowledge_base_id="KB123",
            document_content="Test content",
            document_name="test.txt",
            document_format="txt",
            metadata={"author": "test"}
        )
        
        assert result["success"] is True
        assert result["bucket"] == "test-bucket"
        assert result["key"] == "documents/test.txt"
        assert result["metadata"] == {"author": "test"}
        
        s3_manager.s3_client.put_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_upload_document_invalid_format(self, s3_manager):
        """Test document upload with invalid format."""
        s3_manager.get_bucket_for_kb = MagicMock(return_value="test-bucket")
        
        result = await s3_manager.upload_document(
            knowledge_base_id="KB123",
            document_content="Test content",
            document_name="test.exe",
            document_format="exe"
        )
        
        assert result["success"] is False
        assert "Unsupported document format" in result["error"]

    @pytest.mark.asyncio
    async def test_upload_file_success(self, s3_manager):
        """Test successful file upload."""
        with tempfile.NamedTemporaryFile(suffix=".txt", mode="w") as f:
            f.write("Test file content")
            f.flush()
            
            s3_manager.get_bucket_for_kb = MagicMock(return_value="test-bucket")
            s3_manager.s3_client.put_object = MagicMock()
            
            result = await s3_manager.upload_file(
                knowledge_base_id="KB123",
                file_path=Path(f.name),
                metadata={"type": "test"}
            )
            
            assert result["success"] is True
            assert result["bucket"] == "test-bucket"
            assert "message" in result

    @pytest.mark.asyncio
    async def test_upload_file_not_found(self, s3_manager):
        """Test file upload with nonexistent file."""
        result = await s3_manager.upload_file(
            knowledge_base_id="KB123",
            file_path=Path("/nonexistent/file.txt")
        )
        
        assert result["success"] is False
        assert "File not found" in result["error"]

    @pytest.mark.asyncio
    async def test_upload_file_size_limit(self, s3_manager):
        """Test file upload exceeding size limit."""
        with tempfile.NamedTemporaryFile(suffix=".txt") as f:
            f.write(b"x" * (51 * 1024 * 1024))
            f.flush()
            
            result = await s3_manager.upload_file(
                knowledge_base_id="KB123",
                file_path=Path(f.name)
            )
            
            assert result["success"] is False
            assert "exceeds limit" in result["error"]

    @pytest.mark.asyncio
    async def test_update_document_success(self, s3_manager):
        """Test successful document update."""
        s3_manager.get_bucket_for_kb = MagicMock(return_value="test-bucket")
        s3_manager.s3_client.head_object = MagicMock(
            return_value={
                "Metadata": {"existing": "metadata"},
                "ContentType": "text/plain"
            }
        )
        s3_manager.s3_client.put_object = MagicMock()
        
        result = await s3_manager.update_document(
            knowledge_base_id="KB123",
            document_s3_key="documents/test.txt",
            new_content="Updated content",
            metadata={"updated": "true"}
        )
        
        assert result["success"] is True
        assert result["key"] == "documents/test.txt"
        assert "existing" in result["metadata"]
        assert result["metadata"]["updated"] == "true"

    @pytest.mark.asyncio
    async def test_update_document_not_found(self, s3_manager):
        """Test updating nonexistent document."""
        s3_manager.get_bucket_for_kb = MagicMock(return_value="test-bucket")
        s3_manager.s3_client.head_object = MagicMock(
            side_effect=ClientError(
                {"Error": {"Code": "404"}},
                "head_object"
            )
        )
        
        result = await s3_manager.update_document(
            knowledge_base_id="KB123",
            document_s3_key="documents/nonexistent.txt",
            new_content="Content"
        )
        
        assert result["success"] is False
        assert "Document not found" in result["error"]

    @pytest.mark.asyncio
    async def test_delete_document_success(self, s3_manager):
        """Test successful document deletion."""
        s3_manager.get_bucket_for_kb = MagicMock(return_value="test-bucket")
        s3_manager.s3_client.head_object = MagicMock()
        s3_manager.s3_client.delete_object = MagicMock()
        
        result = await s3_manager.delete_document(
            knowledge_base_id="KB123",
            document_s3_key="documents/test.txt"
        )
        
        assert result["success"] is True
        assert result["key"] == "documents/test.txt"
        s3_manager.s3_client.delete_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_documents(self, s3_manager):
        """Test listing documents in S3."""
        s3_manager.get_bucket_for_kb = MagicMock(return_value="test-bucket")
        s3_manager.s3_client.list_objects_v2 = MagicMock(
            return_value={
                "Contents": [
                    {
                        "Key": "documents/file1.txt",
                        "Size": 1024,
                        "LastModified": "2024-01-01T00:00:00Z",
                        "ETag": "abc123"
                    },
                    {
                        "Key": "documents/file2.pdf",
                        "Size": 2048,
                        "LastModified": "2024-01-02T00:00:00Z",
                        "ETag": "def456"
                    }
                ]
            }
        )
        s3_manager.s3_client.head_object = MagicMock(
            return_value={"Metadata": {}}
        )
        
        result = await s3_manager.list_documents(
            knowledge_base_id="KB123",
            prefix="documents/",
            max_items=10
        )
        
        assert len(result) == 2
        assert result[0]["key"] == "documents/file1.txt"
        assert result[0]["size"] == 1024
        assert result[1]["key"] == "documents/file2.pdf"