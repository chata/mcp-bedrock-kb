"""Tests for utility functions."""

import json
import tempfile
from pathlib import Path

import pytest

from src.bedrock_kb_mcp.utils import (
    calculate_file_hash,
    chunk_text,
    create_s3_metadata_dict,
    extract_document_type,
    format_error_response,
    format_file_size,
    get_file_metadata,
    is_binary_file,
    merge_metadata,
    parse_s3_uri,
    sanitize_s3_key,
    validate_file_path,
    validate_json,
)


class TestValidateFilePath:
    """Test cases for validate_file_path function."""

    def test_valid_file_path(self):
        """Test with valid file path."""
        with tempfile.NamedTemporaryFile() as f:
            path = validate_file_path(f.name)
            assert isinstance(path, Path)
            assert path.exists()

    def test_path_object_input(self):
        """Test with Path object input."""
        with tempfile.NamedTemporaryFile() as f:
            path = validate_file_path(Path(f.name))
            assert isinstance(path, Path)
            assert path.exists()

    def test_nonexistent_file(self):
        """Test with nonexistent file."""
        with pytest.raises(FileNotFoundError):
            validate_file_path("/nonexistent/file.txt")

    def test_directory_path(self):
        """Test with directory path."""
        with tempfile.TemporaryDirectory() as d:
            with pytest.raises(ValueError):
                validate_file_path(d)


class TestFormatErrorResponse:
    """Test cases for format_error_response function."""

    def test_simple_exception(self):
        """Test formatting simple exception."""
        error = ValueError("Test error")
        result = format_error_response(error)
        assert result == "ValueError: Test error"

    def test_aws_error(self):
        """Test formatting AWS error."""
        error = Exception("AWS Error")
        error.response = {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access denied to resource"
            }
        }
        result = format_error_response(error)
        assert "AccessDenied" in result
        assert "Access denied" in result


class TestFileOperations:
    """Test cases for file operation functions."""

    def test_calculate_file_hash(self):
        """Test file hash calculation."""
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write("Test content")
            f.flush()
            
            hash1 = calculate_file_hash(Path(f.name))
            hash2 = calculate_file_hash(Path(f.name))
            
            assert hash1 == hash2
            assert len(hash1) == 64

    def test_format_file_size(self):
        """Test file size formatting."""
        assert format_file_size(100) == "100.00 B"
        assert format_file_size(1024) == "1.00 KB"
        assert format_file_size(1024 * 1024) == "1.00 MB"
        assert format_file_size(1024 * 1024 * 1024) == "1.00 GB"

    def test_get_file_metadata(self):
        """Test getting file metadata."""
        with tempfile.NamedTemporaryFile(suffix=".txt", mode="w") as f:
            f.write("Test content")
            f.flush()
            
            metadata = get_file_metadata(Path(f.name))
            
            assert metadata["size"] > 0
            assert metadata["extension"] == "txt"
            assert metadata["mime_type"] == "text/plain"
            assert "modified" in metadata
            assert "created" in metadata

    def test_is_binary_file(self):
        """Test binary file detection."""
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write("Text content\n")
            f.flush()
            assert not is_binary_file(Path(f.name))
        
        with tempfile.NamedTemporaryFile(mode="wb") as f:
            f.write(b"\x00\x01\x02\x03")
            f.flush()
            assert is_binary_file(Path(f.name))


class TestS3Operations:
    """Test cases for S3-related functions."""

    def test_sanitize_s3_key(self):
        """Test S3 key sanitization."""
        assert sanitize_s3_key("/path/to//file.txt") == "path/to/file.txt"
        assert sanitize_s3_key("path\\to\\file.txt") == "path/to/file.txt"
        assert sanitize_s3_key("file<>name.txt") == "file__name.txt"
        assert sanitize_s3_key("  key  ") == "key"

    def test_parse_s3_uri(self):
        """Test S3 URI parsing."""
        bucket, key = parse_s3_uri("s3://mybucket/path/to/file.txt")
        assert bucket == "mybucket"
        assert key == "path/to/file.txt"
        
        bucket, key = parse_s3_uri("s3://mybucket")
        assert bucket == "mybucket"
        assert key == ""
        
        with pytest.raises(ValueError):
            parse_s3_uri("http://mybucket/file.txt")
        
        with pytest.raises(ValueError):
            parse_s3_uri("s3://")

    def test_create_s3_metadata_dict(self):
        """Test S3 metadata dictionary creation."""
        metadata = {
            "Document Type": "PDF",
            "author": "Test Author",
            "tags": ["tag1", "tag2"],
            "count": 123
        }
        
        s3_metadata = create_s3_metadata_dict(metadata)
        
        assert s3_metadata["document-type"] == "PDF"
        assert s3_metadata["author"] == "Test Author"
        assert s3_metadata["tags"] == '["tag1", "tag2"]'
        assert s3_metadata["count"] == "123"


class TestTextOperations:
    """Test cases for text operation functions."""

    def test_chunk_text_small(self):
        """Test chunking small text."""
        text = "Small text"
        chunks = chunk_text(text, chunk_size=100)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_chunk_text_large(self):
        """Test chunking large text."""
        text = "A" * 1000 + ". " + "B" * 1000 + ". " + "C" * 1000
        chunks = chunk_text(text, chunk_size=1500, overlap=100)
        
        assert len(chunks) > 1
        assert all(len(chunk) <= 1500 for chunk in chunks)

    def test_chunk_text_with_newlines(self):
        """Test chunking text with newlines."""
        text = "Line 1\n" + "A" * 100 + "\nLine 2\n" + "B" * 100
        chunks = chunk_text(text, chunk_size=150, overlap=20)
        
        assert len(chunks) >= 2


class TestJsonOperations:
    """Test cases for JSON operation functions."""

    def test_validate_json_string(self):
        """Test JSON string validation."""
        valid, data, error = validate_json('{"key": "value"}')
        assert valid is True
        assert data == {"key": "value"}
        assert error is None
        
        valid, data, error = validate_json("invalid json")
        assert valid is False
        assert data is None
        assert error is not None

    def test_validate_json_dict(self):
        """Test JSON dictionary validation."""
        valid, data, error = validate_json({"key": "value"})
        assert valid is True
        assert data == {"key": "value"}
        assert error is None


class TestMetadataOperations:
    """Test cases for metadata operation functions."""

    def test_merge_metadata(self):
        """Test metadata merging."""
        meta1 = {"a": 1, "b": 2}
        meta2 = {"b": 3, "c": 4}
        meta3 = None
        meta4 = {"d": 5}
        
        result = merge_metadata(meta1, meta2, meta3, meta4)
        
        assert result == {"a": 1, "b": 3, "c": 4, "d": 5}

    def test_extract_document_type(self):
        """Test document type extraction."""
        assert extract_document_type("file.txt") == "text"
        assert extract_document_type("doc.md") == "markdown"
        assert extract_document_type("/path/to/file.pdf") == "pdf"
        assert extract_document_type("code.py") == "python"
        assert extract_document_type("unknown.xyz") == "unknown"
        assert extract_document_type("no_extension") == "unknown"