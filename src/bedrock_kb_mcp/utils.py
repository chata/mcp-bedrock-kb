from __future__ import annotations

"""
Utility functions for Bedrock Knowledge Base MCP server.

Bedrock KB MCP - Metadata Security Module

This module uses Microsoft Presidio for PII detection.
Presidio is licensed under MIT License.
Copyright (c) Microsoft Corporation. All rights reserved.

MIT License Terms:
https://github.com/microsoft/presidio/blob/main/LICENSE
"""

import hashlib
import json
import logging
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def validate_file_path(file_path: str | Path) -> Path:
    """Validate and convert file path to Path object.

    Args:
        file_path: File path as string or Path

    Returns:
        Validated Path object

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If path is invalid
    """
    try:
        path = Path(file_path).expanduser().resolve()
    except Exception as e:
        raise ValueError(f"Invalid file path: {file_path}") from e

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")

    return path


def format_error_response(error: Exception) -> str:
    """Format an error for user-friendly display.

    Args:
        error: Exception to format

    Returns:
        Formatted error message
    """
    error_type = type(error).__name__
    error_message = str(error)

    if hasattr(error, "response"):
        if "Error" in error.response:
            error_code = error.response["Error"].get("Code", "Unknown")
            error_message = error.response["Error"].get("Message", error_message)
            return f"AWS Error ({error_code}): {error_message}"

    return f"{error_type}: {error_message}"


def calculate_file_hash(file_path: Path, algorithm: str = "sha256") -> str:
    """Calculate hash of a file.

    Args:
        file_path: Path to file
        algorithm: Hash algorithm to use

    Returns:
        Hex digest of file hash
    """
    hash_func = hashlib.new(algorithm)

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)

    return hash_func.hexdigest()


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted size string
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0

    return f"{size_bytes:.2f} PB"


def get_file_metadata(file_path: Path) -> dict[str, Any]:
    """Get metadata for a file.

    Args:
        file_path: Path to file

    Returns:
        File metadata dictionary
    """
    stat = file_path.stat()
    mime_type, encoding = mimetypes.guess_type(str(file_path))

    return {
        "name": file_path.name,
        "path": str(file_path),
        "size": stat.st_size,
        "size_formatted": format_file_size(stat.st_size),
        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
        "mime_type": mime_type or "application/octet-stream",
        "encoding": encoding,
        "extension": file_path.suffix[1:] if file_path.suffix else None,
    }


def sanitize_s3_key(key: str) -> str:
    """Sanitize an S3 object key.

    Args:
        key: S3 object key

    Returns:
        Sanitized key
    """
    key = key.strip()

    key = key.replace("\\", "/")

    while "//" in key:
        key = key.replace("//", "/")

    key = key.lstrip("/")

    invalid_chars = ["<", ">", "|", ":", "*", "?", '"']
    for char in invalid_chars:
        key = key.replace(char, "_")

    return key


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an S3 URI into bucket and key.

    Args:
        uri: S3 URI (s3://bucket/key)

    Returns:
        Tuple of (bucket, key)

    Raises:
        ValueError: If URI is invalid
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")

    uri_parts = uri[5:].split("/", 1)

    if len(uri_parts) < 1 or not uri_parts[0]:
        raise ValueError(f"Invalid S3 URI: {uri}")

    bucket = uri_parts[0]
    key = uri_parts[1] if len(uri_parts) > 1 else ""

    return bucket, key


def chunk_text(text: str, chunk_size: int = 4000, overlap: int = 200) -> list[str]:
    """Split text into overlapping chunks.

    Args:
        text: Text to chunk
        chunk_size: Maximum size of each chunk
        overlap: Number of characters to overlap between chunks

    Returns:
        List of text chunks
    """
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0

    while start < len(text):
        end = min(start + chunk_size, len(text))

        if end < len(text):
            last_period = text.rfind(".", start, end)
            last_newline = text.rfind("\n", start, end)
            last_space = text.rfind(" ", start, end)

            break_point = max(last_period, last_newline, last_space)
            if break_point > start:
                end = break_point + 1

        chunks.append(text[start:end])

        start = end - overlap if end < len(text) else end

    return chunks


def validate_json(data: str | dict) -> tuple[bool, dict | None, str | None]:
    """Validate JSON data.

    Args:
        data: JSON string or dictionary

    Returns:
        Tuple of (is_valid, parsed_data, error_message)
    """
    if isinstance(data, dict):
        return True, data, None

    try:
        parsed = json.loads(data)
        return True, parsed, None
    except json.JSONDecodeError as e:
        return False, None, str(e)


def merge_metadata(*metadata_dicts: dict[str, Any | None]) -> dict[str, Any]:
    """Merge multiple metadata dictionaries.

    Args:
        *metadata_dicts: Variable number of metadata dictionaries

    Returns:
        Merged metadata dictionary
    """
    result = {}

    for metadata in metadata_dicts:
        if metadata:
            result.update(metadata)

    return result


def is_binary_file(file_path: Path, sample_size: int = 8192) -> bool:
    """Check if a file is binary.

    Args:
        file_path: Path to file
        sample_size: Number of bytes to sample

    Returns:
        True if file appears to be binary
    """
    try:
        with open(file_path, "rb") as f:
            sample = f.read(sample_size)

        if b"\x00" in sample:
            return True

        text_characters = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)))

        non_text = sample.translate(None, text_characters)

        if len(non_text) / len(sample) > 0.30:
            return True

        return False
    except Exception:
        return True


def create_s3_metadata_dict(metadata: dict[str, Any]) -> dict[str, str]:
    """Convert metadata dictionary to S3-compatible format.

    Args:
        metadata: Metadata dictionary

    Returns:
        S3-compatible metadata (string values only)
    """
    s3_metadata = {}

    for key, value in metadata.items():
        key = key.replace(" ", "-").lower()

        key = "".join(c for c in key if c.isalnum() or c in "-_")

        # Use tuple for broad compatibility across Python versions
        if isinstance(value, (list, dict)):  # noqa: UP038
            value = json.dumps(value)
        else:
            value = str(value)

        if len(value) > 2048:
            value = value[:2045] + "..."

        s3_metadata[key] = value

    return s3_metadata


def extract_document_type(file_path: str | Path) -> str:
    """Extract document type from file path.

    Args:
        file_path: Path to file

    Returns:
        Document type string
    """
    path = Path(file_path)
    extension = path.suffix[1:].lower() if path.suffix else ""

    type_mapping = {
        "txt": "text",
        "md": "markdown",
        "html": "html",
        "htm": "html",
        "pdf": "pdf",
        "doc": "word",
        "docx": "word",
        "xls": "excel",
        "xlsx": "excel",
        "ppt": "powerpoint",
        "pptx": "powerpoint",
        "json": "json",
        "xml": "xml",
        "csv": "csv",
        "py": "python",
        "js": "javascript",
        "java": "java",
        "cpp": "cpp",
        "c": "c",
        "cs": "csharp",
        "go": "go",
        "rs": "rust",
        "rb": "ruby",
        "php": "php",
        "swift": "swift",
        "kt": "kotlin",
        "yaml": "yaml",
        "yml": "yaml",
    }

    return type_mapping.get(extension, "unknown")
