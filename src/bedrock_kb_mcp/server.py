from __future__ import annotations

#!/usr/bin/env python3
"""MCP Server for Amazon Bedrock Knowledge Base with CRUD operations."""

import asyncio
import logging
from typing import Any

# Optional import for mcp; provide fallbacks for tests without mcp installed
try:
    from mcp.server import Server
    from mcp.server.models import InitializationOptions, ServerCapabilities
    from mcp.types import TextContent, Tool, ToolsCapability
except ImportError:  # Provide minimal shims for typing and test imports

    class Server:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            pass

        def list_tools(self):
            def decorator(func):
                return func

            return decorator

        def call_tool(self):
            def decorator(func):
                return func

            return decorator

        async def run(self, *args, **kwargs):
            raise RuntimeError("MCP runtime is not available in this environment")

    class InitializationOptions:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

    class ServerCapabilities:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

    class ToolsCapability:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

    class TextContent:  # type: ignore
        def __init__(self, type: str, text: str):
            self.type = type
            self.text = text

    class Tool:  # type: ignore
        def __init__(self, **kwargs):
            self.kwargs = kwargs


from .auth_manager import AuthManager
from .bedrock_client import BedrockClient
from .config_manager import ConfigManager
from .s3_manager import S3Manager
from .utils import format_error_response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BedrockKnowledgeBaseMCPServer:
    """MCP Server for Bedrock Knowledge Base operations."""

    def __init__(self):
        """Initialize the MCP server."""
        self.server = Server("bedrock-knowledge-base")
        self.config = ConfigManager()
        self.auth_manager = AuthManager(self.config)
        self.bedrock_client: BedrockClient | None = None
        self.s3_manager: S3Manager | None = None
        self.gdpr_manager = None
        self._setup_handlers()

    def _setup_handlers(self):
        """Set up MCP server handlers."""

        # Import at module level as attribute for test patching
        try:
            from security.gdpr_deletion import GDPRDeletionManager as _GDPRDeletionManager
        except Exception:
            _GDPRDeletionManager = None  # type: ignore
        # Expose for tests to patch
        global GDPRDeletionManager  # type: ignore
        GDPRDeletionManager = _GDPRDeletionManager  # type: ignore

        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """List all available tools."""
            return [
                Tool(
                    name="bedrock_kb_search",
                    description="Search for information in a Bedrock Knowledge Base",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "query": {
                                "type": "string",
                                "description": "The search query",
                            },
                            "num_results": {
                                "type": "integer",
                                "description": "Number of results to return",
                                "default": 5,
                            },
                            "search_type": {
                                "type": "string",
                                "enum": ["SEMANTIC", "HYBRID"],
                                "description": "Type of search",
                                "default": "HYBRID",
                            },
                        },
                        "required": ["knowledge_base_id", "query"],
                    },
                ),
                Tool(
                    name="bedrock_kb_query",
                    description="Query a Knowledge Base with RAG to generate an answer",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "question": {
                                "type": "string",
                                "description": "The question to answer",
                            },
                            "model_arn": {
                                "type": "string",
                                "description": "Foundation Model ARN to use",
                            },
                            "temperature": {
                                "type": "number",
                                "description": "Generation temperature",
                                "default": 0.1,
                            },
                            "max_tokens": {
                                "type": "integer",
                                "description": "Maximum tokens to generate",
                                "default": 2000,
                            },
                        },
                        "required": ["knowledge_base_id", "question"],
                    },
                ),
                Tool(
                    name="bedrock_kb_list",
                    description="List all available Knowledge Bases",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                    },
                ),
                Tool(
                    name="bedrock_kb_upload_document",
                    description="Upload a text document to a Knowledge Base",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "document_content": {
                                "type": "string",
                                "description": "The document content",
                            },
                            "document_name": {
                                "type": "string",
                                "description": "Name of the document",
                            },
                            "document_format": {
                                "type": "string",
                                "enum": ["txt", "md", "html"],
                                "description": "Document format",
                                "default": "txt",
                            },
                            "metadata": {
                                "type": "object",
                                "description": "Document metadata",
                            },
                            "folder_path": {
                                "type": "string",
                                "description": "S3 folder path",
                            },
                        },
                        "required": ["knowledge_base_id", "document_content", "document_name"],
                    },
                ),
                Tool(
                    name="bedrock_kb_upload_file",
                    description="Upload a file to a Knowledge Base",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "file_content": {
                                "type": "string",
                                "description": "Base64 encoded file content",
                            },
                            "file_name": {
                                "type": "string",
                                "description": "Name of the file with extension",
                            },
                            "content_type": {
                                "type": "string",
                                "description": "MIME type of the file (e.g., application/pdf, text/plain)",
                            },
                            "s3_key": {
                                "type": "string",
                                "description": "S3 object key (optional)",
                            },
                            "metadata": {
                                "type": "object",
                                "description": "Document metadata",
                            },
                        },
                        "required": [
                            "knowledge_base_id",
                            "file_content",
                            "file_name",
                            "content_type",
                        ],
                    },
                ),
                Tool(
                    name="bedrock_kb_update_document",
                    description="Update an existing document in a Knowledge Base",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "document_s3_key": {
                                "type": "string",
                                "description": "S3 object key of the document",
                            },
                            "new_content": {
                                "type": "string",
                                "description": "New document content",
                            },
                            "metadata": {
                                "type": "object",
                                "description": "Updated metadata",
                            },
                        },
                        "required": ["knowledge_base_id", "document_s3_key", "new_content"],
                    },
                ),
                Tool(
                    name="bedrock_kb_delete_document",
                    description="Delete a document from a Knowledge Base",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "document_s3_key": {
                                "type": "string",
                                "description": "S3 object key of the document",
                            },
                        },
                        "required": ["knowledge_base_id", "document_s3_key"],
                    },
                ),
                Tool(
                    name="bedrock_kb_list_documents",
                    description="List documents in a Knowledge Base",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "prefix": {
                                "type": "string",
                                "description": "S3 prefix to filter documents",
                            },
                            "max_items": {
                                "type": "integer",
                                "description": "Maximum items to return",
                                "default": 100,
                            },
                        },
                        "required": ["knowledge_base_id"],
                    },
                ),
                Tool(
                    name="bedrock_kb_sync_datasource",
                    description="Start a data source sync job",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "data_source_id": {
                                "type": "string",
                                "description": "The Data Source ID",
                            },
                            "description": {
                                "type": "string",
                                "description": "Job description",
                            },
                        },
                        "required": ["knowledge_base_id", "data_source_id"],
                    },
                ),
                Tool(
                    name="bedrock_kb_get_sync_status",
                    description="Get the status of a sync job",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "knowledge_base_id": {
                                "type": "string",
                                "description": "The Knowledge Base ID",
                            },
                            "data_source_id": {
                                "type": "string",
                                "description": "The Data Source ID",
                            },
                            "job_id": {
                                "type": "string",
                                "description": "Specific job ID",
                            },
                        },
                        "required": ["knowledge_base_id", "data_source_id"],
                    },
                ),
                Tool(
                    name="bedrock_kb_gdpr_deletion_request",
                    description="Create a GDPR deletion request for personal data",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "subject_identifiers": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of PII identifiers to delete (emails, phone numbers, etc.)",
                            },
                            "knowledge_base_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Knowledge Base IDs to process for deletion",
                            },
                            "request_id": {
                                "type": "string",
                                "description": "Optional custom request ID",
                            },
                        },
                        "required": ["subject_identifiers", "knowledge_base_ids"],
                    },
                ),
                Tool(
                    name="bedrock_kb_gdpr_execute_deletion",
                    description="Execute a GDPR deletion request",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "request_id": {
                                "type": "string",
                                "description": "GDPR deletion request ID to execute",
                            },
                        },
                        "required": ["request_id"],
                    },
                ),
                Tool(
                    name="bedrock_kb_gdpr_deletion_status",
                    description="Check the status of a GDPR deletion request",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "request_id": {
                                "type": "string",
                                "description": "GDPR deletion request ID to check",
                            },
                        },
                        "required": ["request_id"],
                    },
                ),
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
            """Handle tool calls."""
            try:
                if self.bedrock_client is None or self.s3_manager is None:
                    await self._initialize_clients()

                if name == "bedrock_kb_search":
                    result = await self.bedrock_client.search(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        query=arguments["query"],
                        num_results=arguments.get("num_results", 5),
                        search_type=arguments.get("search_type", "HYBRID"),
                    )
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_query":
                    result = await self.bedrock_client.query(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        question=arguments["question"],
                        model_arn=arguments.get("model_arn"),
                        temperature=arguments.get("temperature", 0.1),
                        max_tokens=arguments.get("max_tokens", 2000),
                    )
                    return [TextContent(type="text", text=result)]

                elif name == "bedrock_kb_list":
                    result = await self.bedrock_client.list_knowledge_bases()
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_upload_document":
                    result = await self.s3_manager.upload_document(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        document_content=arguments["document_content"],
                        document_name=arguments["document_name"],
                        document_format=arguments.get("document_format", "txt"),
                        metadata=arguments.get("metadata"),
                        folder_path=arguments.get("folder_path"),
                    )
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_upload_file":
                    result = await self.s3_manager.upload_file(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        file_content=arguments["file_content"],
                        file_name=arguments["file_name"],
                        content_type=arguments["content_type"],
                        s3_key=arguments.get("s3_key"),
                        metadata=arguments.get("metadata"),
                    )
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_update_document":
                    result = await self.s3_manager.update_document(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        document_s3_key=arguments["document_s3_key"],
                        new_content=arguments["new_content"],
                        metadata=arguments.get("metadata"),
                    )
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_delete_document":
                    result = await self.s3_manager.delete_document(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        document_s3_key=arguments["document_s3_key"],
                    )
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_list_documents":
                    result = await self.s3_manager.list_documents(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        prefix=arguments.get("prefix"),
                        max_items=arguments.get("max_items", 100),
                    )
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_sync_datasource":
                    result = await self.bedrock_client.start_ingestion_job(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        data_source_id=arguments["data_source_id"],
                        description=arguments.get("description"),
                    )
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_get_sync_status":
                    result = await self.bedrock_client.get_ingestion_job_status(
                        knowledge_base_id=arguments["knowledge_base_id"],
                        data_source_id=arguments["data_source_id"],
                        job_id=arguments.get("job_id"),
                    )
                    return [TextContent(type="text", text=str(result))]

                # GDPR related tools
                elif name == "bedrock_kb_gdpr_deletion_request":
                    if self.gdpr_manager is None:
                        if GDPRDeletionManager is None:
                            return [TextContent(type="text", text="GDPR functionality unavailable")]
                        session = await self.auth_manager.get_session()
                        self.gdpr_manager = GDPRDeletionManager(session, self.config)

                    request_id = await self.gdpr_manager.create_deletion_request(
                        subject_identifiers=arguments["subject_identifiers"],
                        knowledge_base_ids=arguments["knowledge_base_ids"],
                        request_id=arguments.get("request_id"),
                    )
                    return [
                        TextContent(
                            type="text", text=f"GDPR deletion request created: {request_id}"
                        )
                    ]

                elif name == "bedrock_kb_gdpr_execute_deletion":
                    if self.gdpr_manager is None:
                        if GDPRDeletionManager is None:
                            return [TextContent(type="text", text="GDPR functionality unavailable")]
                        session = await self.auth_manager.get_session()
                        self.gdpr_manager = GDPRDeletionManager(session, self.config)

                    result = await self.gdpr_manager.execute_deletion(arguments["request_id"])
                    return [TextContent(type="text", text=str(result))]

                elif name == "bedrock_kb_gdpr_deletion_status":
                    if self.gdpr_manager is None:
                        if GDPRDeletionManager is None:
                            return [TextContent(type="text", text="GDPR functionality unavailable")]
                        session = await self.auth_manager.get_session()
                        self.gdpr_manager = GDPRDeletionManager(session, self.config)

                    status = self.gdpr_manager.get_deletion_status(arguments["request_id"])
                    if status:
                        return [TextContent(type="text", text=str(status))]
                    else:
                        return [
                            TextContent(
                                type="text",
                                text=f"Deletion request {arguments['request_id']} not found",
                            )
                        ]

                else:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]

            except Exception as e:
                logger.error(f"Error in tool {name}: {e}")
                return [TextContent(type="text", text=format_error_response(e))]

    async def _initialize_clients(self):
        """Initialize AWS clients."""
        session = await self.auth_manager.get_session()
        self.bedrock_client = BedrockClient(session, self.config)
        self.s3_manager = S3Manager(session, self.config)

    async def run(self):
        """Run the MCP server."""
        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="bedrock-knowledge-base",
                    server_version="1.0.0",
                    capabilities=ServerCapabilities(tools=ToolsCapability(listChanged=True)),
                ),
            )


def main():
    """Main entry point."""
    server = BedrockKnowledgeBaseMCPServer()
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
