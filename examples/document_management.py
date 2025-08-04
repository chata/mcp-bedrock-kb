#!/usr/bin/env python3
"""
Document management examples for Bedrock Knowledge Base MCP Server.

This script demonstrates CRUD operations for managing documents in a
Knowledge Base, including uploading, updating, deleting, and listing documents.
"""

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Dict


class MockMCPClient:
    """Mock MCP client for demonstration purposes."""
    
    def __init__(self):
        """Initialize the mock client."""
        self.tools_called = []
        self.documents = {
            "documents/aws-lambda-guide.md": {
                "content": "AWS Lambda comprehensive guide...",
                "metadata": {"category": "compute", "version": "1.0"}
            },
            "documents/pricing-overview.txt": {
                "content": "AWS pricing structure overview...",
                "metadata": {"category": "pricing", "version": "1.2"}
            }
        }
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """Mock tool call implementation."""
        self.tools_called.append((tool_name, arguments))
        
        if tool_name == "bedrock_kb_upload_document":
            doc_name = arguments["document_name"]
            key = f"documents/{doc_name}"
            self.documents[key] = {
                "content": arguments["document_content"],
                "metadata": arguments.get("metadata", {})
            }
            return json.dumps({
                "success": True,
                "bucket": "kb-test-bucket",
                "key": key,
                "size": len(arguments["document_content"]),
                "metadata": arguments.get("metadata", {}),
                "message": f"Document uploaded successfully to s3://kb-test-bucket/{key}"
            })
        
        elif tool_name == "bedrock_kb_upload_file":
            file_path = arguments["file_path"]
            s3_key = arguments.get("s3_key", f"documents/{Path(file_path).name}")
            return json.dumps({
                "success": True,
                "bucket": "kb-test-bucket",
                "key": s3_key,
                "size_mb": 0.1,
                "content_type": "text/plain",
                "message": f"File uploaded successfully to s3://kb-test-bucket/{s3_key}"
            })
        
        elif tool_name == "bedrock_kb_update_document":
            doc_key = arguments["document_s3_key"]
            if doc_key in self.documents:
                self.documents[doc_key]["content"] = arguments["new_content"]
                if arguments.get("metadata"):
                    self.documents[doc_key]["metadata"].update(arguments["metadata"])
                return json.dumps({
                    "success": True,
                    "bucket": "kb-test-bucket",
                    "key": doc_key,
                    "size": len(arguments["new_content"]),
                    "message": f"Document updated successfully: s3://kb-test-bucket/{doc_key}"
                })
            else:
                return json.dumps({
                    "success": False,
                    "error": f"Document not found: s3://kb-test-bucket/{doc_key}"
                })
        
        elif tool_name == "bedrock_kb_delete_document":
            doc_key = arguments["document_s3_key"]
            if doc_key in self.documents:
                del self.documents[doc_key]
                return json.dumps({
                    "success": True,
                    "bucket": "kb-test-bucket",
                    "key": doc_key,
                    "message": f"Document deleted successfully: s3://kb-test-bucket/{doc_key}"
                })
            else:
                return json.dumps({
                    "success": False,
                    "error": f"Document not found: s3://kb-test-bucket/{doc_key}"
                })
        
        elif tool_name == "bedrock_kb_list_documents":
            prefix = arguments.get("prefix", "")
            max_items = arguments.get("max_items", 100)
            
            filtered_docs = [
                {
                    "key": key,
                    "size": len(doc["content"]),
                    "size_mb": round(len(doc["content"]) / (1024 * 1024), 2),
                    "last_modified": "2024-01-01T12:00:00Z",
                    "etag": "abc123",
                    "metadata": doc["metadata"],
                    "url": f"s3://kb-test-bucket/{key}"
                }
                for key, doc in self.documents.items()
                if key.startswith(prefix)
            ][:max_items]
            
            return json.dumps(filtered_docs)
        
        elif tool_name == "bedrock_kb_sync_datasource":
            return json.dumps({
                "success": True,
                "jobId": "JOB123456789",
                "knowledgeBaseId": arguments["knowledge_base_id"],
                "dataSourceId": arguments["data_source_id"],
                "status": "STARTING",
                "startedAt": "2024-01-01T12:00:00Z"
            })
        
        elif tool_name == "bedrock_kb_get_sync_status":
            return json.dumps({
                "jobId": "JOB123456789",
                "status": "COMPLETE",
                "startedAt": "2024-01-01T12:00:00Z",
                "updatedAt": "2024-01-01T12:05:00Z",
                "statistics": {
                    "numberOfDocumentsScanned": 10,
                    "numberOfDocumentsIndexed": 9,
                    "numberOfDocumentsFailed": 1,
                    "numberOfDocumentsDeleted": 0
                }
            })
        
        else:
            return f"Mock response for {tool_name}"


async def upload_text_document(client: MockMCPClient, knowledge_base_id: str):
    """Upload a text document to Knowledge Base."""
    print("📄 Uploading text document...")
    
    document_content = """
# AWS Lambda Best Practices

## Performance Optimization
- Keep deployment packages small
- Use provisioned concurrency for predictable latency
- Optimize memory allocation based on CPU requirements

## Cost Management
- Monitor invocation patterns
- Use ARM-based Graviton2 processors when possible
- Implement proper timeout settings

## Security
- Follow principle of least privilege for IAM roles
- Use environment variables for configuration
- Enable AWS X-Ray tracing for monitoring
"""
    
    response = await client.call_tool("bedrock_kb_upload_document", {
        "knowledge_base_id": knowledge_base_id,
        "document_content": document_content.strip(),
        "document_name": "lambda-best-practices",
        "document_format": "md",
        "metadata": {
            "category": "best-practices",
            "service": "lambda",
            "author": "DevOps Team",
            "version": "1.0",
            "tags": ["performance", "cost", "security"]
        },
        "folder_path": "guides/"
    })
    
    result = json.loads(response)
    
    if result.get("success"):
        print(f"✅ Document uploaded successfully!")
        print(f"   Location: {result['message'].split(': ')[1]}")
        print(f"   Size: {result['size']} bytes")
        print(f"   Metadata: {json.dumps(result['metadata'], indent=2)}")
    else:
        print(f"❌ Upload failed: {result.get('error')}")
    
    print()


async def upload_file_example(client: MockMCPClient, knowledge_base_id: str):
    """Upload a file to Knowledge Base."""
    print("📁 Uploading file...")
    
    # Create a temporary file for demonstration
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("This is a sample document for Knowledge Base upload.\n")
        f.write("It contains information about cloud computing concepts.\n")
        f.write("Topics covered: scalability, reliability, cost optimization.")
        temp_file_path = f.name
    
    try:
        response = await client.call_tool("bedrock_kb_upload_file", {
            "knowledge_base_id": knowledge_base_id,
            "file_path": temp_file_path,
            "s3_key": "documents/cloud-concepts.txt",
            "metadata": {
                "type": "educational",
                "topic": "cloud-computing",
                "difficulty": "beginner"
            }
        })
        
        result = json.loads(response)
        
        if result.get("success"):
            print(f"✅ File uploaded successfully!")
            print(f"   Location: {result['message'].split(': ')[1]}")
            print(f"   Size: {result['size_mb']} MB")
            print(f"   Content Type: {result['content_type']}")
        else:
            print(f"❌ Upload failed: {result.get('error')}")
    
    finally:
        # Clean up temporary file
        Path(temp_file_path).unlink()
    
    print()


async def update_document_example(client: MockMCPClient, knowledge_base_id: str):
    """Update an existing document."""
    print("✏️ Updating document...")
    
    updated_content = """
# AWS Lambda Best Practices (Updated)

## Performance Optimization
- Keep deployment packages small (< 50MB for direct upload)
- Use provisioned concurrency for predictable latency
- Optimize memory allocation based on CPU requirements
- Implement connection pooling for database connections

## Cost Management
- Monitor invocation patterns with CloudWatch
- Use ARM-based Graviton2 processors when possible (up to 34% better price-performance)
- Implement proper timeout settings
- Consider Lambda@Edge for global distribution

## Security (Updated)
- Follow principle of least privilege for IAM roles
- Use environment variables for configuration (encrypt sensitive data)
- Enable AWS X-Ray tracing for monitoring
- Implement VPC security groups when needed
- Use AWS Secrets Manager for sensitive configuration

## New: Observability
- Implement structured logging
- Use custom metrics for business KPIs
- Set up alerting for error rates and latency
"""
    
    response = await client.call_tool("bedrock_kb_update_document", {
        "knowledge_base_id": knowledge_base_id,
        "document_s3_key": "documents/lambda-best-practices.md",
        "new_content": updated_content.strip(),
        "metadata": {
            "version": "2.0",
            "last_updated": "2024-01-15",
            "changelog": "Added observability section, expanded security and cost guidance"
        }
    })
    
    result = json.loads(response)
    
    if result.get("success"):
        print(f"✅ Document updated successfully!")
        print(f"   Location: {result['message'].split(': ')[1]}")
        print(f"   New size: {result['size']} bytes")
    else:
        print(f"❌ Update failed: {result.get('error')}")
    
    print()


async def list_documents_example(client: MockMCPClient, knowledge_base_id: str):
    """List documents in the Knowledge Base."""
    print("📋 Listing documents...")
    
    response = await client.call_tool("bedrock_kb_list_documents", {
        "knowledge_base_id": knowledge_base_id,
        "prefix": "documents/",
        "max_items": 20
    })
    
    documents = json.loads(response)
    
    print(f"Found {len(documents)} documents:")
    print()
    
    for doc in documents:
        print(f"📄 {doc['key']}")
        print(f"   Size: {doc['size_mb']} MB ({doc['size']} bytes)")
        print(f"   Modified: {doc['last_modified']}")
        print(f"   ETag: {doc['etag']}")
        
        if doc.get('metadata'):
            print(f"   Metadata:")
            for key, value in doc['metadata'].items():
                print(f"     {key}: {value}")
        
        print(f"   URL: {doc['url']}")
        print()


async def batch_operations_example(client: MockMCPClient, knowledge_base_id: str):
    """Demonstrate batch document operations."""
    print("🔄 Batch operations example...")
    
    # Upload multiple documents
    documents = [
        {
            "name": "ec2-instance-types.md",
            "content": "# EC2 Instance Types\n\nOverview of AWS EC2 instance families...",
            "metadata": {"service": "ec2", "category": "reference"}
        },
        {
            "name": "s3-storage-classes.md",
            "content": "# S3 Storage Classes\n\nGuide to S3 storage options...",
            "metadata": {"service": "s3", "category": "reference"}
        },
        {
            "name": "rds-maintenance.txt",
            "content": "RDS maintenance best practices and scheduling guidelines.",
            "metadata": {"service": "rds", "category": "maintenance"}
        }
    ]
    
    print("Uploading multiple documents...")
    
    for doc in documents:
        response = await client.call_tool("bedrock_kb_upload_document", {
            "knowledge_base_id": knowledge_base_id,
            "document_content": doc["content"],
            "document_name": doc["name"],
            "metadata": doc["metadata"]
        })
        
        result = json.loads(response)
        if result.get("success"):
            print(f"  ✅ {doc['name']} uploaded")
        else:
            print(f"  ❌ {doc['name']} failed: {result.get('error')}")
    
    print()


async def document_versioning_example(client: MockMCPClient, knowledge_base_id: str):
    """Demonstrate document versioning strategy."""
    print("🔖 Document versioning example...")
    
    # Initial version
    print("Creating initial version...")
    v1_content = "# API Documentation v1.0\n\nBasic API endpoints and usage."
    
    await client.call_tool("bedrock_kb_upload_document", {
        "knowledge_base_id": knowledge_base_id,
        "document_content": v1_content,
        "document_name": "api-docs-v1.md",
        "metadata": {
            "version": "1.0",
            "status": "current",
            "created": "2024-01-01"
        }
    })
    
    # Updated version
    print("Creating updated version...")
    v2_content = """# API Documentation v2.0

## New Features
- Authentication endpoints
- Rate limiting
- Pagination support

## Basic Endpoints
Updated API endpoints and usage examples.
"""
    
    await client.call_tool("bedrock_kb_upload_document", {
        "knowledge_base_id": knowledge_base_id,
        "document_content": v2_content,
        "document_name": "api-docs-v2.md",
        "metadata": {
            "version": "2.0",
            "status": "current",
            "created": "2024-01-15",
            "previous_version": "api-docs-v1.md"
        }
    })
    
    # Mark old version as archived
    await client.call_tool("bedrock_kb_update_document", {
        "knowledge_base_id": knowledge_base_id,
        "document_s3_key": "documents/api-docs-v1.md",
        "new_content": v1_content,
        "metadata": {
            "version": "1.0",
            "status": "archived",
            "archived_date": "2024-01-15",
            "superseded_by": "api-docs-v2.md"
        }
    })
    
    print("✅ Document versioning completed")
    print()


async def sync_knowledge_base(client: MockMCPClient, knowledge_base_id: str, data_source_id: str):
    """Sync the Knowledge Base after document changes."""
    print("🔄 Syncing Knowledge Base...")
    
    # Start sync job
    response = await client.call_tool("bedrock_kb_sync_datasource", {
        "knowledge_base_id": knowledge_base_id,
        "data_source_id": data_source_id,
        "description": "Sync after document updates"
    })
    
    sync_result = json.loads(response)
    
    if sync_result.get("success"):
        job_id = sync_result["jobId"]
        print(f"✅ Sync job started: {job_id}")
        print(f"   Status: {sync_result['status']}")
        print(f"   Started: {sync_result['startedAt']}")
        
        # Check sync status
        print("\nChecking sync progress...")
        
        status_response = await client.call_tool("bedrock_kb_get_sync_status", {
            "knowledge_base_id": knowledge_base_id,
            "data_source_id": data_source_id,
            "job_id": job_id
        })
        
        status_result = json.loads(status_response)
        
        print(f"Job Status: {status_result['status']}")
        print(f"Documents processed: {status_result['statistics']['numberOfDocumentsScanned']}")
        print(f"Documents indexed: {status_result['statistics']['numberOfDocumentsIndexed']}")
        print(f"Documents failed: {status_result['statistics']['numberOfDocumentsFailed']}")
        
    else:
        print(f"❌ Sync failed: {sync_result.get('error')}")
    
    print()


async def cleanup_example(client: MockMCPClient, knowledge_base_id: str):
    """Demonstrate document cleanup operations."""
    print("🧹 Cleanup operations...")
    
    # List documents to clean up
    documents_to_delete = [
        "documents/old-guide.txt",
        "documents/deprecated-api.md",
        "documents/temp-notes.txt"
    ]
    
    for doc_key in documents_to_delete:
        print(f"Deleting {doc_key}...")
        
        response = await client.call_tool("bedrock_kb_delete_document", {
            "knowledge_base_id": knowledge_base_id,
            "document_s3_key": doc_key
        })
        
        result = json.loads(response)
        
        if result.get("success"):
            print(f"  ✅ Deleted successfully")
        else:
            print(f"  ℹ️ {result.get('error')}")
    
    print()


async def main():
    """Main function demonstrating document management."""
    print("📚 Bedrock Knowledge Base MCP Server - Document Management Examples")
    print("=" * 70)
    print()
    
    # Initialize mock client
    client = MockMCPClient()
    
    # Example Knowledge Base and Data Source IDs
    knowledge_base_id = "KB123456789"
    data_source_id = "DS987654321"
    
    try:
        print(f"Using Knowledge Base: {knowledge_base_id}")
        print(f"Using Data Source: {data_source_id}")
        print("=" * 70)
        print()
        
        # 1. Upload text document
        await upload_text_document(client, knowledge_base_id)
        
        # 2. Upload file
        await upload_file_example(client, knowledge_base_id)
        
        # 3. Update document
        await update_document_example(client, knowledge_base_id)
        
        # 4. List documents
        await list_documents_example(client, knowledge_base_id)
        
        # 5. Batch operations
        await batch_operations_example(client, knowledge_base_id)
        
        # 6. Document versioning
        await document_versioning_example(client, knowledge_base_id)
        
        # 7. Sync Knowledge Base
        await sync_knowledge_base(client, knowledge_base_id, data_source_id)
        
        # 8. Cleanup operations
        await cleanup_example(client, knowledge_base_id)
        
        print("✅ Document management examples completed!")
        print()
        print(f"Total tool calls made: {len(client.tools_called)}")
        
        # Show summary of operations
        tool_counts = {}
        for tool_name, _ in client.tools_called:
            tool_counts[tool_name] = tool_counts.get(tool_name, 0) + 1
        
        print("\nOperations summary:")
        for tool, count in tool_counts.items():
            print(f"  {tool}: {count}")
    
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    # Run the examples
    asyncio.run(main())