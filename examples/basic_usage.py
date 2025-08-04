#!/usr/bin/env python3
"""
Basic usage examples for Bedrock Knowledge Base MCP Server.

This script demonstrates how to interact with the MCP server for basic
search and query operations.
"""

import asyncio
import json
from typing import Any, Dict

# Note: In a real MCP client implementation, you would use the actual MCP client
# This is a simplified example to demonstrate the expected API calls


class MockMCPClient:
    """Mock MCP client for demonstration purposes."""
    
    def __init__(self):
        """Initialize the mock client."""
        self.tools_called = []
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """Mock tool call implementation."""
        self.tools_called.append((tool_name, arguments))
        
        # Return mock responses based on tool name
        if tool_name == "bedrock_kb_list":
            return json.dumps([
                {
                    "id": "KB123456789",
                    "name": "AWS Documentation",
                    "description": "AWS service documentation",
                    "status": "ACTIVE"
                },
                {
                    "id": "KB987654321", 
                    "name": "Company Policies",
                    "description": "Internal company policies and procedures",
                    "status": "ACTIVE"
                }
            ])
        
        elif tool_name == "bedrock_kb_search":
            return json.dumps({
                "success": True,
                "results": [
                    {
                        "content": "AWS Lambda is a serverless compute service...",
                        "location": {"s3": {"uri": "s3://kb-bucket/aws-lambda-guide.pdf"}},
                        "score": 0.95,
                        "metadata": {"category": "compute"}
                    },
                    {
                        "content": "Lambda pricing is based on requests and duration...",
                        "location": {"s3": {"uri": "s3://kb-bucket/pricing-guide.txt"}},
                        "score": 0.89,
                        "metadata": {"category": "pricing"}
                    }
                ],
                "count": 2
            })
        
        elif tool_name == "bedrock_kb_query":
            return "AWS Lambda is a serverless compute service that runs your code in response to events and automatically manages the underlying compute resources. You pay only for the compute time you consume - there is no charge when your code is not running. Lambda pricing is based on the number of requests and the duration of your code execution."
        
        else:
            return f"Mock response for {tool_name}"


async def list_knowledge_bases(client: MockMCPClient):
    """List all available Knowledge Bases."""
    print("📚 Listing available Knowledge Bases...")
    
    response = await client.call_tool("bedrock_kb_list", {})
    knowledge_bases = json.loads(response)
    
    print(f"Found {len(knowledge_bases)} Knowledge Bases:")
    for kb in knowledge_bases:
        print(f"  • {kb['name']} ({kb['id']})")
        print(f"    Status: {kb['status']}")
        print(f"    Description: {kb['description']}")
        print()
    
    return knowledge_bases


async def search_knowledge_base(client: MockMCPClient, knowledge_base_id: str):
    """Search for information in a Knowledge Base."""
    print("🔍 Searching Knowledge Base...")
    
    search_query = "AWS Lambda pricing"
    
    response = await client.call_tool("bedrock_kb_search", {
        "knowledge_base_id": knowledge_base_id,
        "query": search_query,
        "num_results": 5,
        "search_type": "HYBRID"
    })
    
    results = json.loads(response)
    
    if results.get("success"):
        print(f"Search results for '{search_query}':")
        print(f"Found {results['count']} results")
        print()
        
        for i, result in enumerate(results["results"], 1):
            print(f"Result {i}:")
            print(f"  Content: {result['content'][:100]}...")
            print(f"  Score: {result['score']}")
            print(f"  Source: {result['location']['s3']['uri']}")
            if result.get("metadata"):
                print(f"  Metadata: {result['metadata']}")
            print()
    else:
        print(f"Search failed: {results.get('error', 'Unknown error')}")


async def query_with_rag(client: MockMCPClient, knowledge_base_id: str):
    """Query Knowledge Base with RAG to generate an answer."""
    print("🤖 Querying with RAG...")
    
    question = "How does AWS Lambda pricing work?"
    
    response = await client.call_tool("bedrock_kb_query", {
        "knowledge_base_id": knowledge_base_id,
        "question": question,
        "temperature": 0.1,
        "max_tokens": 2000
    })
    
    print(f"Question: {question}")
    print(f"Answer: {response}")
    print()


async def semantic_search_example(client: MockMCPClient, knowledge_base_id: str):
    """Demonstrate semantic search capabilities."""
    print("🧠 Semantic Search Example...")
    
    queries = [
        "cost optimization strategies",
        "serverless architecture benefits", 
        "auto scaling configuration"
    ]
    
    for query in queries:
        print(f"Searching for: '{query}'")
        
        response = await client.call_tool("bedrock_kb_search", {
            "knowledge_base_id": knowledge_base_id,
            "query": query,
            "num_results": 3,
            "search_type": "SEMANTIC"
        })
        
        results = json.loads(response)
        
        if results.get("success") and results["results"]:
            best_result = results["results"][0]
            print(f"  Best match (score: {best_result['score']}):")
            print(f"  {best_result['content'][:80]}...")
        else:
            print("  No results found")
        
        print()


async def hybrid_search_comparison(client: MockMCPClient, knowledge_base_id: str):
    """Compare semantic vs hybrid search results."""
    print("⚖️  Comparing Search Types...")
    
    query = "AWS Lambda cold start optimization"
    
    print(f"Query: '{query}'")
    print()
    
    # Semantic search
    semantic_response = await client.call_tool("bedrock_kb_search", {
        "knowledge_base_id": knowledge_base_id,
        "query": query,
        "num_results": 2,
        "search_type": "SEMANTIC"
    })
    
    semantic_results = json.loads(semantic_response)
    print("Semantic Search Results:")
    for result in semantic_results.get("results", []):
        print(f"  Score: {result['score']} - {result['content'][:60]}...")
    print()
    
    # Hybrid search
    hybrid_response = await client.call_tool("bedrock_kb_search", {
        "knowledge_base_id": knowledge_base_id,
        "query": query,
        "num_results": 2,
        "search_type": "HYBRID"
    })
    
    hybrid_results = json.loads(hybrid_response)
    print("Hybrid Search Results:")
    for result in hybrid_results.get("results", []):
        print(f"  Score: {result['score']} - {result['content'][:60]}...")
    print()


async def contextual_qa_example(client: MockMCPClient, knowledge_base_id: str):
    """Demonstrate contextual Q&A with different question types."""
    print("❓ Contextual Q&A Examples...")
    
    questions = [
        "What is AWS Lambda?",
        "How much does Lambda cost?",
        "What are Lambda best practices?",
        "Compare Lambda with EC2",
        "How to reduce Lambda cold starts?"
    ]
    
    for question in questions:
        print(f"Q: {question}")
        
        response = await client.call_tool("bedrock_kb_query", {
            "knowledge_base_id": knowledge_base_id,
            "question": question,
            "temperature": 0.2,
            "max_tokens": 150
        })
        
        # Truncate long responses for display
        answer = response[:200] + "..." if len(response) > 200 else response
        print(f"A: {answer}")
        print()


async def main():
    """Main function demonstrating basic usage."""
    print("🚀 Bedrock Knowledge Base MCP Server - Basic Usage Examples")
    print("=" * 60)
    print()
    
    # Initialize mock client
    client = MockMCPClient()
    
    try:
        # 1. List available Knowledge Bases
        knowledge_bases = await list_knowledge_bases(client)
        
        if not knowledge_bases:
            print("❌ No Knowledge Bases found. Please create one first.")
            return
        
        # Use the first Knowledge Base for examples
        kb_id = knowledge_bases[0]["id"]
        kb_name = knowledge_bases[0]["name"]
        
        print(f"Using Knowledge Base: {kb_name} ({kb_id})")
        print("=" * 60)
        print()
        
        # 2. Basic search
        await search_knowledge_base(client, kb_id)
        
        # 3. RAG query
        await query_with_rag(client, kb_id)
        
        # 4. Semantic search examples
        await semantic_search_example(client, kb_id)
        
        # 5. Search type comparison
        await hybrid_search_comparison(client, kb_id)
        
        # 6. Contextual Q&A
        await contextual_qa_example(client, kb_id)
        
        print("✅ Basic usage examples completed!")
        print()
        print("Tool calls made:", len(client.tools_called))
        for tool_name, args in client.tools_called:
            print(f"  - {tool_name}")
    
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    # Run the examples
    asyncio.run(main())