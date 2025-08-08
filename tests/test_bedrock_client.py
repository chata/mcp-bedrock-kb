from __future__ import annotations

"""Tests for BedrockClient."""

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from src.bedrock_kb_mcp.bedrock_client import BedrockClient


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
    config.get = MagicMock(
        side_effect=lambda key, default=None: {
            "aws.region": "us-east-1",
            "bedrock.default_model": "arn:aws:bedrock:us-east-1::foundation-model/test-model",
        }.get(key, default)
    )
    return config


@pytest.fixture
def bedrock_client(mock_session, mock_config):
    """Create a BedrockClient instance with mocks."""
    return BedrockClient(mock_session, mock_config)


class TestBedrockClient:
    """Test cases for BedrockClient."""

    @pytest.mark.asyncio
    async def test_search_success(self, bedrock_client):
        """Test successful Knowledge Base search."""
        bedrock_client.bedrock_agent_runtime.retrieve = MagicMock(
            return_value={
                "retrievalResults": [
                    {
                        "content": {"text": "Result 1"},
                        "location": {"s3": {"uri": "s3://bucket/doc1.txt"}},
                        "score": 0.95,
                        "metadata": {"key": "value"},
                    },
                    {
                        "content": {"text": "Result 2"},
                        "location": {"s3": {"uri": "s3://bucket/doc2.txt"}},
                        "score": 0.85,
                        "metadata": {},
                    },
                ]
            }
        )

        result = await bedrock_client.search(
            knowledge_base_id="KB123", query="test query", num_results=5, search_type="HYBRID"
        )

        assert result["success"] is True
        assert result["count"] == 2
        assert len(result["results"]) == 2
        assert result["results"][0]["content"] == "Result 1"
        assert result["results"][0]["score"] == 0.95

    @pytest.mark.asyncio
    async def test_search_error(self, bedrock_client):
        """Test Knowledge Base search with error."""
        error = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "KB not found"}}, "retrieve"
        )
        bedrock_client.bedrock_agent_runtime.retrieve = MagicMock(side_effect=error)

        result = await bedrock_client.search(knowledge_base_id="KB123", query="test query")

        assert result["success"] is False
        assert "error" in result
        assert result["error_code"] == "ResourceNotFoundException"

    @pytest.mark.asyncio
    async def test_query_success(self, bedrock_client):
        """Test successful Knowledge Base query with RAG."""
        bedrock_client.bedrock_agent_runtime.retrieve_and_generate = MagicMock(
            return_value={"output": {"text": "Generated answer based on knowledge base"}}
        )

        result = await bedrock_client.query(
            knowledge_base_id="KB123",
            question="What is the answer?",
            temperature=0.1,
            max_tokens=2000,
        )

        assert result == "Generated answer based on knowledge base"

    @pytest.mark.asyncio
    async def test_query_error(self, bedrock_client):
        """Test Knowledge Base query with error."""
        error = ClientError(
            {"Error": {"Code": "ValidationException", "Message": "Invalid request"}},
            "retrieve_and_generate",
        )
        bedrock_client.bedrock_agent_runtime.retrieve_and_generate = MagicMock(side_effect=error)

        result = await bedrock_client.query(
            knowledge_base_id="KB123", question="What is the answer?"
        )

        assert "Error:" in result

    @pytest.mark.asyncio
    async def test_list_knowledge_bases(self, bedrock_client):
        """Test listing Knowledge Bases."""
        paginator = MagicMock()
        paginator.paginate = MagicMock(
            return_value=[
                {
                    "knowledgeBaseSummaries": [
                        {
                            "knowledgeBaseId": "KB001",
                            "name": "Test KB 1",
                            "description": "First knowledge base",
                            "status": "ACTIVE",
                            "createdAt": "2024-01-01T00:00:00Z",
                            "updatedAt": "2024-01-02T00:00:00Z",
                        },
                        {
                            "knowledgeBaseId": "KB002",
                            "name": "Test KB 2",
                            "description": "Second knowledge base",
                            "status": "ACTIVE",
                            "createdAt": "2024-01-03T00:00:00Z",
                            "updatedAt": "2024-01-04T00:00:00Z",
                        },
                    ]
                }
            ]
        )

        bedrock_client.bedrock_agent.get_paginator = MagicMock(return_value=paginator)

        result = await bedrock_client.list_knowledge_bases()

        assert len(result) == 2
        assert result[0]["id"] == "KB001"
        assert result[0]["name"] == "Test KB 1"
        assert result[1]["id"] == "KB002"

    @pytest.mark.asyncio
    async def test_get_knowledge_base(self, bedrock_client):
        """Test getting Knowledge Base details."""
        bedrock_client.bedrock_agent.get_knowledge_base = MagicMock(
            return_value={
                "knowledgeBase": {
                    "knowledgeBaseId": "KB123",
                    "name": "Test Knowledge Base",
                    "description": "Test description",
                    "status": "ACTIVE",
                    "roleArn": "arn:aws:iam::123456789012:role/TestRole",
                    "storageConfiguration": {"type": "OPENSEARCH_SERVERLESS"},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z",
                }
            }
        )

        result = await bedrock_client.get_knowledge_base("KB123")

        assert result["id"] == "KB123"
        assert result["name"] == "Test Knowledge Base"
        assert result["status"] == "ACTIVE"
        assert "storageConfiguration" in result

    @pytest.mark.asyncio
    async def test_start_ingestion_job(self, bedrock_client):
        """Test starting an ingestion job."""
        bedrock_client.bedrock_agent.start_ingestion_job = MagicMock(
            return_value={
                "ingestionJob": {
                    "ingestionJobId": "JOB123",
                    "knowledgeBaseId": "KB123",
                    "dataSourceId": "DS123",
                    "status": "STARTING",
                    "startedAt": "2024-01-01T00:00:00Z",
                }
            }
        )

        result = await bedrock_client.start_ingestion_job(
            knowledge_base_id="KB123", data_source_id="DS123", description="Test ingestion"
        )

        assert result["success"] is True
        assert result["jobId"] == "JOB123"
        assert result["status"] == "STARTING"

    @pytest.mark.asyncio
    async def test_get_ingestion_job_status_with_id(self, bedrock_client):
        """Test getting ingestion job status with specific job ID."""
        bedrock_client.bedrock_agent.get_ingestion_job = MagicMock(
            return_value={
                "ingestionJob": {
                    "ingestionJobId": "JOB123",
                    "status": "COMPLETE",
                    "startedAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-01T00:10:00Z",
                    "statistics": {
                        "numberOfDocumentsScanned": 100,
                        "numberOfDocumentsIndexed": 95,
                        "numberOfDocumentsFailed": 5,
                        "numberOfDocumentsDeleted": 0,
                    },
                    "failureReasons": [],
                }
            }
        )

        result = await bedrock_client.get_ingestion_job_status(
            knowledge_base_id="KB123", data_source_id="DS123", job_id="JOB123"
        )

        assert result["jobId"] == "JOB123"
        assert result["status"] == "COMPLETE"
        assert result["statistics"]["numberOfDocumentsIndexed"] == 95

    @pytest.mark.asyncio
    async def test_get_ingestion_job_status_latest(self, bedrock_client):
        """Test getting latest ingestion job status."""
        bedrock_client.bedrock_agent.list_ingestion_jobs = MagicMock(
            return_value={
                "ingestionJobSummaries": [
                    {
                        "ingestionJobId": "JOB456",
                        "status": "IN_PROGRESS",
                        "startedAt": "2024-01-01T00:00:00Z",
                        "updatedAt": "2024-01-01T00:05:00Z",
                        "statistics": {"numberOfDocumentsScanned": 50},
                    }
                ]
            }
        )

        result = await bedrock_client.get_ingestion_job_status(
            knowledge_base_id="KB123", data_source_id="DS123"
        )

        assert result["jobId"] == "JOB456"
        assert result["status"] == "IN_PROGRESS"
