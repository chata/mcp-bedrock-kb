from __future__ import annotations

"""Bedrock API client for Knowledge Base operations."""

import logging
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class BedrockClient:
    """Client for Amazon Bedrock Knowledge Base operations."""

    def __init__(self, session: boto3.Session, config: Any):
        """Initialize Bedrock client.

        Args:
            session: AWS boto3 session
            config: Configuration manager instance
        """
        self.config = config
        self.region = config.get("aws.region", "us-east-1")

        self.bedrock_agent = session.client("bedrock-agent", region_name=self.region)
        self.bedrock_agent_runtime = session.client(
            "bedrock-agent-runtime", region_name=self.region
        )
        self.bedrock_runtime = session.client("bedrock-runtime", region_name=self.region)

        self.default_model = config.get(
            "bedrock.default_model",
            "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0",
        )

    async def search(
        self, knowledge_base_id: str, query: str, num_results: int = 5, search_type: str = "HYBRID"
    ) -> dict[str, Any]:
        """Search for information in a Knowledge Base.

        Args:
            knowledge_base_id: The Knowledge Base ID
            query: Search query
            num_results: Number of results to return
            search_type: Type of search (SEMANTIC or HYBRID)

        Returns:
            Search results
        """
        try:
            response = self.bedrock_agent_runtime.retrieve(
                knowledgeBaseId=knowledge_base_id,
                retrievalQuery={"text": query},
                retrievalConfiguration={
                    "vectorSearchConfiguration": {
                        "numberOfResults": num_results,
                        "overrideSearchType": search_type,
                    }
                },
            )

            results = []
            for item in response.get("retrievalResults", []):
                result = {
                    "content": item.get("content", {}).get("text", ""),
                    "location": item.get("location", {}),
                    "score": item.get("score", 0.0),
                    "metadata": item.get("metadata", {}),
                }
                results.append(result)

            return {"success": True, "results": results, "count": len(results)}

        except ClientError as e:
            logger.error(f"Error searching Knowledge Base: {e}")
            return {
                "success": False,
                "error": str(e),
                "error_code": e.response.get("Error", {}).get("Code", "Unknown"),
            }

    async def query(
        self,
        knowledge_base_id: str,
        question: str,
        model_arn: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 2000,
    ) -> str:
        """Query a Knowledge Base with RAG to generate an answer.

        Args:
            knowledge_base_id: The Knowledge Base ID
            question: Question to answer
            model_arn: Foundation Model ARN to use
            temperature: Generation temperature
            max_tokens: Maximum tokens to generate

        Returns:
            Generated answer
        """
        try:
            request_params = {
                "retrieveAndGenerateConfiguration": {
                    "type": "KNOWLEDGE_BASE",
                    "knowledgeBaseConfiguration": {
                        "knowledgeBaseId": knowledge_base_id,
                        "modelArn": model_arn or self.default_model,
                        "generationConfiguration": {
                            "inferenceConfig": {
                                "textInferenceConfig": {
                                    "temperature": temperature,
                                    "maxTokens": max_tokens,
                                }
                            }
                        },
                    },
                },
                "input": {"text": question},
            }

            response = self.bedrock_agent_runtime.retrieve_and_generate(**request_params)

            return response.get("output", {}).get("text", "No response generated")

        except ClientError as e:
            logger.error(f"Error querying Knowledge Base: {e}")
            return f"Error: {e}"

    async def list_knowledge_bases(self) -> list[dict[str, Any]]:
        """List all available Knowledge Bases.

        Returns:
            List of Knowledge Bases
        """
        try:
            knowledge_bases = []
            paginator = self.bedrock_agent.get_paginator("list_knowledge_bases")

            for page in paginator.paginate():
                for kb in page.get("knowledgeBaseSummaries", []):
                    knowledge_bases.append(
                        {
                            "id": kb.get("knowledgeBaseId"),
                            "name": kb.get("name"),
                            "description": kb.get("description"),
                            "status": kb.get("status"),
                            "createdAt": str(kb.get("createdAt")),
                            "updatedAt": str(kb.get("updatedAt")),
                        }
                    )

            return knowledge_bases

        except ClientError as e:
            logger.error(f"Error listing Knowledge Bases: {e}")
            return []

    async def get_knowledge_base(self, knowledge_base_id: str) -> dict[str, Any]:
        """Get Knowledge Base details.

        Args:
            knowledge_base_id: The Knowledge Base ID

        Returns:
            Knowledge Base details
        """
        try:
            response = self.bedrock_agent.get_knowledge_base(knowledgeBaseId=knowledge_base_id)

            kb = response.get("knowledgeBase", {})
            return {
                "id": kb.get("knowledgeBaseId"),
                "name": kb.get("name"),
                "description": kb.get("description"),
                "status": kb.get("status"),
                "roleArn": kb.get("roleArn"),
                "storageConfiguration": kb.get("storageConfiguration"),
                "createdAt": str(kb.get("createdAt")),
                "updatedAt": str(kb.get("updatedAt")),
            }

        except ClientError as e:
            logger.error(f"Error getting Knowledge Base: {e}")
            return {}

    async def list_data_sources(self, knowledge_base_id: str) -> list[dict[str, Any]]:
        """List data sources for a Knowledge Base.

        Args:
            knowledge_base_id: The Knowledge Base ID

        Returns:
            List of data sources
        """
        try:
            data_sources = []
            paginator = self.bedrock_agent.get_paginator("list_data_sources")

            for page in paginator.paginate(knowledgeBaseId=knowledge_base_id):
                for ds in page.get("dataSourceSummaries", []):
                    data_sources.append(
                        {
                            "id": ds.get("dataSourceId"),
                            "name": ds.get("name"),
                            "description": ds.get("description"),
                            "status": ds.get("status"),
                            "createdAt": str(ds.get("createdAt")),
                            "updatedAt": str(ds.get("updatedAt")),
                        }
                    )

            return data_sources

        except ClientError as e:
            logger.error(f"Error listing data sources: {e}")
            return []

    async def get_data_source(self, knowledge_base_id: str, data_source_id: str) -> dict[str, Any]:
        """Get data source details.

        Args:
            knowledge_base_id: The Knowledge Base ID
            data_source_id: The Data Source ID

        Returns:
            Data source details
        """
        try:
            response = self.bedrock_agent.get_data_source(
                knowledgeBaseId=knowledge_base_id, dataSourceId=data_source_id
            )

            ds = response.get("dataSource", {})
            config = ds.get("dataSourceConfiguration", {})
            s3_config = config.get("s3Configuration", {})

            return {
                "id": ds.get("dataSourceId"),
                "name": ds.get("name"),
                "description": ds.get("description"),
                "status": ds.get("status"),
                "bucketArn": s3_config.get("bucketArn"),
                "inclusionPrefixes": s3_config.get("inclusionPrefixes", []),
                "createdAt": str(ds.get("createdAt")),
                "updatedAt": str(ds.get("updatedAt")),
            }

        except ClientError as e:
            logger.error(f"Error getting data source: {e}")
            return {}

    async def start_ingestion_job(
        self, knowledge_base_id: str, data_source_id: str, description: str | None = None
    ) -> dict[str, Any]:
        """Start a data source ingestion job.

        Args:
            knowledge_base_id: The Knowledge Base ID
            data_source_id: The Data Source ID
            description: Job description

        Returns:
            Ingestion job details
        """
        try:
            params = {"knowledgeBaseId": knowledge_base_id, "dataSourceId": data_source_id}

            if description:
                params["description"] = description

            response = self.bedrock_agent.start_ingestion_job(**params)

            job = response.get("ingestionJob", {})
            return {
                "success": True,
                "jobId": job.get("ingestionJobId"),
                "knowledgeBaseId": job.get("knowledgeBaseId"),
                "dataSourceId": job.get("dataSourceId"),
                "status": job.get("status"),
                "startedAt": str(job.get("startedAt")),
            }

        except ClientError as e:
            logger.error(f"Error starting ingestion job: {e}")
            return {"success": False, "error": str(e)}

    async def get_ingestion_job_status(
        self, knowledge_base_id: str, data_source_id: str, job_id: str | None = None
    ) -> dict[str, Any]:
        """Get the status of an ingestion job.

        Args:
            knowledge_base_id: The Knowledge Base ID
            data_source_id: The Data Source ID
            job_id: Specific job ID (optional)

        Returns:
            Job status information
        """
        try:
            if job_id:
                response = self.bedrock_agent.get_ingestion_job(
                    knowledgeBaseId=knowledge_base_id,
                    dataSourceId=data_source_id,
                    ingestionJobId=job_id,
                )

                job = response.get("ingestionJob", {})
                stats = job.get("statistics", {})

                return {
                    "jobId": job.get("ingestionJobId"),
                    "status": job.get("status"),
                    "startedAt": str(job.get("startedAt")),
                    "updatedAt": str(job.get("updatedAt")),
                    "statistics": {
                        "numberOfDocumentsScanned": stats.get("numberOfDocumentsScanned"),
                        "numberOfDocumentsIndexed": stats.get("numberOfDocumentsIndexed"),
                        "numberOfDocumentsFailed": stats.get("numberOfDocumentsFailed"),
                        "numberOfDocumentsDeleted": stats.get("numberOfDocumentsDeleted"),
                    },
                    "failureReasons": job.get("failureReasons", []),
                }
            else:
                response = self.bedrock_agent.list_ingestion_jobs(
                    knowledgeBaseId=knowledge_base_id,
                    dataSourceId=data_source_id,
                    maxResults=1,
                    sortBy={"attribute": "STARTED_AT", "order": "DESCENDING"},
                )

                jobs = response.get("ingestionJobSummaries", [])
                if jobs:
                    latest_job = jobs[0]
                    return {
                        "jobId": latest_job.get("ingestionJobId"),
                        "status": latest_job.get("status"),
                        "startedAt": str(latest_job.get("startedAt")),
                        "updatedAt": str(latest_job.get("updatedAt")),
                        "statistics": latest_job.get("statistics", {}),
                    }
                else:
                    return {"message": "No ingestion jobs found"}

        except ClientError as e:
            logger.error(f"Error getting ingestion job status: {e}")
            return {"error": str(e)}
