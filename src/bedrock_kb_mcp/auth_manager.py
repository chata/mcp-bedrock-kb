from __future__ import annotations

"""AWS authentication manager for Bedrock Knowledge Base MCP server."""

import logging
import os
from typing import Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class AuthManager:
    """Manage AWS authentication and sessions."""

    def __init__(self, config: Any):
        """Initialize authentication manager.

        Args:
            config: Configuration manager instance
        """
        self.config = config
        self.region = config.get("aws.region", "us-east-1")
        self.profile = config.get("aws.profile")
        self.use_iam_role = config.get("aws.use_iam_role", True)
        self._session: boto3.Session | None = None

    async def get_session(self) -> boto3.Session:
        """Get or create an AWS session.

        Returns:
            boto3.Session: Authenticated AWS session

        Raises:
            NoCredentialsError: If no valid credentials are found
        """
        if self._session is not None:
            try:
                sts = self._session.client("sts")
                sts.get_caller_identity()
                return self._session
            except (ClientError, NoCredentialsError):
                logger.info("Session expired or invalid, creating new session")
                self._session = None

        self._session = await self._create_session()
        return self._session

    async def _create_session(self) -> boto3.Session:
        """Create a new AWS session.

        Returns:
            boto3.Session: New authenticated AWS session

        Raises:
            NoCredentialsError: If no valid credentials are found
        """
        session_params = {"region_name": self.region}

        # Check for profile from config
        if self.profile:
            logger.info(f"Using AWS profile from config: {self.profile}")
            session_params["profile_name"] = self.profile
            try:
                session = boto3.Session(**session_params)
                self._validate_session(session)
                return session
            except Exception as e:
                logger.warning(f"Failed to use profile {self.profile}: {e}")
                if not self.use_iam_role:
                    raise

        # If no profile in config, boto3 will automatically check AWS_PROFILE env var
        # Try creating session without explicit profile (boto3 handles AWS_PROFILE)
        if os.environ.get("AWS_PROFILE"):
            logger.info(
                f"AWS_PROFILE environment variable detected: {os.environ.get('AWS_PROFILE')}"
            )
            try:
                session = boto3.Session(**session_params)
                self._validate_session(session)
                logger.info("Successfully authenticated using AWS_PROFILE environment variable")
                return session
            except Exception as e:
                logger.warning(f"Failed to use AWS_PROFILE: {e}")
                if not self.use_iam_role:
                    raise

        if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
            logger.info("Using AWS credentials from environment variables")
            # AWS_SESSION_TOKEN is automatically picked up by boto3 if present
            if os.environ.get("AWS_SESSION_TOKEN"):
                logger.info("Using temporary credentials with session token")
            session = boto3.Session(**session_params)
            self._validate_session(session)
            return session

        if self.use_iam_role:
            logger.info("Attempting to use IAM role credentials")
            session = boto3.Session(**session_params)
            try:
                self._validate_session(session)
                logger.info("Successfully using IAM role credentials")
                return session
            except NoCredentialsError:
                logger.error("No IAM role credentials available")
                raise

        error = NoCredentialsError()
        error.fmt = (
            "No valid AWS credentials found. Please configure AWS credentials via:\n"
            "1. AWS SSO profile in config\n"
            "2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)\n"
            "3. IAM role (if running on AWS)"
        )
        raise error

    def _validate_session(self, session: boto3.Session):
        """Validate that a session has valid credentials.

        Args:
            session: boto3 session to validate

        Raises:
            NoCredentialsError: If credentials are invalid
        """
        try:
            sts = session.client("sts")
            identity = sts.get_caller_identity()
            logger.info(f"Authenticated as: {identity.get('Arn')}")
        except NoCredentialsError:
            raise
        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidClientTokenId":
                raise NoCredentialsError("Invalid AWS credentials")
            raise

    async def get_account_id(self) -> str | None:
        """Get the AWS account ID.

        Returns:
            AWS account ID or None
        """
        try:
            session = await self.get_session()
            sts = session.client("sts")
            identity = sts.get_caller_identity()
            return identity.get("Account")
        except Exception as e:
            logger.error(f"Failed to get account ID: {e}")
            return None

    async def get_caller_identity(self) -> dict[str, str]:
        """Get the caller identity information.

        Returns:
            Caller identity information
        """
        try:
            session = await self.get_session()
            sts = session.client("sts")
            return sts.get_caller_identity()
        except Exception as e:
            logger.error(f"Failed to get caller identity: {e}")
            return {}

    async def check_permissions(self, required_actions: list[str]) -> dict[str, bool]:
        """Check if the current credentials have specific permissions.

        Args:
            required_actions: List of IAM actions to check

        Returns:
            Dictionary mapping actions to permission status
        """
        results = {}
        session = await self.get_session()

        for action in required_actions:
            service, operation = action.split(":", 1)

            try:
                if service == "bedrock":
                    client = session.client("bedrock-agent", region_name=self.region)
                    if operation == "ListKnowledgeBases":
                        client.list_knowledge_bases(maxResults=1)
                    results[action] = True
                elif service == "bedrock-runtime":
                    client = session.client("bedrock-agent-runtime", region_name=self.region)
                    results[action] = True
                elif service == "s3":
                    client = session.client("s3", region_name=self.region)
                    if operation == "ListBuckets":
                        client.list_buckets()
                    results[action] = True
                else:
                    results[action] = False
            except ClientError as e:
                if e.response["Error"]["Code"] in [
                    "AccessDeniedException",
                    "UnauthorizedOperation",
                ]:
                    results[action] = False
                else:
                    results[action] = True
            except Exception:
                results[action] = False

        return results

    async def refresh_credentials(self):
        """Refresh AWS credentials if using temporary credentials."""
        logger.info("Refreshing AWS credentials")
        self._session = None
        await self.get_session()
