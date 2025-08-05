"""Tests for AuthManager."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from src.bedrock_kb_mcp.auth_manager import AuthManager
from src.bedrock_kb_mcp.config_manager import ConfigManager


class TestAuthManager:
    """Test cases for AuthManager."""

    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return ConfigManager()

    @pytest.fixture
    def auth_manager(self, config):
        """Create an AuthManager instance."""
        return AuthManager(config)

    @pytest.mark.asyncio
    async def test_init(self, config):
        """Test AuthManager initialization."""
        auth_manager = AuthManager(config)

        assert auth_manager.config == config
        assert auth_manager.region == config.get("aws.region", "us-east-1")
        assert auth_manager.profile == config.get("aws.profile")
        assert auth_manager.use_iam_role == config.get("aws.use_iam_role", True)

    @pytest.mark.asyncio
    async def test_profile_from_config(self):
        """Test using profile from configuration."""
        config = ConfigManager()
        config.set("aws.profile", "test-profile")
        auth_manager = AuthManager(config)

        assert auth_manager.profile == "test-profile"

    @pytest.mark.asyncio
    async def test_aws_profile_environment_variable(self):
        """Test AWS_PROFILE environment variable handling."""
        # Set test environment
        os.environ["AWS_PROFILE"] = "env-profile"
        os.environ["AWS_REGION"] = "us-west-2"

        config = ConfigManager()
        auth_manager = AuthManager(config)

        # The config should have loaded the profile from environment
        assert config.get("aws.profile") == "env-profile"
        assert auth_manager.profile == "env-profile"

    @pytest.mark.asyncio
    async def test_create_session_with_profile(self):
        """Test session creation with profile."""
        config = ConfigManager()
        config.set("aws.profile", "test-profile")
        auth_manager = AuthManager(config)

        with patch("boto3.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {
                "Arn": "arn:aws:iam::123456789012:user/test",
                "Account": "123456789012",
            }
            mock_session.client.return_value = mock_sts
            mock_session_class.return_value = mock_session

            await auth_manager._create_session()

            # Verify Session was created with profile
            mock_session_class.assert_called_with(
                region_name=auth_manager.region, profile_name="test-profile"
            )

    @pytest.mark.asyncio
    async def test_create_session_with_access_keys(self):
        """Test session creation with access keys in environment."""
        saved_keys = {
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN"),
            "AWS_PROFILE": os.environ.get("AWS_PROFILE"),
        }

        try:
            # Clear AWS environment variables
            for key in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_PROFILE",
            ]:
                if key in os.environ:
                    del os.environ[key]

            # Set test credentials
            os.environ["AWS_ACCESS_KEY_ID"] = "AKIAIOSFODNN7EXAMPLE"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            os.environ["AWS_SESSION_TOKEN"] = "test-token"

            config = ConfigManager()
            auth_manager = AuthManager(config)

            with patch("boto3.Session") as mock_session_class:
                mock_session = MagicMock()
                mock_sts = MagicMock()
                mock_sts.get_caller_identity.return_value = {
                    "Arn": "arn:aws:iam::123456789012:user/test"
                }
                mock_session.client.return_value = mock_sts
                mock_session_class.return_value = mock_session

                await auth_manager._create_session()

                # Session should be created without profile when access keys are present
                mock_session_class.assert_called_with(region_name=auth_manager.region)

        finally:
            # Restore environment
            for key, value in saved_keys.items():
                if value is None and key in os.environ:
                    del os.environ[key]
                elif value is not None:
                    os.environ[key] = value

    @pytest.mark.asyncio
    async def test_session_caching(self, auth_manager):
        """Test that sessions are cached and reused."""
        with patch("boto3.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {"Arn": "test-arn"}
            mock_session.client.return_value = mock_sts
            mock_session_class.return_value = mock_session

            # First call
            session1 = await auth_manager.get_session()
            assert mock_session_class.call_count == 1

            # Second call should use cached session
            session2 = await auth_manager.get_session()
            assert mock_session_class.call_count == 1  # No new session created
            assert session1 == session2

    @pytest.mark.asyncio
    async def test_session_refresh_on_error(self, auth_manager):
        """Test session refresh when credentials expire."""
        with patch("boto3.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_sts = MagicMock()

            # First call succeeds
            mock_sts.get_caller_identity.return_value = {"Arn": "test-arn"}
            mock_session.client.return_value = mock_sts
            mock_session_class.return_value = mock_session

            session1 = await auth_manager.get_session()
            assert auth_manager._session is not None

            # Simulate expired credentials
            mock_sts.get_caller_identity.side_effect = ClientError(
                {"Error": {"Code": "ExpiredToken"}}, "GetCallerIdentity"
            )

            # Reset mock to create new session
            mock_session_class.reset_mock()
            new_mock_session = MagicMock()
            new_mock_sts = MagicMock()
            new_mock_sts.get_caller_identity.return_value = {"Arn": "new-arn"}
            new_mock_session.client.return_value = new_mock_sts
            mock_session_class.return_value = new_mock_session

            # This should create a new session
            session2 = await auth_manager.get_session()
            assert mock_session_class.call_count == 1  # New session created
            assert session2 != session1

    @pytest.mark.asyncio
    async def test_no_credentials_error(self):
        """Test handling when no credentials are available."""
        # Create config without IAM role support
        config = ConfigManager()
        config.set("aws.use_iam_role", False)
        auth_manager = AuthManager(config)

        with patch("boto3.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.side_effect = NoCredentialsError()
            mock_session.client.return_value = mock_sts
            mock_session_class.return_value = mock_session

            with pytest.raises(NoCredentialsError):
                await auth_manager._create_session()

    @pytest.mark.asyncio
    async def test_get_account_id(self, auth_manager):
        """Test getting AWS account ID."""
        with patch.object(auth_manager, "get_session", new_callable=AsyncMock) as mock_get_session:
            mock_session = MagicMock()
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {
                "Account": "123456789012",
                "Arn": "arn:aws:iam::123456789012:user/test",
            }
            mock_session.client.return_value = mock_sts
            mock_get_session.return_value = mock_session

            account_id = await auth_manager.get_account_id()
            assert account_id == "123456789012"

    @pytest.mark.asyncio
    async def test_check_permissions(self, auth_manager):
        """Test checking IAM permissions."""
        with patch.object(auth_manager, "get_session", new_callable=AsyncMock) as mock_get_session:
            mock_session = MagicMock()

            # Mock bedrock-agent client
            mock_bedrock = MagicMock()
            mock_bedrock.list_knowledge_bases.return_value = {"knowledgeBases": []}

            # Mock S3 client
            mock_s3 = MagicMock()
            mock_s3.list_buckets.return_value = {"Buckets": []}

            def mock_client(service_name, **kwargs):
                if service_name == "bedrock-agent":
                    return mock_bedrock
                elif service_name == "s3":
                    return mock_s3
                else:
                    return MagicMock()

            mock_session.client.side_effect = mock_client
            mock_get_session.return_value = mock_session

            results = await auth_manager.check_permissions(
                [
                    "bedrock:ListKnowledgeBases",
                    "s3:ListBuckets",
                ]
            )

            assert results["bedrock:ListKnowledgeBases"] is True
            assert results["s3:ListBuckets"] is True
