"""Tests for ConfigManager."""

import os
import tempfile

import yaml

from src.bedrock_kb_mcp.config_manager import ConfigManager


class TestConfigManager:
    """Test cases for ConfigManager."""

    def test_default_config(self):
        """Test default configuration is loaded."""
        config = ConfigManager()

        assert config.get("aws.region") == "us-east-1"
        assert config.get("aws.use_iam_role") is True
        assert config.get("s3.upload_prefix") == "documents/"
        assert config.get("document_processing.max_file_size_mb") == 50

    def test_get_nested_config(self):
        """Test getting nested configuration values."""
        config = ConfigManager()

        assert config.get("aws.region") == "us-east-1"
        assert config.get("bedrock.default_model").startswith("arn:aws:bedrock")
        assert config.get("nonexistent.key", "default") == "default"

    def test_set_config(self):
        """Test setting configuration values."""
        config = ConfigManager()

        config.set("aws.region", "us-west-2")
        assert config.get("aws.region") == "us-west-2"

        config.set("new.nested.key", "value")
        assert config.get("new.nested.key") == "value"

    def test_load_from_file(self):
        """Test loading configuration from YAML file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(
                {
                    "aws": {"region": "eu-west-1", "profile": "test-profile"},
                    "s3": {"default_bucket": "test-bucket"},
                },
                f,
            )
            temp_path = f.name

        try:
            config = ConfigManager(config_path=temp_path)

            assert config.get("aws.region") == "eu-west-1"
            assert config.get("aws.profile") == "test-profile"
            assert config.get("s3.default_bucket") == "test-bucket"
            assert config.get("aws.use_iam_role") is True
        finally:
            os.unlink(temp_path)

    def test_load_from_environment(self):
        """Test loading configuration from environment variables."""
        # Save current environment
        saved_env = {}
        env_vars = [
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
            "AWS_PROFILE",
            "S3_DEFAULT_BUCKET",
            "DOC_MAX_FILE_SIZE_MB",
            "AWS_USE_IAM_ROLE",
        ]
        for var in env_vars:
            if var in os.environ:
                saved_env[var] = os.environ[var]
                del os.environ[var]

        os.environ["AWS_REGION"] = "ap-northeast-1"
        os.environ["S3_DEFAULT_BUCKET"] = "env-bucket"
        os.environ["DOC_MAX_FILE_SIZE_MB"] = "100"
        os.environ["AWS_USE_IAM_ROLE"] = "false"

        try:
            config = ConfigManager()

            assert config.get("aws.region") == "ap-northeast-1"
            assert config.get("s3.default_bucket") == "env-bucket"
            assert config.get("document_processing.max_file_size_mb") == 100
            assert config.get("aws.use_iam_role") is False
        finally:
            # Clean up
            for var in env_vars:
                if var in os.environ:
                    del os.environ[var]
            # Restore saved environment
            for var, value in saved_env.items():
                os.environ[var] = value

    def test_save_to_file(self):
        """Test saving configuration to file."""
        config = ConfigManager()
        config.set("aws.region", "us-east-2")
        config.set("test.value", "test123")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            temp_path = f.name

        try:
            config.save_to_file(temp_path)

            with open(temp_path) as f:
                saved_config = yaml.safe_load(f)

            assert saved_config["aws"]["region"] == "us-east-2"
            assert saved_config["test"]["value"] == "test123"
        finally:
            os.unlink(temp_path)

    def test_validate_config(self):
        """Test configuration validation."""
        config = ConfigManager()

        results = config.validate()
        assert results["valid"] is True
        assert len(results["errors"]) == 0

        config.set("aws.region", None)
        results = config.validate()
        assert results["valid"] is False
        assert any("AWS region" in error for error in results["errors"])

        config.set("aws.region", "us-east-1")
        config.set("document_processing.max_file_size_mb", -1)
        results = config.validate()
        assert results["valid"] is False
        assert any("file size" in error for error in results["errors"])

    def test_deep_merge(self):
        """Test deep merge functionality."""
        config = ConfigManager()

        base = {"a": {"b": 1, "c": 2}, "d": 3}
        update = {"a": {"b": 10, "e": 4}, "f": 5}

        result = config._deep_merge(base, update)

        assert result["a"]["b"] == 10
        assert result["a"]["c"] == 2
        assert result["a"]["e"] == 4
        assert result["d"] == 3
        assert result["f"] == 5

    def test_parse_env_value(self):
        """Test environment value parsing."""
        config = ConfigManager()

        assert config._parse_env_value("true") is True
        assert config._parse_env_value("false") is False
        assert config._parse_env_value("123") == 123
        assert config._parse_env_value("45.67") == 45.67
        assert config._parse_env_value("text") == "text"
        assert config._parse_env_value("[a,b,c]") == ["a", "b", "c"]

    def test_aws_default_region_fallback(self):
        """Test AWS_DEFAULT_REGION as fallback for AWS_REGION."""
        # Test 1: Only AWS_DEFAULT_REGION set
        os.environ["AWS_DEFAULT_REGION"] = "eu-central-1"
        config = ConfigManager()
        assert config.get("aws.region") == "eu-central-1"

        # Test 2: AWS_REGION overrides AWS_DEFAULT_REGION
        os.environ["AWS_REGION"] = "us-west-2"
        config2 = ConfigManager()
        assert config2.get("aws.region") == "us-west-2"

    def test_aws_profile_from_environment(self):
        """Test AWS_PROFILE loading from environment."""
        # Set AWS_PROFILE
        os.environ["AWS_PROFILE"] = "test-profile"
        config = ConfigManager()
        assert config.get("aws.profile") == "test-profile"
