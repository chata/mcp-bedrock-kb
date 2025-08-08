from __future__ import annotations

"""Configuration manager for Bedrock Knowledge Base MCP server."""

import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manage configuration for the MCP server."""

    DEFAULT_CONFIG = {
        "aws": {"region": "us-east-1", "profile": None, "use_iam_role": True},
        "bedrock": {
            "default_model": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0",
            "default_kb_id": None,
        },
        "s3": {"default_bucket": None, "upload_prefix": "documents/"},
        "document_processing": {
            "supported_formats": ["txt", "md", "html", "pdf", "docx"],
            "max_file_size_mb": 50,
            "encoding": "utf-8",
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "file": None,
        },
        "mcp": {
            "server_name": "bedrock-knowledge-base",
            "version": "1.0.0",
            "description": "Bedrock Knowledge Base MCP Server with CRUD operations",
        },
    }

    def __init__(self, config_path: Path | None = None):
        """Initialize configuration manager.

        Args:
            config_path: Path to configuration file (optional)
        """
        # Deep copy to avoid modifying the class-level DEFAULT_CONFIG
        import copy

        self.config = copy.deepcopy(self.DEFAULT_CONFIG)
        self.config_path = config_path

        if config_path:
            self.load_from_file(config_path)

        self.load_from_environment()
        self._setup_logging()

    def load_from_file(self, config_path: Path):
        """Load configuration from a YAML file.

        Args:
            config_path: Path to configuration file
        """
        config_path = Path(config_path)

        if not config_path.exists():
            logger.warning(f"Configuration file not found: {config_path}")
            return

        try:
            with open(config_path) as f:
                file_config = yaml.safe_load(f)

            if file_config:
                self.config = self._deep_merge(self.config, file_config)
                logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")

    def load_from_environment(self):
        """Load configuration from environment variables."""
        env_mapping = {
            "AWS_REGION": ("aws", "region"),
            "AWS_DEFAULT_REGION": ("aws", "region"),  # Fallback if AWS_REGION not set
            "AWS_PROFILE": ("aws", "profile"),
            "AWS_USE_IAM_ROLE": ("aws", "use_iam_role"),
            "BEDROCK_DEFAULT_MODEL": ("bedrock", "default_model"),
            "BEDROCK_DEFAULT_KB_ID": ("bedrock", "default_kb_id"),
            "S3_DEFAULT_BUCKET": ("s3", "default_bucket"),
            "S3_UPLOAD_PREFIX": ("s3", "upload_prefix"),
            "DOC_MAX_FILE_SIZE_MB": ("document_processing", "max_file_size_mb"),
            "DOC_ENCODING": ("document_processing", "encoding"),
            "LOG_LEVEL": ("logging", "level"),
            "LOG_FILE": ("logging", "file"),
        }

        # Process AWS_DEFAULT_REGION first, then AWS_REGION (which takes precedence)
        for env_var in ["AWS_DEFAULT_REGION", "AWS_REGION"]:
            if env_var in env_mapping:
                value = os.environ.get(env_var)
                if value is not None:
                    config_path = env_mapping[env_var]
                    self._set_nested(self.config, config_path, self._parse_env_value(value))
                    logger.debug(f"Set {'.'.join(config_path)} from environment variable {env_var}")

        # Process other environment variables
        for env_var, config_path in env_mapping.items():
            if env_var not in ["AWS_DEFAULT_REGION", "AWS_REGION"]:
                value = os.environ.get(env_var)
                if value is not None:
                    self._set_nested(self.config, config_path, self._parse_env_value(value))
                    logger.debug(f"Set {'.'.join(config_path)} from environment variable {env_var}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value using dot notation.

        Args:
            key: Configuration key (e.g., "aws.region")
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key.split(".")
        value = self.config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def set(self, key: str, value: Any):
        """Set a configuration value using dot notation.

        Args:
            key: Configuration key (e.g., "aws.region")
            value: Value to set
        """
        keys = key.split(".")
        self._set_nested(self.config, keys, value)

    def get_all(self) -> dict[str, Any]:
        """Get the entire configuration.

        Returns:
            Complete configuration dictionary
        """
        return self.config.copy()

    def save_to_file(self, config_path: Path | None = None):
        """Save configuration to a YAML file.

        Args:
            config_path: Path to save configuration file
        """
        if not config_path:
            config_path = self.config_path

        if not config_path:
            raise ValueError("No configuration file path specified")

        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(config_path, "w") as f:
            yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Saved configuration to {config_path}")

    def validate(self) -> dict[str, Any]:
        """Validate the configuration.

        Returns:
            Validation results with warnings and errors
        """
        results = {"valid": True, "warnings": [], "errors": []}

        if not self.get("aws.region"):
            results["errors"].append("AWS region is not configured")
            results["valid"] = False

        if not self.get("s3.default_bucket"):
            results["warnings"].append(
                "No default S3 bucket configured. Will try to detect from Knowledge Base."
            )

        supported_formats = self.get("document_processing.supported_formats", [])
        if not supported_formats:
            results["errors"].append("No supported document formats configured")
            results["valid"] = False

        max_file_size = self.get("document_processing.max_file_size_mb", 0)
        if max_file_size <= 0:
            results["errors"].append("Invalid max file size configuration")
            results["valid"] = False

        log_level = self.get("logging.level", "INFO")
        if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            results["warnings"].append(f"Invalid log level: {log_level}, using INFO")

        return results

    def _deep_merge(self, base: dict, update: dict) -> dict:
        """Deep merge two dictionaries.

        Args:
            base: Base dictionary
            update: Dictionary to merge into base

        Returns:
            Merged dictionary
        """
        result = base.copy()

        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def _set_nested(self, config: dict, keys: tuple, value: Any):
        """Set a nested configuration value.

        Args:
            config: Configuration dictionary
            keys: Tuple of keys for nested access
            value: Value to set
        """
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]

        config[keys[-1]] = value

    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value to appropriate type.

        Args:
            value: String value from environment

        Returns:
            Parsed value
        """
        if value.lower() in ["true", "yes", "1"]:
            return True
        elif value.lower() in ["false", "no", "0"]:
            return False

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        if value.startswith("[") and value.endswith("]"):
            try:
                items = value[1:-1].split(",")
                return [item.strip() for item in items if item.strip()]
            except Exception:
                pass

        return value

    def _setup_logging(self):
        """Set up logging based on configuration."""
        log_level = getattr(logging, self.get("logging.level", "INFO"))
        log_format = self.get("logging.format")
        log_file = self.get("logging.file")

        logging.basicConfig(level=log_level, format=log_format, filename=log_file)

        if log_file:
            logger.info(f"Logging to file: {log_file}")
