"""Pytest configuration and shared fixtures."""

import os

import pytest


@pytest.fixture(autouse=True)
def clean_aws_environment():
    """Automatically clean AWS-related environment variables for each test."""
    # Save current environment
    saved_env = {}
    aws_vars = [
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_USE_IAM_ROLE",
        "BEDROCK_DEFAULT_MODEL",
        "BEDROCK_DEFAULT_KB_ID",
        "S3_DEFAULT_BUCKET",
        "S3_UPLOAD_PREFIX",
        "DOC_MAX_FILE_SIZE_MB",
        "DOC_ENCODING",
        "LOG_LEVEL",
        "LOG_FILE",
    ]

    for var in aws_vars:
        if var in os.environ:
            saved_env[var] = os.environ[var]
            del os.environ[var]

    yield

    # Restore environment
    for var in aws_vars:
        if var in os.environ:
            del os.environ[var]

    for var, value in saved_env.items():
        os.environ[var] = value
