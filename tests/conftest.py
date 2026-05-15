
import os
import boto3
import pytest
from botocore.client import Config

@pytest.fixture(scope="session")
def s3_config():
    """Get S3 configuration from environment variables."""
    return {
        "endpoint_url": os.environ.get("S3M_PROXY_URL", "http://localhost:8000"),
        "secondary_url": os.environ.get("S3M_SECONDARY_URL", "http://localhost:9002"),
        "access_key": os.environ.get("S3M_ACCESS_KEY", "minioadmin"),
        "secret_key": os.environ.get("S3M_SECRET_KEY", "minioadmin"),
        "region": os.environ.get("S3M_REGION", "us-east-1"),
    }

@pytest.fixture(scope="session")
def s3_proxy(s3_config):
    """Boto3 client for the S3M proxy."""
    return boto3.client(
        "s3",
        endpoint_url=s3_config["endpoint_url"],
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
        config=Config(signature_version="s3v4"),
        region_name=s3_config["region"],
    )

@pytest.fixture(scope="session")
def s3_secondary(s3_config):
    """Boto3 client for the secondary backend (direct access)."""
    return boto3.client(
        "s3",
        endpoint_url=s3_config["secondary_url"],
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
        config=Config(signature_version="s3v4"),
        region_name=s3_config["region"],
    )

@pytest.fixture(scope="session")
def s3_resource(s3_config):
    """Boto3 resource for the S3M proxy."""
    return boto3.resource(
        "s3",
        endpoint_url=s3_config["endpoint_url"],
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
        config=Config(signature_version="s3v4"),
        region_name=s3_config["region"],
    )
