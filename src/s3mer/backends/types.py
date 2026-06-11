"""Shared backend client types."""

from s3mer.backends.client import S3BackendClient
from s3mer.backends.memory_backend import MemoryS3BackendClient

BackendClient = S3BackendClient | MemoryS3BackendClient
