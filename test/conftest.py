import os
from typing import Iterable

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_s3.client import S3Client

from maap.AWS import AWS


@pytest.fixture(scope="session")
def aws() -> AWS:
    return AWS(
        "https://test_requester_pays_endpoint.com",
        "https://test_s3_signed_url_endpoint.com",
        "https://test_earthdata_s3_credentials_endpoint.com",
        "https://test_workspace_bucket_endpoint.com",
        {}
    )


@pytest.fixture(scope="function")
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials) -> Iterable[S3Client]:
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")
