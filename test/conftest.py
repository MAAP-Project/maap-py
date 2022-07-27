import pytest

from maap.AWS import AWS


@pytest.fixture(scope="session")
def aws() -> AWS:
    return AWS(
        "https://test_requester_pays_endpoint.com",
        "https://test_s3_signed_url_endpoint.com",
        "https://test_earthdata_s3_credentials_endpoint.com",
        {},
    )
