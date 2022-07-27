import pytest
import requests
import responses

from maap.AWS import AWS

aws = AWS(
    "https://test_requester_pays_endpoint.com",
    "https://test_s3_signed_url_endpoint.com",
    "https://test_earthdata_s3_credentials_endpoint.com",
    {}
)

@responses.activate
def test_requester_pays_credentials():
    responses.get(
        url="https://test_requester_pays_endpoint.com",
        status=403
    )

    with pytest.raises(requests.exceptions.HTTPError):
        aws.requester_pays_credentials()

@responses.activate
def test_s3_signed_url():
    responses.get(
        url="https://test_s3_signed_url_endpoint.com",
        status=403
    )

    with pytest.raises(requests.exceptions.HTTPError):
        aws.s3_signed_url("","")

@responses.activate
def test_earthdata_s3_credentials():
    responses.get(
        url="https://test_earthdata_s3_credentials_endpoint.com",
        status=403
    )

    with pytest.raises(requests.exceptions.HTTPError):
        aws.earthdata_s3_credentials("")
