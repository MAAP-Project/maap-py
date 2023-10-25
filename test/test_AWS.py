import pytest
import requests
import responses

from maap.AWS import AWS


@responses.activate
def test_requester_pays_credentials(aws: AWS):
    responses.get(url=aws._requester_pays_endpoint, status=403)

    with pytest.raises(requests.exceptions.HTTPError, match="403"):
        aws.requester_pays_credentials()


@responses.activate
def test_s3_signed_url(aws: AWS):
    responses.get(url=aws._s3_signed_url_endpoint, status=403)

    with pytest.raises(requests.exceptions.HTTPError, match="403"):
        aws.s3_signed_url("", "")


@responses.activate
def test_earthdata_s3_credentials(aws: AWS):
    responses.get(url=aws._earthdata_s3_credentials_endpoint, status=403)

    with pytest.raises(requests.exceptions.HTTPError, match="403"):
        aws.earthdata_s3_credentials("")

        
@responses.activate
def test_workspace_bucket_credentials(aws: AWS):
    responses.get(url=aws._workspace_bucket_endpoint, status=403)

    with pytest.raises(requests.exceptions.HTTPError, match="403"):
        aws.workspace_bucket_credentials("")
