import pathlib
import re

import os
import os.path
import pytest
import requests
import responses
from mypy_boto3_s3.client import S3Client

from maap.Result import Granule

GRANULE_BASE_URL = "https://data.mydaac.earthdata.nasa.gov"


@responses.activate
def test_getData_403_raises(tmp_path: pathlib.Path):
    responses.get(
        url=re.compile(f"{GRANULE_BASE_URL}/.*"),
        status=403,
        body="""
            <?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>AccessDenied</Code>
                <Message>Access Denied</Message>
                <RequestId>REQUEST_ID</RequestId>
                <HostId>HOST_ID</HostId>
            </Error>
            """,
    )

    url = f"{GRANULE_BASE_URL}/path/to/mydata"
    granule = Granule(
        metaResult={"Granule": {"OnlineAccessURLs": {"OnlineAccessURL": {"URL": url}}}},
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    with pytest.raises(requests.exceptions.HTTPError, match="403"):
        granule.getData(str(tmp_path))


@responses.activate
def test_getData_no_overwrite_existing(tmp_path: pathlib.Path):
    filename = "greeting.txt"
    old_body = "I was here first!"

    # Create "existing" file
    with open(os.path.join(tmp_path, filename), mode="w") as f:
        f.write(old_body)

    new_body = "Oops! I'll leave quietly."
    responses.get(url=re.compile(f"{GRANULE_BASE_URL}/.*"), status=200, body=new_body)

    url = f"{GRANULE_BASE_URL}/path/to/{filename}"
    granule = Granule(
        metaResult={"Granule": {"OnlineAccessURLs": {"OnlineAccessURL": {"URL": url}}}},
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    destpath = granule.getData(str(tmp_path))

    # The "existing" file should NOT be overwritten
    with open(destpath, mode="r") as f:
        assert f.read() == old_body


@responses.activate
def test_getData_no_overwrite_non_existing(tmp_path: pathlib.Path):
    body = "hello world!"
    responses.get(
        url=re.compile(f"{GRANULE_BASE_URL}/.*"),
        status=200,
        body=body,
    )

    url = f"{GRANULE_BASE_URL}/path/to/greeting.txt"
    granule = Granule(
        metaResult={"Granule": {"OnlineAccessURLs": {"OnlineAccessURL": {"URL": url}}}},
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    destfile = granule.getData(str(tmp_path))

    # New file should have been written, since it didn't exist before
    assert os.path.exists(destfile)


@responses.activate
def test_getData_overwrite_existing(tmp_path: pathlib.Path):
    filename = "greeting.txt"
    old_body = "I was here first!"

    # Create "existing" file
    with open(os.path.join(tmp_path, filename), mode="w") as f:
        f.write(old_body)

    new_body = "There's a new kid in town!"
    responses.get(url=re.compile(f"{GRANULE_BASE_URL}/.*"), status=200, body=new_body)

    url = f"{GRANULE_BASE_URL}/path/to/{filename}"
    granule = Granule(
        metaResult={"Granule": {"OnlineAccessURLs": {"OnlineAccessURL": {"URL": url}}}},
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    destpath = granule.getData(str(tmp_path), overwrite=True)

    # The "existing" file SHOULD have been overwritten
    with open(destpath, mode="r") as f:
        assert f.read() == new_body


@responses.activate
def test_getData_overwrite_non_existing(tmp_path: pathlib.Path):
    body = "hello world!"
    responses.get(
        url=re.compile(f"{GRANULE_BASE_URL}/.*"),
        status=200,
        body=body,
    )

    url = f"{GRANULE_BASE_URL}/path/to/greeting.txt"
    granule = Granule(
        metaResult={"Granule": {"OnlineAccessURLs": {"OnlineAccessURL": {"URL": url}}}},
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    destfile = granule.getData(str(tmp_path), overwrite=True)

    # New file should have been written, since it didn't exist before
    assert os.path.exists(destfile)


def test_Granule_single_location():
    """
    _location should be s3 url
    _fallback should be None since only the s3 location is provided
    """
    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": {
                        "URL": "s3://ornl-cumulus-prod-protected/gedi/*/data/*.h5"
                    }
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )
    assert granule._location == "s3://ornl-cumulus-prod-protected/gedi/*/data/*.h5"
    assert granule._fallback == None


def test_Granule_fallback_location():
    """
    _location should be s3 url
    _fallback should be https://.../*.h5 since a https url is provided
    """
    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": [
                        {"URL": "s3://ornl-cumulus-prod-protected/gedi/*/data/*.h5"},
                        {
                            "URL": "https://data.ornldaac.earthdata.nasa.gov/protected/gedi/*/data/*.h5"
                        },
                        {
                            "URL": "https://data.ornldaac.earthdata.nasa.gov/public/gedi/*/data/*.h5.sha256"
                        },
                    ]
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )
    assert granule._location == "s3://ornl-cumulus-prod-protected/gedi/*/data/*.h5"
    assert (
        granule._fallback
        == "https://data.ornldaac.earthdata.nasa.gov/protected/gedi/*/data/*.h5"
    )


def test_Granule_https_locations():
    """
    _location should be https url since no s3 location is provided
    _fallback should be the same as _location
    """
    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": [
                        {
                            "URL": "https://data.ornldaac.earthdata.nasa.gov/protected/gedi/*/data/*.h5"
                        },
                        {
                            "URL": "https://data.ornldaac.earthdata.nasa.gov/public/gedi/*/data/*.h5.sha256"
                        },
                    ]
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )
    assert (
        granule._location
        == "https://data.ornldaac.earthdata.nasa.gov/protected/gedi/*/data/*.h5"
    )
    assert (
        granule._fallback
        == "https://data.ornldaac.earthdata.nasa.gov/protected/gedi/*/data/*.h5"
    )


def test_getData_s3_url_only(s3: S3Client, tmp_path: pathlib.Path):
    s3.create_bucket(Bucket="mybucket")
    s3.put_object(Bucket="mybucket", Key="file.txt", Body="s3 contents")

    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": {"URL": "s3://mybucket/file.txt"}
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    with open(granule.getData(str(tmp_path))) as f:
        assert f.read() == "s3 contents"


@responses.activate
def test_getData_s3_url_with_fallback_in_order(tmp_path: pathlib.Path):
    responses.get(
        url="https://host/file.txt",
        status=200,
        body="http contents",
    )

    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": [
                        {
                            # getData should try this first, but should fail because
                            # we have not mocked the indicated S3 object.
                            "URL": "s3://mybucket/file.txt"
                        },
                        {
                            # getData should fallback to using this URL after failing
                            # with the S3 URL above.
                            "URL": "https://host/file.txt"
                        },
                    ]
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    with open(granule.getData(str(tmp_path))) as f:
        assert f.read() == "http contents"


@responses.activate
def test_getData_s3_url_with_fallback_wrong_order(s3: S3Client, tmp_path: pathlib.Path):
    s3.create_bucket(Bucket="mybucket")
    s3.put_object(Bucket="mybucket", Key="file.txt", Body="s3 contents")

    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": [
                        {
                            # getData should NOT attempt to use this because it should
                            # use the S3 URL below, even though this URL is the first
                            # in the list.
                            "URL": "https://host/file.txt"
                        },
                        {
                            # getData should use this URL first, and it should succeed
                            # because we have mocked the indicated S3 object.
                            "URL": "s3://mybucket/file.txt"
                        },
                    ]
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    with open(granule.getData(str(tmp_path))) as f:
        assert f.read() == "s3 contents"


@responses.activate
def test_getData_https_url_only(tmp_path: pathlib.Path):
    responses.get(
        url="https://host/file.txt",
        status=200,
        body="http contents",
    )

    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": {
                        # getData should use this URL since there is no S3 URL to try.
                        "URL": "https://host/file.txt"
                    }
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    with open(granule.getData(str(tmp_path))) as f:
        assert f.read() == "http contents"


@responses.activate
def test_getData_s3_with_multiple_fallbacks(tmp_path: pathlib.Path):
    responses.get(
        url="https://host/file.txt",
        status=200,
        body="http contents",
    )

    granule = Granule(
        metaResult={
            "Granule": {
                "OnlineAccessURLs": {
                    "OnlineAccessURL": [
                        {
                            # getData should NOT attempt to use this because the
                            # filename does not match the filename in the S3 URL.
                            "URL": "https://host/file.txt.sha256"
                        },
                        {
                            # getData should use this URL first, but should fail because
                            # because we have not mocked the indicated S3 object.
                            "URL": "s3://mybucket/file.txt"
                        },
                        {
                            # getData should use this after failing with the S3 URL
                            # above, because the filename in this URL matches the
                            # filename in the S3 URL.
                            "URL": "https://host/file.txt"
                        },
                    ]
                }
            }
        },
        awsAccessKey="",
        awsAccessSecret="",
        apiHeader={},
        cmrFileUrl="",
    )

    with open(granule.getData(str(tmp_path))) as f:
        assert f.read() == "http contents"
