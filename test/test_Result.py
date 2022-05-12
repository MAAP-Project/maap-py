import pathlib
import re

import os
import os.path
import pytest
import requests
import responses

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
        dps=None,
    )

    with pytest.raises(requests.exceptions.HTTPError):
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
        dps=None,
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
        dps=None,
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
        dps=None,
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
        dps=None,
    )

    destfile = granule.getData(str(tmp_path), overwrite=True)

    # New file should have been written, since it didn't exist before
    assert os.path.exists(destfile)
