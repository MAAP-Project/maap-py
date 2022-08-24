import pathlib
import re

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
    )

    with pytest.raises(requests.exceptions.HTTPError, match="403"):
        granule.getData(str(tmp_path))


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
