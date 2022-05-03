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
        dps=None,
    )

    with pytest.raises(requests.exceptions.HTTPError):
        granule.getData(str(tmp_path))
