import json
import logging
import urllib
import requests


class AWS:
    """
    Functions used for Member API interfacing
    """

    def __init__(
        self,
        requester_pays_endpoint,
        s3_signed_url_endpoint,
        earthdata_s3_credentials_endpoint,
        workspace_bucket_endpoint,
        api_header,
    ):
        self._api_header = api_header
        self._requester_pays_endpoint = requester_pays_endpoint
        self._earthdata_s3_credentials_endpoint = earthdata_s3_credentials_endpoint
        self._workspace_bucket_endpoint = workspace_bucket_endpoint
        self._s3_signed_url_endpoint = s3_signed_url_endpoint
        self._logger = logging.getLogger(__name__)

    def requester_pays_credentials(self, expiration=60 * 60 * 12):
        headers = self._api_header
        headers["Accept"] = "application/json"

        response = requests.get(
            url=self._requester_pays_endpoint + "?exp=" + str(expiration),
            headers=self._api_header,
        )
        response.raise_for_status()

        return json.loads(response.text)

    def s3_signed_url(self, bucket, key, expiration=60 * 60 * 12):
        headers = self._api_header
        headers["Accept"] = "application/json"
        _url = self._s3_signed_url_endpoint.replace("{bucket}", bucket).replace(
            "{key}", key
        )

        response = requests.get(
            url=_url + "?exp=" + str(expiration), headers=self._api_header
        )
        response.raise_for_status()

        return json.loads(response.text)

    def earthdata_s3_credentials(self, endpoint_uri):
        headers = self._api_header
        headers["Accept"] = "application/json"
        _parsed_endpoint = urllib.parse.quote(urllib.parse.quote(endpoint_uri, safe=""))
        _url = self._earthdata_s3_credentials_endpoint.replace(
            "{endpoint_uri}", _parsed_endpoint
        )

        response = requests.get(url=_url, headers=self._api_header)
        response.raise_for_status()

        result = json.loads(response.text)
        result["DAAC"] = urllib.parse.urlparse(endpoint_uri).netloc

        return result

    def workspace_bucket_credentials(self):
        headers = self._api_header
        headers["Accept"] = "application/json"

        response = requests.get(
            url=self._workspace_bucket_endpoint,
            headers=self._api_header,
        )

        response.raise_for_status()

        return json.loads(response.text)
