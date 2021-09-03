import requests
import logging
import json


class AWS:
    """
    Functions used for Member API interfacing
    """
    def __init__(self, requester_pays_endpoint, s3_signed_url_endpoint, api_header):
        self._api_header = api_header
        self._requester_pays_endpoint = requester_pays_endpoint
        self._s3_signed_url_endpoint = s3_signed_url_endpoint
        self._logger = logging.getLogger(__name__)

    def requester_pays_credentials(self, expiration=60 * 60 * 12):
        headers = self._api_header
        headers['Accept'] = 'application/json'

        response = requests.get(
            url=self._requester_pays_endpoint + '?exp=' + str(expiration),
            headers=self._api_header
        )

        if response:
            return json.loads(response.text)
        else:
            return None

    def s3_signed_url(self, bucket, key, expiration=60 * 60 * 12):
        headers = self._api_header
        headers['Accept'] = 'application/json'
        _url = self._s3_signed_url_endpoint.replace("{bucket}", bucket).replace("{key}", key)

        response = requests.get(
            url=_url + '?exp=' + str(expiration),
            headers=self._api_header
        )

        if response:
            return json.loads(response.text)
        else:
            return None




