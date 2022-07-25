import requests
import logging
import json
import urllib


class AWS:
    """
    Functions used for Member API interfacing
    """
    def __init__(self, requester_pays_endpoint, s3_signed_url_endpoint, earthdata_s3_credentials_endpoint, api_header):
        self._api_header = api_header
        self._requester_pays_endpoint = requester_pays_endpoint
        self._earthdata_s3_credentials_endpoint = earthdata_s3_credentials_endpoint
        self._s3_signed_url_endpoint = s3_signed_url_endpoint
        self._logger = logging.getLogger(__name__)

        def requests_response(_url: str, _headers: object):
            try:
                response = requests.get(
                    url=_url,
                    headers=_headers
                )

                if response:
                    return json.loads(response.text)
                response.raise_for_status()
            except requests.exceptions.ConnectionError as e:
                return ("Connection Error:", e)
            except requests.exceptions.HTTPError as e:
                return ("HTTP Error:", e)
            except requests.exceptions.Timeout as e:
                return ("Request Timeout:", e)
            except requests.exceptions.TooManyRedirects as e:
                return ("Redirects Error:", e)
            except requests.exceptions.RequestException as e:
                return ("Request Exception:", e)

    def requester_pays_credentials(self, expiration=60 * 60 * 12):
        headers = self._api_header
        headers['Accept'] = 'application/json'

        return self.requests_response(self._requester_pays_endpoint + '?exp=' + str(expiration), self._api_header)

    def s3_signed_url(self, bucket, key, expiration=60 * 60 * 12):
        headers = self._api_header
        headers['Accept'] = 'application/json'
        _url = self._s3_signed_url_endpoint.replace("{bucket}", bucket).replace("{key}", key)

        return self.requests_response(_url + '?exp=' + str(expiration), self._api_header)

    def earthdata_s3_credentials(self, endpoint_uri):
        headers = self._api_header
        headers['Accept'] = 'application/json'
        _parsed_endpoint = urllib.parse.quote(urllib.parse.quote(endpoint_uri, safe=''))
        _url = self._earthdata_s3_credentials_endpoint.replace("{endpoint_uri}", _parsed_endpoint)

        return self.requests_response(_url, self._api_header)
