import requests
import logging
import json


class Profile:
    """
    Functions used for Member API interfacing
    """
    def __init__(self, profile_endpoint, api_header):
        self._api_header = api_header
        self._profile_endpoint = profile_endpoint
        self._logger = logging.getLogger(__name__)

    def account_info(self, proxy_ticket = None):
        headers = self._api_header
        headers['Accept'] = 'application/json'
        if 'proxy-ticket' not in headers and proxy_ticket:
            headers['proxy-ticket'] = proxy_ticket

        response = requests.get(
            url=self._profile_endpoint,
            headers=headers
        )

        if response:
            return json.loads(response.text)
        else:
            return None




