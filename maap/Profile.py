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

    def account_info(self):
        headers = self._api_header
        headers['Accept'] = 'application/json'

        response = requests.get(
            url=self._profile_endpoint,
            headers=self._api_header
        )
        print("graceal1 in account info ")
        print(self._profile_endpoint)
        print(self._api_header)
        # Graceal need to remove this
        return {"username": "grallewellyn"}

        if response:
            return json.loads(response.text)
        else:
            return None




