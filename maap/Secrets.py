import requests
import logging
import json
from maap.utils import endpoints
from maap.utils import requests_utils
from maap.utils import endpoints

logger = logging.getLogger(__name__)

class Secrets:
    """
    Functions used for member secrets API interfacing
    """
    def __init__(self, member_endpoint, api_header):
        self._api_header = api_header
        self._members_endpoint = f"{member_endpoint}/{endpoints.MEMBERS_SECRETS}"


    def get_secrets(self):
        """
        Returns a list of secrets for a given user.

        Returns:
            list: Returns a list of dicts containing secret names e.g. [{'secret_name': 'secret1'}, {'secret_name': 'secret2'}].
        """
        try:
            response = requests.get(
                url = self._members_endpoint,
                headers=self._api_header
            )
            logger.debug(f"Response from get_secrets request: {response.text}")
            return json.loads(response.text)
        except Exception as e:
            raise(f"Error retrieving secrets: {e}")


    def get_secret(self, secret_name):
        """
        Returns secret value for provided secret name.

        Args:
            secret_name (str, required): Secret name.

        Returns:
            string: Secret value.

        Raises:
            ValueError: If secret name is not provided.
        """
        if secret_name is None:
            raise ValueError("Secret name parameter cannot be None.")

        try:
            response = requests.get(
                url = f"{self._members_endpoint}/{secret_name}",
                headers=self._api_header
            )

            # Return secret value directly for user ease-of-use
            if response.ok:
                response = response.json()
                return response["secret_value"]

            logger.debug(f"Response from get_secret request: {response.text}")
            return json.loads(response.text)
        except Exception as e:
            raise(f"Error retrieving secret: {e}")


    def add_secret(self, secret_name=None, secret_value=None):
        """
        Adds a secret. Secret name must be provided. Secret value may be null.

        Args:
            secret_name (str, required): Secret name.
            secret_value (str, optional): Secret value.

        Returns:
            dict: Containing name and value of secret that was just added.

        Raises:
            ValueError: If secret name or secret value is not provided.
        """
        if secret_name is None or secret_value is None:
            raise ValueError("Failed to add secret. Secret name and secret value must not be 'None'.")

        try:
            response = requests.post(
                url = self._members_endpoint,
                headers=self._api_header,
                data=json.dumps({"secret_name": secret_name, "secret_value": secret_value})
            )

            logger.debug(f"Response from add_secret: {response.text}")
            return json.loads(response.text)
        except Exception as e:
            raise(f"Error adding secret: {e}")


    def delete_secret(self, secret_name=None):
        """
        Deletes a secret.

        Args:
            secret_name (str, required): Secret name.

        Returns:
            dict: Containing response code and message indicating whether or not deletion was successful.

        Raises:
            ValueError: If secret name is not provided.
        """
        if secret_name is None:
            raise ValueError("Failed to delete secret. Please provide secret name.")

        try:
            response = requests.delete(
                url = f"{self._members_endpoint}/{secret_name}",
                headers=self._api_header
            )

            logger.debug(f"Response from delete_secret: {response.text}")
            return json.loads(response.text)
        except Exception as e:
            raise(f"Error deleting secret: {e}")
    





