import requests
import logging
import json
from maap.utils import endpoints
from maap.utils import requests_utils
from maap.utils import endpoints


class Secrets:
    """
    Functions used for member secrets API interfacing
    """
    def __init__(self, member_endpoint, api_header):
        self._api_header = api_header
        self._members_endpoint = f"{member_endpoint}/{endpoints.MEMBERS_SECRETS}"
        self._logger = logging.getLogger(__name__)


    def get_secrets(self):
        """
        Returns a list of secrets for a given user.

        Returns:
            list: Secret names for a given user.
        """
        try:
            response = requests.get(
                url = self._members_endpoint,
                headers=self._api_header
            )

            return json.loads(response.text)

        except Exception as ex:
            raise(f"Error retrieving secrets: {ex}")


    def get_secret(self, secret_name=None):
        """
        Returns secret value for provided secret name.

        Args:
            secret_name (str, required): Secret name.

        Returns:
            dict: Secret name and value.

        Raises:
            ValueError: If secret name is not provided.
        """
        try:
            if secret_name is None:
                raise ValueError("Failed to get secret value. Please provide secret name.")

            response = requests.get(
                url = f"{self._members_endpoint}/{secret_name}",
                headers=self._api_header
            )

            return json.loads(response.text)

        except Exception as ex:
            raise(f"Error retrieving secret: {ex}")


    def add_secret(self, secret_name=None, secret_value=None):
        """
        Adds a secret. Secret name must be provided. Secret value may be null.

        Args:
            secret_name (str, required): Secret name.
            secret_value (str, optional): Secret value.

        Returns:
            dict: Containing name and value of secret that was just added.

        Raises:
            ValueError: If secret name is not provided.
        """
        try:
            if secret_name is None:
                raise ValueError("Failed to add secret. Please provide secret name.")

            response = requests.post(
                url = self._members_endpoint,
                headers=self._api_header,
                data=json.dumps({"secret_name": secret_name, "secret_value": secret_value})
            )

            return json.loads(response.text)

        except Exception as ex:
            raise(f"Error adding secret: {ex}")



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
        try:
            if secret_name is None:
                raise ValueError("Failed to delete secret. Please provide secret name.")

            response = requests.delete(
                url = f"{self._members_endpoint}/{secret_name}",
                headers=self._api_header
            )

            return json.loads(response.text)

        except Exception as ex:
            raise(f"Error deleting secret: {ex}")
    





