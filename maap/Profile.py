"""
User Profile Management
=======================

This module provides the :class:`Profile` class for accessing user account
information from the MAAP Member API.

Example
-------
Access user profile information::

    from maap.maap import MAAP

    maap = MAAP()
    profile = maap.profile.account_info()
    print(f"Username: {profile['username']}")

See Also
--------
:class:`maap.maap.MAAP` : Main client class
"""

import requests
import logging
import json


class Profile:
    """
    Interface for user profile operations.

    The Profile class provides methods to retrieve user account information
    from the MAAP Member API.

    Parameters
    ----------
    profile_endpoint : str
        URL endpoint for the Member API.
    api_header : dict
        HTTP headers including authentication tokens.

    Examples
    --------
    Access via MAAP client::

        >>> maap = MAAP()
        >>> info = maap.profile.account_info()
        >>> print(f"User ID: {info['id']}")
        >>> print(f"Username: {info['username']}")

    Notes
    -----
    The Profile instance is automatically created when initializing the
    :class:`~maap.maap.MAAP` client and is accessible via ``maap.profile``.

    See Also
    --------
    :class:`~maap.AWS.AWS` : AWS credential management
    :class:`~maap.Secrets.Secrets` : User secrets management
    """

    def __init__(self, profile_endpoint, api_header):
        self._api_header = api_header
        self._profile_endpoint = profile_endpoint
        self._logger = logging.getLogger(__name__)

    def account_info(self, proxy_ticket=None):
        """
        Get user account information.

        Retrieves the profile information for the currently authenticated user.

        Parameters
        ----------
        proxy_ticket : str, optional
            Proxy granting ticket for authentication. If not provided, uses
            the ticket from environment variables if available.

        Returns
        -------
        dict or None
            Dictionary containing user profile information, or ``None`` if
            the request fails. Profile fields include:

            - ``id``: User's unique identifier
            - ``username``: User's username
            - Additional profile fields as configured by the MAAP platform

        Examples
        --------
        Get basic account info::

            >>> info = maap.profile.account_info()
            >>> if info:
            ...     print(f"Username: {info['username']}")

        Use with explicit proxy ticket::

            >>> info = maap.profile.account_info(proxy_ticket='PGT-...')

        Notes
        -----
        This method is used internally by :meth:`~maap.maap.MAAP.submitJob`
        to automatically include the username with job submissions.

        See Also
        --------
        :class:`~maap.Secrets.Secrets` : Manage user secrets
        """
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
