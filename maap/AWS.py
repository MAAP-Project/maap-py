"""
AWS Credential Management
=========================

This module provides the :class:`AWS` class for obtaining AWS credentials
and signed URLs for accessing S3 data.

The AWS class supports multiple credential scenarios:

* Requester-pays bucket access (for public datasets requiring authentication)
* Pre-signed URL generation for temporary access
* Earthdata S3 credentials for accessing external DAACs
* Workspace bucket credentials for MAAP-managed storage

Example
-------
Get credentials for S3 access::

    from maap.maap import MAAP

    maap = MAAP()

    # Get requester-pays credentials
    creds = maap.aws.requester_pays_credentials()
    print(f"Access Key: {creds['accessKeyId']}")

    # Generate a signed URL
    signed = maap.aws.s3_signed_url('bucket-name', 'path/to/file.h5')
    print(f"Signed URL: {signed['url']}")

See Also
--------
:class:`maap.maap.MAAP` : Main client class
"""

import json
import logging
import urllib
import requests


class AWS:
    """
    Interface for AWS credential operations.

    The AWS class provides methods to obtain temporary AWS credentials and
    pre-signed URLs for accessing S3 data from various sources.

    Parameters
    ----------
    requester_pays_endpoint : str
        URL endpoint for requester-pays credentials.
    s3_signed_url_endpoint : str
        URL endpoint for generating signed URLs.
    earthdata_s3_credentials_endpoint : str
        URL endpoint for Earthdata S3 credentials.
    workspace_bucket_endpoint : str
        URL endpoint for workspace bucket credentials.
    api_header : dict
        HTTP headers including authentication tokens.

    Examples
    --------
    Access via MAAP client::

        >>> maap = MAAP()
        >>> creds = maap.aws.requester_pays_credentials()
        >>> print(f"Key: {creds['accessKeyId']}")

    Notes
    -----
    The AWS instance is automatically created when initializing the
    :class:`~maap.maap.MAAP` client and is accessible via ``maap.aws``.

    See Also
    --------
    :class:`~maap.Profile.Profile` : User profile management
    :class:`~maap.Secrets.Secrets` : User secrets management
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
        """
        Get temporary credentials for requester-pays S3 buckets.

        Obtains AWS credentials that can be used to access S3 buckets
        configured with requester-pays enabled.

        Parameters
        ----------
        expiration : int, optional
            Credential validity duration in seconds. Default is 43200
            (12 hours).

        Returns
        -------
        dict
            Dictionary containing AWS credentials:

            - ``accessKeyId``: AWS access key ID
            - ``secretAccessKey``: AWS secret access key
            - ``sessionToken``: Temporary session token
            - ``expiration``: Token expiration timestamp

        Raises
        ------
        requests.HTTPError
            If the credential request fails.

        Examples
        --------
        Get credentials and configure boto3::

            >>> creds = maap.aws.requester_pays_credentials()
            >>> import boto3
            >>> s3 = boto3.client(
            ...     's3',
            ...     aws_access_key_id=creds['accessKeyId'],
            ...     aws_secret_access_key=creds['secretAccessKey'],
            ...     aws_session_token=creds['sessionToken']
            ... )

        Get short-lived credentials::

            >>> creds = maap.aws.requester_pays_credentials(expiration=3600)

        See Also
        --------
        :meth:`earthdata_s3_credentials` : For external DAAC access
        :meth:`workspace_bucket_credentials` : For MAAP workspace access
        """
        headers = self._api_header
        headers["Accept"] = "application/json"

        response = requests.get(
            url=self._requester_pays_endpoint + "?exp=" + str(expiration),
            headers=self._api_header,
        )
        response.raise_for_status()

        return json.loads(response.text)

    def s3_signed_url(self, bucket, key, expiration=60 * 60 * 12):
        """
        Generate a pre-signed URL for an S3 object.

        Creates a temporary URL that allows access to a private S3 object
        without requiring AWS credentials.

        Parameters
        ----------
        bucket : str
            S3 bucket name.
        key : str
            S3 object key (path within the bucket).
        expiration : int, optional
            URL validity duration in seconds. Default is 43200 (12 hours).

        Returns
        -------
        dict
            Dictionary containing:

            - ``url``: Pre-signed URL for the object
            - Additional metadata about the signed URL

        Raises
        ------
        requests.HTTPError
            If the URL generation fails.

        Examples
        --------
        Generate a signed URL::

            >>> result = maap.aws.s3_signed_url(
            ...     'maap-data-store',
            ...     'path/to/data.h5'
            ... )
            >>> print(f"Access data at: {result['url']}")

        Short-lived URL for sharing::

            >>> result = maap.aws.s3_signed_url(
            ...     'bucket', 'key',
            ...     expiration=3600  # 1 hour
            ... )

        Notes
        -----
        Pre-signed URLs allow sharing access to private S3 objects with
        users who don't have AWS credentials. The URL contains temporary
        authentication parameters.

        See Also
        --------
        :meth:`requester_pays_credentials` : Get full credentials instead
        """
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
        """
        Get S3 credentials for accessing external Earthdata DAACs.

        Obtains temporary AWS credentials for accessing data stored in
        external DAAC S3 buckets through Earthdata OAuth.

        Parameters
        ----------
        endpoint_uri : str
            The S3 credential endpoint URL for the target DAAC.
            Each DAAC has a unique endpoint.

        Returns
        -------
        dict
            Dictionary containing AWS credentials and DAAC information:

            - ``accessKeyId``: AWS access key ID
            - ``secretAccessKey``: AWS secret access key
            - ``sessionToken``: Temporary session token
            - ``expiration``: Token expiration timestamp
            - ``DAAC``: The DAAC hostname (extracted from endpoint)

        Raises
        ------
        requests.HTTPError
            If the credential request fails.

        Examples
        --------
        Get LP DAAC credentials::

            >>> creds = maap.aws.earthdata_s3_credentials(
            ...     'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials'
            ... )
            >>> print(f"DAAC: {creds['DAAC']}")
            >>> # Use credentials to access LP DAAC data

        Notes
        -----
        Different DAACs have different S3 credential endpoints. Check the
        DAAC's documentation for the correct endpoint URL.

        Common DAAC endpoints:

        - LP DAAC: ``https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials``
        - NSIDC: ``https://data.nsidc.earthdatacloud.nasa.gov/s3credentials``
        - GES DISC: ``https://data.gesdisc.earthdata.nasa.gov/s3credentials``

        See Also
        --------
        :meth:`requester_pays_credentials` : For MAAP-hosted data
        """
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
        """
        Get credentials for the MAAP workspace bucket.

        Obtains temporary AWS credentials for accessing the user's
        workspace storage on MAAP.

        Returns
        -------
        dict
            Dictionary containing AWS credentials:

            - ``accessKeyId``: AWS access key ID
            - ``secretAccessKey``: AWS secret access key
            - ``sessionToken``: Temporary session token
            - ``expiration``: Token expiration timestamp
            - Additional workspace bucket information

        Raises
        ------
        requests.HTTPError
            If the credential request fails.

        Examples
        --------
        Access workspace bucket::

            >>> creds = maap.aws.workspace_bucket_credentials()
            >>> import boto3
            >>> s3 = boto3.client(
            ...     's3',
            ...     aws_access_key_id=creds['accessKeyId'],
            ...     aws_secret_access_key=creds['secretAccessKey'],
            ...     aws_session_token=creds['sessionToken']
            ... )

        Notes
        -----
        The workspace bucket is user-specific storage provided by MAAP
        for storing analysis results and intermediate files.

        See Also
        --------
        :meth:`requester_pays_credentials` : For accessing external data
        :meth:`maap.maap.MAAP.uploadFiles` : Upload files to shared storage
        """
        headers = self._api_header
        headers["Accept"] = "application/json"

        response = requests.get(
            url=self._workspace_bucket_endpoint,
            headers=self._api_header,
        )

        response.raise_for_status()

        return json.loads(response.text)
