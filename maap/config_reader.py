"""
Configuration Management
========================

This module provides the :class:`MaapConfig` class for managing MAAP client
configuration, including API endpoints, authentication tokens, and settings.

The configuration is automatically fetched from the MAAP API server when
the client is initialized.

Example
-------
Configuration is typically accessed through the MAAP client::

    from maap.maap import MAAP

    maap = MAAP()
    print(f"API Root: {maap.config.maap_api_root}")
    print(f"Page Size: {maap.config.page_size}")

Notes
-----
Configuration values are cached to avoid repeated API calls. Environment
variables can override certain settings.

See Also
--------
:class:`maap.maap.MAAP` : Main client class
"""

import logging
import os
import requests
from urllib.parse import urlparse, urljoin, urlunsplit, SplitResult
from collections import namedtuple
from functools import cache

logger = logging.getLogger(__name__)


def _get_maap_api_host_url_scheme():
    """
    Get the URL scheme for MAAP API connections.

    Returns
    -------
    str
        URL scheme ('http' or 'https'). Defaults to 'https' unless
        overridden by the ``MAAP_API_HOST_SCHEME`` environment variable.
    """
    scheme = os.environ.get("MAAP_API_HOST_SCHEME", None)
    if not scheme:
        logger.debug("No url scheme defined in env var MAAP_API_HOST_SCHEME; defaulting to 'https'.")
        scheme = "https"
    return scheme


def _get_config_url(maap_host):
    """
    Construct the configuration endpoint URL.

    Parameters
    ----------
    maap_host : str
        The MAAP API hostname, optionally with scheme.

    Returns
    -------
    str
        Full URL to the configuration endpoint.

    Raises
    ------
    ValueError
        If an unsupported URL scheme is provided.
    """
    base_url = urlparse(maap_host)
    maap_api_config_endpoint = os.getenv("MAAP_API_CONFIG_ENDPOINT", "api/environment/config")
    supported_schemes = ("http", "https")
    if base_url.scheme and base_url.scheme not in supported_schemes:
        raise ValueError(f"Unsupported scheme for MAAP API host: {base_url.scheme!r}. Must be one of: {', '.join(map(repr, supported_schemes))}.")
    config_url = (
        urljoin(maap_host, maap_api_config_endpoint)
        if base_url.netloc
        else SplitResult(
                scheme=_get_maap_api_host_url_scheme(),
                netloc=base_url.path,
                path=maap_api_config_endpoint,
                query='',
                fragment=''
            ).geturl()
    )
    return config_url


def _get_api_root(config_url, config):
    """
    Determine the API root URL from configuration.

    Parameters
    ----------
    config_url : str
        The configuration endpoint URL.
    config : dict
        Configuration dictionary from the API.

    Returns
    -------
    str
        The API root URL with trailing slash.
    """
    api_root_url = urlparse(config.get("service").get("maap_api_root"))
    config_url = urlparse(config_url)
    return SplitResult(scheme=config_url.scheme, netloc=config_url.netloc, path=api_root_url.path+"/",
                       query='', fragment='').geturl()


@cache
def _get_client_config(maap_host):
    """
    Fetch and cache client configuration from the API.

    Parameters
    ----------
    maap_host : str
        The MAAP API hostname.

    Returns
    -------
    dict
        Configuration dictionary containing service endpoints and settings.

    Notes
    -----
    Results are cached using functools.cache to avoid repeated API calls
    for the same host.
    """
    config_url = _get_config_url(maap_host)
    logger.debug(f"Requesting client config from api at: {config_url}")
    response = requests.get(config_url)
    try:
        response.raise_for_status()
        config = response.json()
        config["service"]["maap_api_root"] = _get_api_root(config_url, config)
        return config
    except Exception as ex:
        logger.error(f"Unable to read maap config from api: {ex}")


class MaapConfig:
    """
    MAAP client configuration manager.

    Manages all configuration settings for the MAAP client, including API
    endpoints, authentication tokens, and operational parameters.

    Parameters
    ----------
    maap_host : str
        The MAAP API hostname (e.g., ``'api.maap-project.org'``).

    Attributes
    ----------
    maap_host : str
        The configured API hostname.
    maap_api_root : str
        Base URL for all API requests.
    maap_token : str
        Authentication token for API requests.
    page_size : int
        Number of results per page for CMR queries.
    content_type : str
        Default content type for CMR requests.

    Endpoint Attributes
    -------------------
    algorithm_register : str
        Endpoint for algorithm registration.
    algorithm_build : str
        Endpoint for algorithm builds.
    mas_algo : str
        Endpoint for algorithm management.
    dps_job : str
        Endpoint for DPS job operations.
    member_dps_token : str
        Endpoint for DPS token retrieval.
    requester_pays : str
        Endpoint for requester-pays credentials.
    edc_credentials : str
        Endpoint for Earthdata Cloud credentials.
    workspace_bucket_credentials : str
        Endpoint for workspace bucket credentials.
    s3_signed_url : str
        Endpoint for generating signed S3 URLs.
    wmts : str
        Endpoint for WMTS tile service.
    member : str
        Endpoint for member/profile operations.
    search_granule_url : str
        Endpoint for CMR granule searches.
    search_collection_url : str
        Endpoint for CMR collection searches.

    AWS Attributes
    --------------
    aws_access_key : str or None
        AWS access key from environment.
    aws_access_secret : str or None
        AWS secret key from environment.
    s3_user_upload_bucket : str or None
        S3 bucket for user uploads.
    s3_user_upload_dir : str or None
        S3 directory prefix for user uploads.

    Other Attributes
    ----------------
    indexed_attributes : list
        Custom indexed attributes for CMR searches.
    mapbox_token : str
        Mapbox access token for visualization.
    tiler_endpoint : str
        URL for the tile rendering service.

    Examples
    --------
    Access configuration through MAAP client::

        >>> maap = MAAP()
        >>> print(f"API Root: {maap.config.maap_api_root}")
        >>> print(f"Granule Search URL: {maap.config.search_granule_url}")

    Notes
    -----
    Configuration is automatically loaded from the MAAP API when the client
    is initialized. Many settings can be overridden via environment variables:

    - ``MAAP_API_HOST``: Override API hostname
    - ``MAAP_CMR_PAGE_SIZE``: Override CMR page size
    - ``MAAP_CMR_CONTENT_TYPE``: Override CMR content type
    - ``MAAP_PGT``: Proxy granting ticket
    - ``MAAP_AWS_ACCESS_KEY_ID``: AWS access key
    - ``MAAP_AWS_SECRET_ACCESS_KEY``: AWS secret key
    - ``MAAP_S3_USER_UPLOAD_BUCKET``: Upload bucket
    - ``MAAP_S3_USER_UPLOAD_DIR``: Upload directory
    - ``MAAP_MAPBOX_ACCESS_TOKEN``: Mapbox token

    See Also
    --------
    :class:`~maap.maap.MAAP` : Main client class
    """

    def __init__(self, maap_host):
        self.__config = _get_client_config(maap_host)
        self.maap_host = maap_host
        self.maap_api_root = self.__config.get("service").get("maap_api_root")
        self.maap_token = self.__config.get("service").get("maap_token")
        self.page_size = os.environ.get("MAAP_CMR_PAGE_SIZE", 20)
        self._PROXY_GRANTING_TICKET = os.environ.get("MAAP_PGT", '')
        self.content_type = os.environ.get("MAAP_CMR_CONTENT_TYPE", "application/echo10+xml")
        self.algorithm_register = self._get_api_endpoint("algorithm_register")
        self.algorithm_build = self._get_api_endpoint("algorithm_build")
        self.mas_algo = self._get_api_endpoint("mas_algo")
        self.dps_job = self._get_api_endpoint("dps_job")
        self.member_dps_token = self._get_api_endpoint("member_dps_token")
        self.requester_pays = self._get_api_endpoint("requester_pays")
        self.edc_credentials = self._get_api_endpoint("edc_credentials")
        self.workspace_bucket_credentials = self._get_api_endpoint("workspace_bucket_credentials")
        self.s3_signed_url = self._get_api_endpoint("s3_signed_url")
        self.wmts = self._get_api_endpoint("wmts")
        self.member = self._get_api_endpoint("member")
        self.tiler_endpoint = self.__config.get("service").get("tiler_endpoint")
        self.aws_access_key = os.environ.get("MAAP_AWS_ACCESS_KEY_ID")
        self.aws_access_secret = os.environ.get("MAAP_AWS_SECRET_ACCESS_KEY")
        self.s3_user_upload_bucket = os.environ.get("MAAP_S3_USER_UPLOAD_BUCKET")
        self.s3_user_upload_dir = os.environ.get("MAAP_S3_USER_UPLOAD_DIR")
        self.search_granule_url = self._get_api_endpoint("search_granule_url")
        self.search_collection_url = self._get_api_endpoint("search_collection_url")
        self.indexed_attributes = self.__config.get("search").get("indexed_attributes")
        self.mapbox_token = os.environ.get("MAAP_MAPBOX_ACCESS_TOKEN", '')

    def _get_api_endpoint(self, config_key):
        """
        Construct a full API endpoint URL.

        Parameters
        ----------
        config_key : str
            The configuration key for the endpoint.

        Returns
        -------
        str
            Full URL for the endpoint.
        """
        endpoint = str(self.__config.get("maap_endpoint").get(config_key)).strip("/")
        return urljoin(self.maap_api_root, endpoint)

    def get(self, profile, key):
        """
        Get a configuration value.

        Parameters
        ----------
        profile : str
            Configuration section name.
        key : str
            Configuration key within the section.

        Returns
        -------
        any
            The configuration value, or None if not found.
        """
        return self.__config.get(profile, key)
