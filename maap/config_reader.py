import logging
import os
import requests
from urllib.parse import urlparse, urljoin, urlunsplit, SplitResult
from collections import namedtuple
from functools import cache

logger = logging.getLogger(__name__)


def _get_maap_api_host_url_scheme():
    # Check if OS ENV override exists, used by dev testing
    scheme = os.environ.get("MAAP_API_HOST_SCHEME", None)
    if not scheme:
        logger.debug("No url scheme defined in env var MAAP_API_HOST_SCHEME; defaulting to 'https'.")
        scheme = "https"
    return scheme


def _get_config_url(maap_host):
    # This is added to remove the assumption of scheme specially for local dev testing
    # also maintains backwards compatibility for user to use MAAP("api.maap-project.org")
    base_url = urlparse(maap_host)
    maap_api_config_endpoint = os.getenv("MAAP_API_CONFIG_ENDPOINT", "api/environment/config")
    supported_schemes = ("http", "https")
    if base_url.scheme and base_url.scheme not in supported_schemes:
        raise ValueError(f"Unsupported scheme for MAAP API host: {base_url.scheme!r}. Must be one of: {', '.join(map(repr, supported_schemes))}.")
    # If the netloc is empty, that means that the url does not contain scheme:// and the url parser would put the
    # hostname in the path section. See https://docs.python.org/3.11/library/urllib.parse.html#urllib.parse.urlparse
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
    # Set maap api root to currently supplied maap host
    api_root_url = urlparse(config.get("service").get("maap_api_root"))
    config_url = urlparse(config_url)
    # Add trailing slash to api_root_url.path to ensure that urljoin does not remove it
    # eg. urljoin("http://api.maap-project.org/api", "dps") will return http://api.dit.maap-project.org/dps
    # But we want http://api.dit.maap-project.org/api/dps
    return SplitResult(scheme=config_url.scheme, netloc=config_url.netloc, path=api_root_url.path+"/",
                       query='', fragment='').geturl()


@cache
def _get_client_config(maap_host):
    # This is added to remove the assumption of scheme specially for local dev testing
    # also maintains backwards compatibility for user to use MAAP("api.maap-project.org")
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
        # Remove any prefix "/" for urljoin
        endpoint = str(self.__config.get("maap_endpoint").get(config_key)).strip("/")
        return urljoin(self.maap_api_root, endpoint)

    def get(self, profile, key):
        return self.__config.get(profile, key)
