import json
import logging
import os
import sys
import requests
from urllib.parse import urlparse, urljoin, urlunparse
import importlib_resources as resources
from maap.singleton import Singleton

logger = logging.getLogger(__name__)


def _get_client_config(maap_host):
    base_url = urlparse(maap_host)
    # This is added to remove the assumption of scheme specially for local dev testing
    # also maintains backwards compatibility for user to use MAAP("api.maap-project.org")
    if not base_url.scheme.startswith("http"):
        scheme = os.environ.get("MAAP_HOST_SCHEME", None)
        if not scheme:
            logger.debug("No url scheme defined in env var MAAP_HOST_SCHEME using https")
            scheme = "https"
        config_url = base_url._replace(scheme=scheme, netloc=maap_host, path="api/environment/config").geturl()
    else:
        config_url = urljoin(maap_host, "api/environment/config")
    try:
        logger.debug(f"Requesting client config from api at: {config_url}")
        response = requests.get(config_url, verify=False)
        if response.status_code != 200:
            raise EnvironmentError("Unable to get config from supplied maap_host")
        config = response.json()
        return config
    except Exception as ex:
        logger.error(f"Unable to read maap config from api: {ex}")


class MaapConfig(metaclass=Singleton):
    def __init__(self, maap_host=None):
        if maap_host is None:
            raise EnvironmentError("MAAP is not configured. Please initialize a MAAP object: maap = MAAP()")
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
        endpoint = str(self.__config.get("maap_endpoint").get(config_key)).strip("/")
        return os.path.join(self.maap_api_root, endpoint)

    def get(self, profile, key):
        return self.__config.get(profile, key)
    #
    # @property
    # def s3_user_upload_bucket(self):
    #     return self.__s3_user_upload_bucket
    #
    # @s3_user_upload_bucket.setter
    # def s3_user_upload_bucket(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.s3_user_upload_bucket = val
    #     return
    #
    # @property
    # def s3_user_upload_dir(self):
    #     return self.__s3_user_upload_dir
    #
    # @s3_user_upload_dir.setter
    # def s3_user_upload_dir(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.s3_user_upload_dir = val
    #     return
    #
    # @property
    # def indexed_attributes(self):
    #     return self.__indexed_attributes
    #
    # @indexed_attributes.setter
    # def indexed_attributes(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.indexed_attributes = val
    #     return
    #
    # @property
    # def maap_token(self):
    #     return self.__maap_token
    #
    # @maap_token.setter
    # def maap_token(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.maap_token = val
    #     return
    #
    # @property
    # def page_size(self):
    #     return self.__page_size
    #
    # @page_size.setter
    # def page_size(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.page_size = val
    #     return
    #
    # @property
    # def content_type(self):
    #     return self.__content_type
    #
    # @content_type.setter
    # def content_type(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.content_type = val
    #     return
    #
    # @property
    # def algorithm_register(self):
    #     return self.__algorithm_register
    #
    # @algorithm_register.setter
    # def algorithm_register(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.algorithm_register = val
    #     return
    #
    # @property
    # def algorithm_build(self):
    #     return self.__algorithm_build
    #
    # @algorithm_build.setter
    # def algorithm_build(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.algorithm_build = val
    #     return
    #
    # @property
    # def mas_algo(self):
    #     return self.__mas_algo
    #
    # @mas_algo.setter
    # def mas_algo(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.mas_algo = val
    #     return
    #
    # @property
    # def wmts(self):
    #     return self.__wmts
    #
    # @wmts.setter
    # def wmts(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.wmts = val
    #     return
    #
    # @property
    # def member(self):
    #     return self.__member
    #
    # @member.setter
    # def member(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.member = val
    #     return
    #
    # @property
    # def tiler_endpoint(self):
    #     return self.__tiler_endpoint
    #
    # @tiler_endpoint.setter
    # def tiler_endpoint(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.tiler_endpoint = val
    #     return
    #
    # @property
    # def maap_host(self):
    #     return self.__maap_host
    #
    # @maap_host.setter
    # def maap_host(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.maap_host = val
    #     return
    #
    # @property
    # def aws_access_key(self):
    #     return self.__aws_access_key
    #
    # @aws_access_key.setter
    # def aws_access_key(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.aws_access_key = val
    #     return
    #
    # @property
    # def aws_access_secret(self):
    #     return self.__aws_access_secret
    #
    # @aws_access_secret.setter
    # def aws_access_secret(self, val):
    #     """
    #     :param val:
    #     :return: None
    #     """
    #     self.aws_access_secret = val
    #     return
    #
    # @staticmethod
    # def __get_config_path(directory):
    #     return os.path.join(directory, "maap.cfg")
