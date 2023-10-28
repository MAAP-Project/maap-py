import json
import logging
import os

from maap.singleton import Singleton

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

logger = logging.getLogger(__name__)


class ConfigReader(metaclass=Singleton):
    def __init__(self, maap_host=None, config_file_path=''):
        self.__config = ConfigParser()
        configfile_present = False
        config_paths = list(map(self.__get_config_path, [os.path.dirname(config_file_path), os.curdir, os.path.expanduser("~"), os.environ.get("MAAP_CONF") or '.']))
        for loc in config_paths:
            try:
                with open(loc) as source:
                    self.__config.read_file(source)
                    configfile_present = True
                    break
            except IOError:
                pass
        if not configfile_present:
            raise IOError("No maap.cfg file found. Locations checked: " + '; '.join(config_paths))

        if maap_host is None:
            if not self.__config.has_option('service', 'maap_host'):
                raise KeyError("No maap_host configured in config file or provided as parameter")
        else:
            self.__config.set('service', 'maap_host', maap_host)
        self.__maap_host = self.__config.get("service", "maap_host")

        self.__maap_token = self.__config.get("service", "maap_token")
        self.__page_size = self.__config.getint("request", "page_size")
        self.__content_type = self.__config.get("request", "content_type")
        self.__algorithm_register = self._get_api_endpoint("algorithm_register")
        self.__algorithm_build = self._get_api_endpoint("algorithm_build")
        self.__mas_algo = self._get_api_endpoint("mas_algo")
        self.__dps_job = self._get_api_endpoint("dps_job")
        self.__wmts = self._get_api_endpoint("wmts")
        self.__member = self._get_api_endpoint("member")
        self.__tiler_endpoint = self.__config.get("service", "tiler_endpoint")




        self.__aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID") or self.__config.get("aws", "aws_access_key_id")
        self.__aws_access_secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or self.__config.get("aws",
                                                                                              "aws_secret_access_key")
        self.__s3_user_upload_bucket = os.environ.get("S3_USER_UPLOAD_BUCKET") or self.__config.get("aws",
                                                                                                  "user_upload_bucket")
        self.__s3_user_upload_dir = os.environ.get("S3_USER_UPLOAD_DIR") or self.__config.get("aws",
                                                                                            "user_upload_directory")
        self.__indexed_attributes = json.loads(self.__config.get("search", "indexed_attributes"))

    def _get_api_endpoint(self, config_key):
        return 'https://{}/api/{}'.format(self.maap_host, self.__config.get("maap_endpoint", config_key))

    def get(self, profile, key):
        return self.__config.get(profile, key)

    @property
    def s3_user_upload_bucket(self):
        return self.__s3_user_upload_bucket

    @s3_user_upload_bucket.setter
    def s3_user_upload_bucket(self, val):
        """
        :param val:
        :return: None
        """
        self.__s3_user_upload_bucket = val
        return

    @property
    def s3_user_upload_dir(self):
        return self.__s3_user_upload_dir

    @s3_user_upload_dir.setter
    def s3_user_upload_dir(self, val):
        """
        :param val:
        :return: None
        """
        self.__s3_user_upload_dir = val
        return

    @property
    def indexed_attributes(self):
        return self.__indexed_attributes

    @indexed_attributes.setter
    def indexed_attributes(self, val):
        """
        :param val:
        :return: None
        """
        self.__indexed_attributes = val
        return

    @property
    def maap_token(self):
        return self.__maap_token

    @maap_token.setter
    def maap_token(self, val):
        """
        :param val:
        :return: None
        """
        self.__maap_token = val
        return

    @property
    def page_size(self):
        return self.__page_size

    @page_size.setter
    def page_size(self, val):
        """
        :param val:
        :return: None
        """
        self.__page_size = val
        return

    @property
    def content_type(self):
        return self.__content_type

    @content_type.setter
    def content_type(self, val):
        """
        :param val:
        :return: None
        """
        self.__content_type = val
        return

    @property
    def algorithm_register(self):
        return self.__algorithm_register

    @algorithm_register.setter
    def algorithm_register(self, val):
        """
        :param val:
        :return: None
        """
        self.__algorithm_register = val
        return

    @property
    def algorithm_build(self):
        return self.__algorithm_build

    @algorithm_build.setter
    def algorithm_build(self, val):
        """
        :param val:
        :return: None
        """
        self.__algorithm_build = val
        return

    @property
    def mas_algo(self):
        return self.__mas_algo

    @mas_algo.setter
    def mas_algo(self, val):
        """
        :param val:
        :return: None
        """
        self.__mas_algo = val
        return

    @property
    def dps_job(self):
        return self.__dps_job

    @dps_job.setter
    def dps_job(self, val):
        """
        :param val:
        :return: None
        """
        self.__dps_job = val
        return

    @property
    def wmts(self):
        return self.__wmts

    @wmts.setter
    def wmts(self, val):
        """
        :param val:
        :return: None
        """
        self.__wmts = val
        return

    @property
    def member(self):
        return self.__member

    @member.setter
    def member(self, val):
        """
        :param val:
        :return: None
        """
        self.__member = val
        return

    @property
    def tiler_endpoint(self):
        return self.__tiler_endpoint

    @tiler_endpoint.setter
    def tiler_endpoint(self, val):
        """
        :param val:
        :return: None
        """
        self.__tiler_endpoint = val
        return

    @property
    def maap_host(self):
        return self.__maap_host

    @maap_host.setter
    def maap_host(self, val):
        """
        :param val:
        :return: None
        """
        self.__maap_host = val
        return

    @property
    def aws_access_key(self):
        return self.__aws_access_key

    @aws_access_key.setter
    def aws_access_key(self, val):
        """
        :param val:
        :return: None
        """
        self.__aws_access_key = val
        return

    @property
    def aws_access_secret(self):
        return self.__aws_access_secret

    @aws_access_secret.setter
    def aws_access_secret(self, val):
        """
        :param val:
        :return: None
        """
        self.__aws_access_secret = val
        return

    @staticmethod
    def __get_config_path(directory):
        return os.path.join(directory, "maap.cfg")
