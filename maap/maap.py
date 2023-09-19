import logging
import boto3
import uuid
import urllib.parse
import json
import os
from mapboxgl.utils import *
from mapboxgl.viz import *

from .Result import Collection, Granule
from maap.utils.Presenter import Presenter
from maap.utils.CMR import CMR
from maap.Profile import Profile
from maap.AWS import AWS
from maap.dps.DpsHelper import DpsHelper
from maap.utils import endpoints


logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


class MAAP(object):
    def __init__(self, maap_host=''):
        self.config = ConfigParser()

        config_paths = list(map(self._get_config_path, [os.curdir, os.path.expanduser("~"), os.environ.get("MAAP_CONF") or '.']))

        for loc in config_paths:
            try:
                with open(loc) as source:
                    self.config.read_file(source)
                    break
            except IOError:
                pass

        if not self.config.has_option('service', 'maap_host'):
            raise IOError("No maap.cfg file found. Locations checked: " + '; '.join(config_paths))

        self._MAAP_TOKEN = self.config.get("service", "maap_token")
        self._PROXY_GRANTING_TICKET = os.environ.get("MAAP_PGT") or ''
        self._PAGE_SIZE = self.config.getint("request", "page_size")
        self._CONTENT_TYPE = self.config.get("request", "content_type")

        # Take maap_host from constructor if provided, otherwise use the default config value
        self._MAAP_HOST = maap_host if maap_host else self.config.get("service", "maap_host")
        self._SEARCH_GRANULE_URL = self._get_api_endpoint("search_granule_url")
        self._SEARCH_COLLECTION_URL = self._get_api_endpoint("search_collection_url")
        self._ALGORITHM_REGISTER = self._get_api_endpoint("algorithm_register")
        self._ALGORITHM_BUILD = self._get_api_endpoint("algorithm_build")
        self._MAS_ALGO = self._get_api_endpoint("mas_algo")
        self._DPS_JOB = self._get_api_endpoint("dps_job")
        self._WMTS = self._get_api_endpoint("wmts")
        self._MEMBER = self._get_api_endpoint("member")
        self._REQUESTER_PAYS = self._get_api_endpoint("requester_pays")
        self._EDC_CREDENTIALS = self._get_api_endpoint("edc_credentials")
        self._S3_SIGNED_URL = self._get_api_endpoint("s3_signed_url")

        self._TILER_ENDPOINT = self.config.get("service", "tiler_endpoint")
        self._AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID") or self.config.get("aws", "aws_access_key_id")
        self._AWS_ACCESS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY") or self.config.get("aws", "aws_secret_access_key")
        self._S3_USER_UPLOAD_BUCKET = os.environ.get("S3_USER_UPLOAD_BUCKET") or self.config.get("aws", "user_upload_bucket")
        self._S3_USER_UPLOAD_DIR = os.environ.get("S3_USER_UPLOAD_DIR") or self.config.get("aws", "user_upload_directory")
        self._MAPBOX_TOKEN = os.environ.get("MAPBOX_ACCESS_TOKEN") or ''
        self._INDEXED_ATTRIBUTES = json.loads(self.config.get("search", "indexed_attributes"))

        self._CMR = CMR(self._INDEXED_ATTRIBUTES, self._PAGE_SIZE, self._get_api_header())
        self._DPS = DpsHelper(self._get_api_header())
        self.profile = Profile(self._MEMBER, self._get_api_header())
        self.aws = AWS(self._REQUESTER_PAYS, self._S3_SIGNED_URL, self._EDC_CREDENTIALS, self._get_api_header())

    def _get_api_endpoint(self, config_key):
        return 'https://{}/api/{}'.format(self._MAAP_HOST, self.config.get("maap_endpoint", config_key))

    def _get_api_header(self, content_type=None):

        api_header = {'Accept': content_type if content_type else self._CONTENT_TYPE, 'token': self._MAAP_TOKEN}

        if os.environ.get("MAAP_PGT"):
            api_header['proxy-ticket'] = os.environ.get("MAAP_PGT")

        return api_header

    def _get_config_path(self, directory):
        return os.path.join(directory, "maap.cfg")

    def _upload_s3(self, filename, bucket, objectKey):
        """
        Upload file to S3, utility function useful for mocking in tests.
        :param filename (string) - local filename (and path)
        :param bucket (string) - S3 bucket to upload to
        :param objectKey (string) - S3 directory and filename to upload the local file to
        :return: S3 upload_file response
        """
        return s3_client.upload_file(filename, bucket, objectKey)

    def searchGranule(self, limit=20, **kwargs):
        """
            Search the CMR granules

            :param limit: limit of the number of results
            :param kwargs: search parameters
            :return: list of results (<Instance of Result>)
            """
        results = self._CMR.get_search_results(url=self._SEARCH_GRANULE_URL, limit=limit, **kwargs)
        return [Granule(result,
                        self._AWS_ACCESS_KEY,
                        self._AWS_ACCESS_SECRET,
                        self._SEARCH_GRANULE_URL,
                        self._get_api_header()) for result in results][:limit]

    def getCallFromEarthdataQuery(self, query, variable_name='maap', limit=1000):
        """
            Generate a literal string to use for calling the MAAP API

            :param query: a Json-formatted string from an Earthdata search-style query. See: https://github.com/MAAP-Project/earthdata-search/blob/master/app/controllers/collections_controller.rb
            :param variable_name: the name of the MAAP variable to qualify the search call
            :param limit: the max records to return
            :return: string in the form of a MAAP API call
            """
        return self._CMR.generateGranuleCallFromEarthDataRequest(query, variable_name, limit)

    def getCallFromCmrUri(self, search_url, variable_name='maap', limit=1000, search='granule'):
        """
            Generate a literal string to use for calling the MAAP API

            :param search_url: a Json-formatted string from an Earthdata search-style query. See: https://github.com/MAAP-Project/earthdata-search/blob/master/app/controllers/collections_controller.rb
            :param variable_name: the name of the MAAP variable to qualify the search call
            :param limit: the max records to return
            :param search: defaults to 'granule' search, otherwise can be a 'collection' search
            :return: string in the form of a MAAP API call
            """
        return self._CMR.generateCallFromEarthDataQueryString(search_url, variable_name, limit, search)

    def searchCollection(self, limit=100, **kwargs):
        """
        Search the CMR collections
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        results = self._CMR.get_search_results(url=self._SEARCH_COLLECTION_URL, limit=limit, **kwargs)
        return [Collection(result, self._MAAP_HOST) for result in results][:limit]

    def getQueues(self):
        url = os.path.join(self._ALGORITHM_REGISTER, 'resource')
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(self._ALGORITHM_REGISTER))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            headers=self._get_api_header()
        )
        return response

    def registerAlgorithm(self, arg):
        headers = self._get_api_header()
        headers['Content-Type'] = 'application/json'
        logger.debug('POST request sent to {}'.format(self._ALGORITHM_REGISTER))
        logger.debug('headers:')
        logger.debug(headers)
        logger.debug('request is')
        logger.debug(arg)
        response = requests.post(
            url=self._ALGORITHM_REGISTER,
            data=arg,
            headers=headers
        )
        return response

    def listAlgorithms(self):
        url = self._MAS_ALGO
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def describeAlgorithm(self, algoid):
        url = os.path.join(self._MAS_ALGO, algoid)
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def publishAlgorithm(self, algoid):
        url = self._MAS_ALGO.replace('algorithm','publish')
        headers = self._get_api_header()
        body = { "algo_id": algoid}
        logger.debug('POST request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        logger.debug('body:')
        logger.debug(body)
        response = requests.post(
            url=url,
            headers=headers,
            data=body
        )
        return response

    def deleteAlgorithm(self, algoid):
        url = os.path.join(self._MAS_ALGO, algoid)
        headers = self._get_api_header()
        logging.debug('DELETE request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.delete(
            url=url,
            headers=headers
        )
        return response

    def getCapabilities(self):
        url = self._DPS_JOB
        headers = self._get_api_header()
        logging.debug('GET request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def getJobStatus(self, jobid):
        url = os.path.join(self._DPS_JOB, jobid, endpoints.DPS_JOB_STATUS)
        headers = self._get_api_header()
        logging.debug('GET request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def getJobResult(self, jobid):
        url = os.path.join(self._DPS_JOB, jobid)
        headers = self._get_api_header()
        logging.debug('GET request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def getJobMetrics(self, jobid):
        url = os.path.join(self._DPS_JOB, jobid, endpoints.DPS_JOB_METRICS)
        headers = self._get_api_header()
        logging.debug('GET request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def dismissJob(self, jobid):
        url = os.path.join(self._DPS_JOB, endpoints.DPS_JOB_DISMISS, jobid)
        headers = self._get_api_header()
        logging.debug('DELETE request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.delete(
            url=url,
            headers=headers
        )
        return response

    def deleteJob(self, jobid):
        url = os.path.join(self._DPS_JOB, jobid)
        headers = self._get_api_header()
        logging.debug('DELETE request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.delete(
            url=url,
            headers=headers
        )
        return response

    def listJobs(self, username=None):
        if username==None and self.profile is not None and 'username' in self.profile.account_info().keys():
            username = self.profile.account_info()['username']
        url = os.path.join(self._DPS_JOB, username, endpoints.DPS_JOB_LIST)
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def submitJob(self, **kwargs):
        response = self._DPS.submit_job(request_url=self._DPS_JOB, **kwargs)
        return response

    def uploadFiles(self, filenames):
        """
        Uploads files to a user-added staging directory.
        Enables users of maap-py to potentially share files generated on the MAAP.
        :param filenames: List of one or more filenames to upload
        :return: String message including UUID of subdirectory of files
        """
        bucket = self._S3_USER_UPLOAD_BUCKET
        prefix = self._S3_USER_UPLOAD_DIR
        uuid_dir = uuid.uuid4()
        # TODO(aimee): This should upload to a user-namespaced directory
        for filename in filenames:
            basename = os.path.basename(filename)
            response = self._upload_s3(filename, bucket, f"{prefix}/{uuid_dir}/{basename}")
        return f"Upload file subdirectory: {uuid_dir} (keep a record of this if you want to share these files with other users)"

    def _get_browse(self, granule_ur):
        response = requests.get(
            url='{}/GetTile'.format(self._WMTS),
            params=dict(granule_ur=granule_ur),
            headers=dict(Accept='application/json')
        )
        return response

    def _get_capabilities(self, granule_ur):
        response = requests.get(
            url='{}/GetCapabilities'.format(self._WMTS),
            params=dict(granule_ur=granule_ur),
            headers=dict(Accept='application/json')
        )
        return response

    def show(self, granule, display_config={}):
        granule_ur = granule['Granule']['GranuleUR']
        browse_file = json.loads(self._get_browse(granule_ur).text)['browse']
        capabilities = json.loads(self._get_capabilities(granule_ur).text)['body']
        presenter = Presenter(capabilities, display_config)
        query_params = dict(url=browse_file, **presenter.display_config)
        qs = urllib.parse.urlencode(query_params)
        tiles_url = f"{self._TILER_ENDPOINT}/tiles/{{z}}/{{x}}/{{y}}.png?{qs}"
        viz = RasterTilesViz(
            tiles_url,
            height='800px',
            zoom=10,
            access_token=self._MAPBOX_TOKEN,
            tiles_size=256,
            tiles_bounds=presenter.bbox,
            center=(presenter.lng, presenter.lat),
            tiles_minzoom=presenter.minzoom,
            tiles_maxzoom=presenter.maxzoom,
        )
        viz.show()


if __name__ == "__main__":
    print("initialized")

