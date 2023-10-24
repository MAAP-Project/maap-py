import json
import logging
import boto3
import uuid
import urllib.parse
import os
from mapboxgl.utils import *
from mapboxgl.viz import *
from .Result import Collection, Granule, Result
from maap.config_reader import ConfigReader
from maap.dps.dps_job import DPSJob
from maap.utils.requests_utils import RequestsUtils
from maap.utils.Presenter import Presenter
from maap.utils.CMR import CMR
from maap.utils import algorithm_utils
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

    def __init__(self, maap_host='', config_file_path=''):
        self.config = ConfigParser()

        # Adding this for newer capability imported frm SISTER, leaving the rest of config imports as is
        self._singlelton_config = ConfigReader(config_file_path=config_file_path)

        config_paths = list(map(self._get_config_path, [os.path.dirname(config_file_path), os.curdir, os.path.expanduser("~"), os.environ.get("MAAP_CONF") or '.']))

        for loc in config_paths:
            try:
                with open(loc) as source:
                    self.config.read_file(source)
                    logger.debug("Loaded config file from source %s " % loc)
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
        self._MEMBER_DPS_TOKEN = self._get_api_endpoint("member_dps_token")
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
        self._DPS = DpsHelper(self._get_api_header(), self._MEMBER_DPS_TOKEN)
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
                        self._get_api_header(),
                        self._DPS) for result in results][:limit]

    def downloadGranule(self, online_access_url, destination_path=".", overwrite=False):
        """
            Direct download of http Earthdata granule URL (protected or public).

            :param online_access_url: the value of the granule's http OnlineAccessURL
            :param destination_path: use the current directory as default
            :param overwrite: don't download by default if the target file exists
            :return: the file path of the download file
            """

        filename = os.path.basename(urllib.parse.urlparse(online_access_url).path)
        destination_file = filename.replace("/", "")
        final_destination = os.path.join(destination_path, destination_file)

        proxy = Result({})
        proxy._dps = self._DPS
        proxy._cmrFileUrl = self._SEARCH_GRANULE_URL
        proxy._apiHeader = self._get_api_header()
        # noinspection PyProtectedMember
        return proxy._getHttpData(online_access_url, overwrite, final_destination)

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
        headers = RequestsUtils.generate_dps_headers()
        headers['Content-Type'] = 'application/json'
        logger.debug('headers:')
        logger.debug(headers)
        logger.debug('request is')
        if type(arg) is dict:
            arg = json.dumps(arg)
        logger.debug(arg)
        response = requests.post(
            url=self._ALGORITHM_REGISTER,
            data=arg,
            headers=headers
        )
        logger.debug('POST request sent to {}'.format(self._ALGORITHM_REGISTER))
        return response

    def register_algorithm_from_yaml_file(self, file_path):
        algo_config = algorithm_utils.read_yaml_file(file_path)
        return self.registerAlgorithm(algo_config)

    def register_algorithm_from_yaml_file_backwards_compatible(self, file_path):
        algo_yaml = algorithm_utils.read_yaml_file(file_path)
        key_map = {"algo_name": "algorithm_name", "version": "code_version", "environment": "environment_name",
                   "description": "algorithm_description", "docker_url": "docker_container_url",
                   "inputs": "algorithm_params", "run_command": "script_command", "repository_url": "repo_url"}
        output_config = {}
        for key, value in algo_yaml.items():
            if key in key_map:
                if key == "inputs":
                    inputs = []
                    for argument in value:
                        inputs.append({"field": argument.get("name"), "download": argument.get("download")})
                    output_config.update({"algorithm_params": inputs})
                else:
                    output_config.update({key_map.get(key): value})
            else:
                output_config.update({key: value})
        logger.debug("Registering with config %s " % json.dumps(output_config))
        return self.registerAlgorithm(json.dumps(output_config))

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
        logger.debug('DELETE request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.delete(
            url=url,
            headers=headers
        )
        return response

    def getCapabilities(self):
        url = self._DPS_JOB
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def getJob(self, jobid):
        job = DPSJob()
        job.id = jobid
        job.retrieve_attributes()
        return job

    def getJobStatus(self, jobid):
        job = DPSJob()
        job.id = jobid
        return job.retrieve_status()

    def getJobResult(self, jobid):
        job = DPSJob()
        job.id = jobid
        return job.retrieve_result()

    def getJobMetrics(self, jobid):
        job = DPSJob()
        job.id = jobid
        return job.retrieve_metrics()

    def dismissJob(self, jobid):
        job = DPSJob()
        job.id = jobid
        return job.dismiss_job()

    def deleteJob(self, jobid):
        job = DPSJob()
        job.id = jobid
        return job.delete_job()

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

    def submitJob(self, retrieve_attributes=False, **kwargs):
        response = self._DPS.submit_job(request_url=self._DPS_JOB, **kwargs)
        job = DPSJob()
        job.set_submitted_job_result(response)
        try:
            if retrieve_attributes:
                job.retrieve_attributes()
        except:
            logger.debug(f"Unable to retrieve attributes for job: {job}")
        return job

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

