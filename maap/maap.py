import json
import logging
import boto3
import uuid
import urllib.parse
import os
import sys

import importlib_resources as resources
import requests
from maap.Result import Collection, Granule, Result
from maap.config_reader import MaapConfig
from maap.dps.dps_job import DPSJob
from maap.utils import requests_utils
from maap.utils.Presenter import Presenter
from maap.utils.CMR import CMR
from maap.utils import algorithm_utils
from maap.Profile import Profile
from maap.AWS import AWS
from maap.dps.DpsHelper import DpsHelper
from maap.utils import endpoints

logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')


class MAAP(object):

    def __init__(self, maap_host=os.getenv('MAAP_API_HOST', 'api.maap-project.org')):
        self.config = MaapConfig(maap_host=maap_host)

        self._CMR = CMR(self.config.indexed_attributes, self.config.page_size, self._get_api_header())
        self._DPS = DpsHelper(self._get_api_header(), self.config.member_dps_token)
        self.profile = Profile(self.config.member, self._get_api_header())
        self.aws = AWS(
            self.config.requester_pays,
            self.config.s3_signed_url,
            self.config.edc_credentials,
            self.config.workspace_bucket_credentials,
            self._get_api_header()
        )

    def _get_api_header(self, content_type=None):

        api_header = {'Accept': content_type if content_type else self.config.content_type, 'token': self.config.maap_token}

        if os.environ.get("MAAP_PGT"):
            api_header['proxy-ticket'] = os.environ.get("MAAP_PGT")

        return api_header

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
        results = self._CMR.get_search_results(url=self.config.search_granule_url, limit=limit, **kwargs)
        return [Granule(result,
                        self.config.aws_access_key,
                        self.config.aws_access_secret,
                        self.config.search_granule_url,
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
        proxy._cmrFileUrl = self.config.search_granule_url
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
        results = self._CMR.get_search_results(url=self.config.search_collection_url, limit=limit, **kwargs)
        return [Collection(result, self.config.maap_host) for result in results][:limit]

    def getQueues(self):
        url = os.path.join(self.config.algorithm_register, 'resource')
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(self.config.algorithm_register))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            headers=self._get_api_header()
        )
        return response

    def registerAlgorithm(self, arg):
        logger.debug('Registering algorithm with args ')
        if type(arg) is dict:
            arg = json.dumps(arg)
        logger.debug(arg)
        response = requests_utils.make_request(url=self.config.algorithm_register, config=self.config,
                                               content_type='application/json', request_type=requests_utils.POST,
                                               data=arg)
        logger.debug('POST request sent to {}'.format(self.config.algorithm_register))
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
        url = self.config.mas_algo
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
        url = os.path.join(self.config.mas_algo, algoid)
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
        url = self.config.mas_algo.replace('algorithm', 'publish')
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
        url = os.path.join(self.config.mas_algo, algoid)
        headers = self._get_api_header()
        logger.debug('DELETE request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.delete(
            url=url,
            headers=headers
        )
        return response


    def getJob(self, jobid):
        job = DPSJob(self.config)
        job.id = jobid
        job.retrieve_attributes()
        return job

    def getJobStatus(self, jobid):
        job = DPSJob(self.config)
        job.id = jobid
        return job.retrieve_status()

    def getJobResult(self, jobid):
        job = DPSJob(self.config)
        job.id = jobid
        return job.retrieve_result()

    def getJobMetrics(self, jobid):
        job = DPSJob(self.config)
        job.id = jobid
        return job.retrieve_metrics()

    def cancelJob(self, jobid):
        job = DPSJob(self.config)
        job.id = jobid
        return job.cancel_job()

    def listJobs(self, username=None, **kwargs):
        if username is None and self.profile is not None and 'username' in self.profile.account_info().keys():
            username = self.profile.account_info()['username']

        url = os.path.join(self.config.dps_job, username, endpoints.DPS_JOB_LIST)
        valid_keys = ['algo_id', 'end_time', 'get_job_details', 'offset', 'page_size', 'priority', 'queue', 'start_time', 'status', 'tag', 'version']

        params = {k: v for k, v in kwargs.items() if k in valid_keys and v is not None}
        
        # DPS requests use 'job_type', which is a concatenation of 'algo_id' and 'version'
        if 'algo_id' in params and 'version' in params:
            params['job_type'] = params['algo_id'] + ':' + params['version']

        params.pop('algo_id', None)
        params.pop('version', None)

        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            headers=headers,
            params=params,
        )
        return response

    def submitJob(self, identifier, algo_id, version, queue, retrieve_attributes=False, **kwargs):
        response = self._DPS.submit_job(request_url=self.config.dps_job,
                                        identifier=identifier, algo_id=algo_id, version=version, queue=queue, **kwargs)
        job = DPSJob(self.config)
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
        bucket = self.config.s3_user_upload_bucket
        prefix = self.config.s3_user_upload_dir
        uuid_dir = uuid.uuid4()
        # TODO(aimee): This should upload to a user-namespaced directory
        for filename in filenames:
            basename = os.path.basename(filename)
            response = self._upload_s3(filename, bucket, f"{prefix}/{uuid_dir}/{basename}")
        return f"Upload file subdirectory: {uuid_dir} (keep a record of this if you want to share these files with other users)"

    def _get_browse(self, granule_ur):
        response = requests.get(
            url=f'{self.config.wmts}/GetTile',
            params=dict(granule_ur=granule_ur),
            headers=dict(Accept='application/json')
        )
        return response

    def _get_capabilities(self, granule_ur):
        response = requests.get(
            url=f'{self.config.wmts}/GetCapabilities',
            params=dict(granule_ur=granule_ur),
            headers=dict(Accept='application/json')
        )
        return response

    def show(self, granule, display_config={}):
        from mapboxgl.viz import RasterTilesViz

        granule_ur = granule['Granule']['GranuleUR']
        browse_file = json.loads(self._get_browse(granule_ur).text)['browse']
        capabilities = json.loads(self._get_capabilities(granule_ur).text)['body']
        presenter = Presenter(capabilities, display_config)
        query_params = dict(url=browse_file, **presenter.display_config)
        qs = urllib.parse.urlencode(query_params)
        tiles_url = f"{self.config.tiler_endpoint}/tiles/{{z}}/{{x}}/{{y}}.png?{qs}"
        viz = RasterTilesViz(
            tiles_url,
            height='800px',
            zoom=10,
            access_token=self.config.mapbox_token,
            tiles_size=256,
            tiles_bounds=presenter.bbox,
            center=(presenter.lng, presenter.lat),
            tiles_minzoom=presenter.minzoom,
            tiles_maxzoom=presenter.maxzoom,
        )
        viz.show()


if __name__ == "__main__":
    print("initialized")
