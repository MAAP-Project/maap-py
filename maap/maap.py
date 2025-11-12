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
from maap.Secrets import Secrets
from maap.dps.DpsHelper import DpsHelper
from maap.utils import endpoints
from maap.utils import job

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
        self.secrets = Secrets(self.config.member, self._get_api_header(content_type="application/json"))

    def _get_api_header(self, content_type=None):

        api_header = {'Accept': content_type if content_type else self.config.content_type, 'token': self.config.maap_token, 'Content-Type': content_type if content_type else self.config.content_type}

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

    def search_granule(self, limit=20, **kwargs):
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

    def download_granule(self, online_access_url, destination_path=".", overwrite=False):
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

    def get_call_from_earthdata_query(self, query, variable_name='maap', limit=1000):
        """
            Generate a literal string to use for calling the MAAP API

            :param query: a Json-formatted string from an Earthdata search-style query. See: https://github.com/MAAP-Project/earthdata-search/blob/master/app/controllers/collections_controller.rb
            :param variable_name: the name of the MAAP variable to qualify the search call
            :param limit: the max records to return
            :return: string in the form of a MAAP API call
            """
        return self._CMR.generateGranuleCallFromEarthDataRequest(query, variable_name, limit)

    def get_call_from_cmr_uri(self, search_url, variable_name='maap', limit=1000, search='granule'):
        """
            Generate a literal string to use for calling the MAAP API

            :param search_url: a Json-formatted string from an Earthdata search-style query. See: https://github.com/MAAP-Project/earthdata-search/blob/master/app/controllers/collections_controller.rb
            :param variable_name: the name of the MAAP variable to qualify the search call
            :param limit: the max records to return
            :param search: defaults to 'granule' search, otherwise can be a 'collection' search
            :return: string in the form of a MAAP API call
            """
        return self._CMR.generateCallFromEarthDataQueryString(search_url, variable_name, limit, search)

    def search_collection(self, limit=100, **kwargs):
        """
        Search the CMR collections
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        results = self._CMR.get_search_results(url=self.config.search_collection_url, limit=limit, **kwargs)
        return [Collection(result, self.config.maap_host) for result in results][:limit]

    def get_queues(self):
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

    # def register_algorithm_from_cwl_file(self, file_path):
    #     """
    #     Registers an algorithm from a CWL file
    #     """
    #     # Read cwl file returns a dict in the format to register an algorithm without a CWL
    #     algo_config = algorithm_utils.read_cwl_file(file_path)
    #     headers = self._get_api_header(content_type='application/json')
    #     logger.debug('POST request sent to {}'.format(self.config.processes_ogc))
    #     response = requests.post(
    #         url=self.config.processes_ogc,
    #         headers=headers,
    #         json=algo_config
    #     )
    #     return response

    # def get_job(self, jobid):
    #     job = DPSJob(self.config)
    #     job.id = jobid
    #     job.retrieve_attributes()
    #     return job

    def upload_files(self, filenames):
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

    # OGC-compliant endpoint functions
    def list_algorithms(self):
        """
        Search all OGC processes
        :return: Response object with all deployed processes
        """
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(self.config.processes_ogc))

        response = requests.get(
            url=self.config.processes_ogc,
            headers=headers
        )
        return response

    def register_algorithm(self, execution_unit_href):
        """
        Deploy a new OGC process
        :param execution_unit_href: URL to the CWL file
        :return: Response object with deployment information
        """
        headers = self._get_api_header(content_type='application/json')
        data = {
            "executionUnit": {
                "href": execution_unit_href
            }
        }
        logger.debug('POST request sent to {}'.format(self.config.processes_ogc))
        response = requests.post(
            url=self.config.processes_ogc,
            headers=headers,
            json=data
        )
        return response

    def get_deployment_status(self, deployment_id):
        """
        Query the current status of an algorithm being deployed
        :param deployment_id: The deployment job ID
        :return: Response object with deployment status
        """
        url = os.path.join(self.config.deployment_jobs_ogc, str(deployment_id))
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def describe_algorithm(self, process_id):
        """
        Get detailed information about a specific OGC process
        :param process_id: The process ID to describe
        :return: Response object with process details
        """
        url = os.path.join(self.config.processes_ogc, str(process_id))
        headers = self._get_api_header()
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def update_algorithm(self, process_id, execution_unit_href):
        """
        Replace an existing OGC process (must be the original deployer)
        :param process_id: The process ID to update
        :param execution_unit_href: URL to the new CWL file
        :return: Response object with update information
        """
        url = os.path.join(self.config.processes_ogc, str(process_id))
        headers = self._get_api_header(content_type='application/json')
        data = {
            "executionUnit": {
                "href": execution_unit_href
            }
        }
        logger.debug('PUT request sent to {}'.format(url))
        response = requests.put(
            url=url,
            headers=headers,
            json=data
        )
        return response

    def delete_algorithm(self, process_id):
        """
        Delete an existing OGC process (must be the original deployer)
        :param process_id: The process ID to delete
        :return: Response object with deletion confirmation
        """
        url = os.path.join(self.config.processes_ogc, str(process_id))
        headers = self._get_api_header()
        logger.debug('DELETE request sent to {}'.format(url))
        response = requests.delete(
            url=url,
            headers=headers
        )
        return response

    def get_algorithm_package(self, process_id):
        """
        Access the formal description that can be used to deploy an OGC process
        :param process_id: The process ID
        :return: Response object with process package description
        """
        url = os.path.join(self.config.processes_ogc, str(process_id), 'package')
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def submit_job(self, process_id, inputs, queue, dedup=None, tag=None):
        """
        Execute an OGC process job
        :param process_id: The process ID to execute
        :param inputs: Dictionary of input parameters for the process
        :param queue: Queue to run the job on
        :param dedup: Optional deduplication flag
        :param tag: Optional user-defined tag for the job
        :return: Response object with job execution information
        """
        url = os.path.join(self.config.processes_ogc, str(process_id), 'execution')
        headers = self._get_api_header(content_type='application/json')
        data = {
            "inputs": inputs,
            "queue": queue
        }
        if dedup is not None:
            data["dedup"] = dedup
        if tag is not None:
            data["tag"] = tag
        
        logger.debug('POST request sent to {}'.format(url))

        response = requests.post(
            url=url,
            headers=headers,
            json=data
        )
        return response

    def get_job_status(self, job_id):
        """
        Get the status of an OGC job
        :param job_id: The job ID to check status for
        :return: Response object with job status
        """
        url = os.path.join(self.config.jobs_ogc, str(job_id))
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))

        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def cancel_job(self, job_id, wait_for_completion=False):
        """
        Cancel a running OGC job or delete a queued job
        :param job_id: The job ID to cancel
        :param wait_for_completion: Whether to wait for the cancellation to complete
        :return: Response object with cancellation status
        """
        url = os.path.join(self.config.jobs_ogc, str(job_id))
        params = {}
        if wait_for_completion:
            params['wait_for_completion'] = str(wait_for_completion).lower()
        
        headers = self._get_api_header()
        logger.debug('DELETE request sent to {}'.format(url))

        response = requests.delete(
            url=url,
            headers=headers,
            params=params
        )
        return response

    def get_job_result(self, job_id):
        """
        Get the results of a completed OGC job
        :param job_id: The job ID to get results for
        :return: Response object with job results
        """
        url = os.path.join(self.config.jobs_ogc, str(job_id), 'results')
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        response = requests.get(
            url=url,
            headers=headers
        )
        return response

    def list_jobs(self, *,
                       process_id=None, 
                       limit=None, 
                       get_job_details=True, 
                       offset=0, 
                       page_size=10, 
                       queue=None,
                       status=None,
                       tag=None, 
                       min_duration=None, 
                       max_duration=None,
                       type=None,
                       datetime=None,
                       priority=None):
        """
        Returns a list of jobs for a given user that matches query params provided.

        Args:
            process_id (id, optional): Algorithm ID to only show jobs submitted for this algorithm
            limit (int, optional): Limit of jobs to send back
            get_job_details (bool, optional): Flag that determines whether to return a detailed job list or a compact list containing just the job ids and their associated job tags. Default is True.
            offset (int, optional): Offset for pagination. Default is 0.
            page_size (int, optional): Page size for pagination. Default is 10.
            queue (str, optional): Job processing resource.
            status (str, optional): Job status, e.g. job-completed, job-failed, job-started, job-queued.
            tag (str, optional): User job tag/identifier.
            min_duration (int, optional): Minimum duration in seconds
            max_duration (int, optional): Maximum duration in seconds
            type (str, optional): Type, available values: process
            datetime (str, optional): Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals are expressed using double-dots.
            priority (int, optional): Job priority, 0-9

        Returns:
            list: List of jobs for a given user that matches query params provided.

        Raises:
            ValueError: If either algo_id or version is provided, but not both.
        """
        params = {
            k: v
            for k, v in (
                ("processID", process_id),
                ("limit", limit),
                ("getJobDetails", get_job_details),
                ("offset", offset),
                ("pageSize", page_size),
                ("queue", queue),
                ("status", status),
                ("tag", tag),
                ("minDuration", min_duration),
                ("maxDuration", max_duration),
                ("type", type),
                ("datetime", datetime),
                ("priority", priority),
            )
            if v is not None
        }

        url = os.path.join(self.config.jobs_ogc)
        headers = self._get_api_header()
        
        logger.debug('GET request sent to {}'.format(url))
        response = requests.get(
            url=url,
            headers=headers,
            params=params
        )
        return response

    def get_job_metrics(self, job_id):
        """
        Get metrics for an OGC job
        :param job_id: The job ID to get metrics for
        :return: Response object with job metrics
        """
        url = os.path.join(self.config.jobs_ogc, str(job_id), 'metrics')
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(url))
        response = requests.get(
            url=url,
            headers=headers
        )
        return response


if __name__ == "__main__":
    print("initialized")