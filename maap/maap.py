import logging
import boto3
import uuid
import urllib.parse
import time
import os
from mapboxgl.utils import *
from mapboxgl.viz import *
from datetime import datetime

from maap.config_reader import ConfigReader
from maap.dps_job import DPSJobProps
from .Result import Collection, Granule
from maap.utils.Presenter import Presenter
from maap.utils.CMR import CMR
from maap.Profile import Profile
from maap.dps.DpsHelper import DpsHelper
from maap.utils import endpoints
from .errors import QueryTimeout, QueryFailure

logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')


class MAAP(object):
    def __init__(self, self_signed=False):
        self.__self_signed = self_signed
        self.__config = ConfigReader()

        self._MAAP_TOKEN = self.__config.maap_token
        self._PROXY_GRANTING_TICKET = os.environ.get("MAAP_PGT") or ''

        self._ALGORITHM_REGISTER = self.__config.algorithm_register
        self._MAS_ALGO = self.__config.mas_algo
        self._DPS_JOB = self.__config.dps_job
        self._WMTS = self.__config.wmts

        self._AWS_ACCESS_KEY = self.__config.aws_access_key
        self._AWS_ACCESS_SECRET = self.__config.aws_access_secret
        self._S3_USER_UPLOAD_BUCKET = self.__config.s3_user_upload_bucket
        self._S3_USER_UPLOAD_DIR = self.__config.s3_user_upload_dir
        self._MAPBOX_TOKEN = os.environ.get("MAPBOX_ACCESS_TOKEN") or ''

        self._CMR = CMR(self.__config.indexed_attributes, self.__config.page_size, self._get_api_header())
        self._DPS = DpsHelper(self._get_api_header())
        self.profile = Profile(self.__config.member, self._get_api_header())
        self.__job_props = DPSJobProps()

    def _get_api_header(self, content_type=None):

        api_header = {
            'Accept': content_type if content_type else self.__config.content_type,
        }
        if self._MAAP_TOKEN.lower().startswith('basic') or self._MAAP_TOKEN.lower().startswith('bearer'):
            api_header['Authorization'] = self._MAAP_TOKEN
        else:
            api_header['token'] = self._MAAP_TOKEN

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
        results = self._CMR.get_search_results(url=self.__config.search_granule_url, limit=limit, **kwargs)
        return [Granule(result, self._AWS_ACCESS_KEY, self._AWS_ACCESS_SECRET) for result in results][:limit]

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
        results = self._CMR.get_search_results(url=self.__config.search_collection_url, limit=limit, **kwargs)
        return [Collection(result, self.__config.maap_host) for result in results][:limit]

    def getQueues(self):
        url = os.path.join(self._ALGORITHM_REGISTER, 'resource')
        headers = self._get_api_header()
        logger.debug('GET request sent to {}'.format(self._ALGORITHM_REGISTER))
        logger.debug('headers:')
        logger.debug(headers)
        response = requests.get(
            url=url,
            verify=self.__self_signed,
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
            verify=self.__self_signed,
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
            verify=self.__self_signed,
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
            verify=self.__self_signed,
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
            verify=self.__self_signed,
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
            verify=self.__self_signed,
            headers=headers
        )
        return response

    @staticmethod
    def __check_response(dps_response):
        if dps_response.status_code not in [200, 201]:
            raise RuntimeError('response is not 200 or 201. code: {}. details: {}'.format(dps_response.status_code,
                                                                                          dps_response.content))
        return dps_response.content.decode('UTF-8')

    def getJobStatus(self, jobid):
        url = os.path.join(self._DPS_JOB, jobid, endpoints.DPS_JOB_STATUS)
        headers = self._get_api_header()
        logging.debug('GET request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.get(
            url=url,
            verify=self.__self_signed,
            headers=headers
        )
        self.__job_props.set_job_status_result(self.__check_response(response))
        return self.__job_props

    def getJobResult(self, jobid):
        url = os.path.join(self._DPS_JOB, jobid)
        headers = self._get_api_header()
        logging.debug('GET request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.get(
            url=url,
            verify=self.__self_signed,
            headers=headers
        )
        self.__job_props.set_job_results_result(self.__check_response(response))
        return self.__job_props

    def getJobMetrics(self, jobid):
        url = os.path.join(self._DPS_JOB, jobid, endpoints.DPS_JOB_METRICS)
        headers = self._get_api_header()
        logging.debug('GET request sent to {}'.format(url))
        logging.debug('headers:')
        logging.debug(headers)
        response = requests.get(
            url=url,
            verify=self.__self_signed,
            headers=headers
        )
        self.__job_props.set_job_metrics_result(self.__check_response(response))
        return self.__job_props

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
            verify=self.__self_signed,
            headers=headers
        )
        return response

    def submitJob(self, **kwargs):
        response = self._DPS.submit_job(request_url=self._DPS_JOB, **kwargs)
        self.__job_props.set_submitted_job_result(self.__check_response(response))
        return self.__job_props

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

    def executeQuery(self, src, query={}, poll_results=True, timeout=180, wait_interval=.5, max_redirects=5):
        """
        Helper to execute query and poll results URL until results are returned
        or timeout is reached.

        src -- a dict-like object stipulating which dataset is to be queried.
            Object must contain 'Collection' key. 'Collection' value must
            contain 'ShortName' and 'VersionId' entries. Granule-related value
            must contain a 'Collection' entry, complying with aforementioned
            'Collection' object requirements.
        query -- dict-like object describing parameters for query (default {}).
            Currently supported parameters:
                - where -- optional dict-like object mapping fields to required
                    values, used for filtering query by properties
                - bbox -- optional GeoJSON-compliant bounding box ([minX, minY,
                    maxX, maxY]) by which to spatially filter data
                - fields -- optional list of fields to return in query response
        poll_results -- system will poll for results and return results response
            if True, otherwise will return response from Query Service (default
            True)
        timeout -- maximum number of seconds to wait for response, only used if
            poll_results=True (default 180)
        wait_interval -- number of seconds to wait between each poll for
            results, only used if poll_results=True (default 0.5)
        max_redirects -- maximum number of redirects to follow when scheduling
            an execution (default 5)
        """
        url = self.__config.query_endpoint
        redirect_count = 0
        while True:
            response = requests.post(
                url=url,
                headers=dict(Accept='application/json'),
                json=dict(src=src, query=query),
                allow_redirects=False
            )

            if not response.is_redirect:
                break

            # By default, requests follows POST redirects with GET request.
            # Instead, we'll make the POST again to the new URL.
            redirect_url = response.headers.get('Location', url)
            if redirect_url is url:
                break

            redirect_count += 1
            if redirect_count >= max_redirects:
                break

            logger.debug(f'Received redirect at {url}. Retrying query at {redirect_url}')
            url = redirect_url

        if not poll_results:
            # Return the response of query execution
            return response

        response.raise_for_status()
        if (response.is_redirect):
            raise requests.HTTPError(
                'Received redirect as query execution response '
                'Is your the "query_endpoint" configuration correct?'
                f'\n{response.status_code}: {response.text}'
            )
        execution = response.json()
        results = execution['results']

        # Poll results
        start = datetime.now()
        while (datetime.now() - start).seconds < timeout:
            r = requests.get(verify=self.__self_signed, url=results)

            if r.status_code == 200:
                # Return the response of query results
                if r.headers.get('x-amz-meta-failed'):
                    raise QueryFailure(
                        f'The backing query service failed to process query:\n{r.text}'
                    )
                return r

            if r.status_code == 404:
                continue

            r.raise_for_status()
            time.sleep(wait_interval)

        raise QueryTimeout('Query results did not appear within {} seconds'.format(timeout))

    def _get_browse(self, granule_ur):
        response = requests.get(
            url='{}/GetTile'.format(self._WMTS),
            params=dict(granule_ur=granule_ur),
            verify=self.__self_signed,
            headers=dict(Accept='application/json')
        )
        return response

    def _get_capabilities(self, granule_ur):
        response = requests.get(
            url='{}/GetCapabilities'.format(self._WMTS),
            params=dict(granule_ur=granule_ur),
            verify=self.__self_signed,
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
        tiles_url = f"{self.__config.tiler_endpoint}/tiles/{{z}}/{{x}}/{{y}}.png?{qs}"
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

