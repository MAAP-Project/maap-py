"""
MAAP Python Client
==================

This module provides the main entry point for interacting with the NASA MAAP
(Multi-Mission Algorithm and Analysis Platform) API.

The :class:`MAAP` class is the primary interface for all MAAP operations including:

* Searching for granules and collections in CMR (Common Metadata Repository)
* Registering, managing, and executing algorithms on the Data Processing System (DPS)
* Managing user secrets and AWS credentials
* Uploading and downloading files
* Visualizing geospatial data

Example
-------
Basic usage::

    from maap.maap import MAAP

    # Initialize the MAAP client
    maap = MAAP()

    # Search for granules
    granules = maap.search_granule(
        short_name='GEDI02_A',
        limit=10
    )

    # Download a granule
    for granule in granules:
        local_path = granule.getData()

Note
----
The MAAP client automatically reads configuration from the MAAP API endpoint.
Authentication is handled via environment variables or the MAAP platform.

See Also
--------
:class:`maap.Result.Granule` : Class representing a CMR granule
:class:`maap.Result.Collection` : Class representing a CMR collection
:class:`maap.dps.dps_job.DPSJob` : Class representing a DPS job
"""

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
    """
    Main client class for interacting with the MAAP API.

    The MAAP class provides a unified interface for all MAAP platform operations,
    including data discovery, algorithm management, job submission, and file operations.

    Parameters
    ----------
    maap_host : str, optional
        The hostname of the MAAP API server. Defaults to the value of the
        ``MAAP_API_HOST`` environment variable, or ``'api.maap-project.org'``
        if not set.

    Attributes
    ----------
    config : MaapConfig
        Configuration object containing API endpoints and settings.
    profile : Profile
        Interface for user profile operations.
    aws : AWS
        Interface for AWS credential operations.
    secrets : Secrets
        Interface for user secrets management.

    Examples
    --------
    Initialize with default settings::

        >>> from maap.maap import MAAP
        >>> maap = MAAP()

    Initialize with a custom host::

        >>> maap = MAAP(maap_host='api.ops.maap-project.org')

    Search for granules::

        >>> granules = maap.search_granule(
        ...     short_name='GEDI02_A',
        ...     bounding_box='-122.5,37.5,-121.5,38.5',
        ...     limit=5
        ... )
        >>> for g in granules:
        ...     print(g.getDescription())

    Submit a job::

        >>> job = maap.submit_job(
        ...     identifier='my_analysis',
        ...     algo_id='my_algorithm',
        ...     version='main',
        ...     queue='maap-dps-worker-8gb',
        ...     input_file='s3://bucket/input.tif'
        ... )
        >>> print(f"Job submitted: {job.id}")

    Notes
    -----
    The MAAP client requires proper authentication to access most features.
    Authentication is typically handled automatically when running within
    the MAAP Algorithm Development Environment (ADE).

    Environment Variables
    ---------------------
    MAAP_API_HOST : str
        Override the default MAAP API host.
    MAAP_PGT : str
        Proxy Granting Ticket for authentication.
    MAAP_AWS_ACCESS_KEY_ID : str
        AWS access key for S3 operations.
    MAAP_AWS_SECRET_ACCESS_KEY : str
        AWS secret key for S3 operations.

    See Also
    --------
    :class:`maap.Profile.Profile` : User profile management
    :class:`maap.AWS.AWS` : AWS credential management
    :class:`maap.Secrets.Secrets` : User secrets management
    """

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
        """
        Generate HTTP headers for API requests.

        Constructs the authorization and content-type headers required for
        making authenticated requests to the MAAP API.

        Parameters
        ----------
        content_type : str, optional
            The content type for the request. If not specified, uses the
            default content type from configuration.

        Returns
        -------
        dict
            Dictionary containing HTTP headers including:
            - ``Accept``: The expected response content type
            - ``Content-Type``: The request content type
            - ``token``: The MAAP authentication token
            - ``proxy-ticket``: The proxy granting ticket (if available)

        Notes
        -----
        This is an internal method used by other MAAP methods to construct
        proper API request headers. The proxy ticket is automatically included
        if the ``MAAP_PGT`` environment variable is set.
        """
        api_header = {'Accept': content_type if content_type else self.config.content_type, 'token': self.config.maap_token, 'Content-Type': content_type if content_type else self.config.content_type}

        if os.environ.get("MAAP_PGT"):
            api_header['proxy-ticket'] = os.environ.get("MAAP_PGT")

        return api_header

    def _upload_s3(self, filename, bucket, objectKey):
        """
        Upload a file to Amazon S3.

        Internal utility method for uploading files to S3 storage.

        Parameters
        ----------
        filename : str
            Local path to the file to upload.
        bucket : str
            Name of the S3 bucket to upload to.
        objectKey : str
            The S3 object key (path) where the file will be stored.

        Returns
        -------
        dict
            S3 upload response containing upload metadata.

        Notes
        -----
        This is an internal method primarily used by :meth:`upload_files`.
        It uses the boto3 S3 client configured at module level.
        """
        return s3_client.upload_file(filename, bucket, objectKey)

    def search_granule(self, limit=20, **kwargs):
        """
        Search for granules in the CMR (Common Metadata Repository).

        Queries the CMR database for granules matching the specified criteria.
        Granules represent individual data files within a collection.

        Parameters
        ----------
        limit : int, optional
            Maximum number of results to return. Default is 20.
        **kwargs : dict
            Search parameters to filter results. Common parameters include:

            short_name : str
                Collection short name (e.g., 'GEDI02_A').
            collection_concept_id : str
                Unique CMR collection identifier.
            bounding_box : str
                Spatial filter as 'west,south,east,north' coordinates.
            temporal : str
                Temporal filter as 'start_date,end_date' in ISO format.
            polygon : str
                Polygon coordinates for spatial filtering.
            readable_granule_name : str
                Filter by granule name pattern. Supports wildcards.
            instrument : str
                Filter by instrument name (e.g., 'uavsar').
            platform : str
                Filter by platform name (e.g., 'GEDI').
            site_name : str
                Filter by site name for MAAP-indexed datasets.

        Returns
        -------
        list of Granule
            List of :class:`~maap.Result.Granule` objects matching the search
            criteria. Each granule provides methods to access download URLs
            and retrieve data.

        Examples
        --------
        Search by collection name::

            >>> granules = maap.search_granule(
            ...     short_name='GEDI02_A',
            ...     limit=10
            ... )

        Search with spatial bounds::

            >>> granules = maap.search_granule(
            ...     collection_concept_id='C1234567890-MAAP',
            ...     bounding_box='-122.5,37.5,-121.5,38.5',
            ...     limit=5
            ... )

        Search with temporal filter::

            >>> granules = maap.search_granule(
            ...     short_name='AFLVIS2',
            ...     temporal='2019-01-01T00:00:00Z,2019-12-31T23:59:59Z',
            ...     limit=100
            ... )

        Search with pattern matching::

            >>> granules = maap.search_granule(
            ...     readable_granule_name='*2019*',
            ...     short_name='GEDI02_A'
            ... )

        Download results::

            >>> for granule in granules:
            ...     print(granule.getDescription())
            ...     local_path = granule.getData(destpath='/tmp')

        Notes
        -----
        - Multiple search parameters can be combined with pipe (``|``) delimiter.
        - Wildcard characters (``*``, ``?``) are supported for pattern matching.
        - Results are automatically paginated internally.

        See Also
        --------
        :meth:`search_collection` : Search for collections
        :class:`~maap.Result.Granule` : Granule result class
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
        Download a granule directly from an HTTP URL.

        Downloads data from an Earthdata HTTP URL, handling both public and
        protected (authenticated) resources automatically.

        Parameters
        ----------
        online_access_url : str
            The HTTP URL of the granule to download. This is typically obtained
            from a granule's ``OnlineAccessURL`` field.
        destination_path : str, optional
            Directory path where the file will be saved. Default is the current
            working directory (``'.'``).
        overwrite : bool, optional
            If ``True``, overwrite existing files. If ``False`` (default), skip
            download if the file already exists.

        Returns
        -------
        str
            The local file path of the downloaded file.

        Examples
        --------
        Download a granule by URL::

            >>> local_file = maap.download_granule(
            ...     'https://data.maap-project.org/file/data.h5',
            ...     destination_path='/tmp/downloads'
            ... )
            >>> print(f"Downloaded to: {local_file}")

        Force overwrite of existing files::

            >>> local_file = maap.download_granule(
            ...     url,
            ...     destination_path='/tmp',
            ...     overwrite=True
            ... )

        Notes
        -----
        This method handles authentication automatically:

        - First attempts an unauthenticated request
        - Falls back to EDL (Earthdata Login) federated authentication if needed
        - Uses DPS machine tokens when running inside a DPS job

        For most use cases, prefer using :meth:`Granule.getData()` instead,
        which handles URL selection automatically.

        See Also
        --------
        :meth:`search_granule` : Search for granules
        :meth:`~maap.Result.Granule.getData` : Download granule data
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
        Generate a MAAP API call string from an Earthdata search query.

        Converts a JSON-formatted Earthdata search query into a Python code
        string that can be used to call the MAAP API.

        Parameters
        ----------
        query : str
            A JSON-formatted string representing an Earthdata search query.
            This is the format used by the Earthdata Search application.
        variable_name : str, optional
            The variable name to use in the generated code for the MAAP
            client instance. Default is ``'maap'``.
        limit : int, optional
            Maximum number of records to return. Default is 1000.

        Returns
        -------
        str
            A Python code string that can be executed to perform the
            equivalent MAAP API search.

        Examples
        --------
        Convert an Earthdata query::

            >>> query = '{"instrument_h": ["GEDI"], "bounding_box": "-180,-90,180,90"}'
            >>> code = maap.get_call_from_earthdata_query(query)
            >>> print(code)
            maap.search_granule(instrument="GEDI", bounding_box="-180,-90,180,90", limit=1000)

        Notes
        -----
        This is useful for converting queries from the Earthdata Search
        web interface to MAAP API calls. The generated string can be
        executed using ``eval()`` or used as a reference.

        See Also
        --------
        :meth:`get_call_from_cmr_uri` : Generate call from CMR URI
        :meth:`search_granule` : Execute a granule search
        """
        return self._CMR.generateGranuleCallFromEarthDataRequest(query, variable_name, limit)

    def get_call_from_cmr_uri(self, search_url, variable_name='maap', limit=1000, search='granule'):
        """
        Generate a MAAP API call string from a CMR REST API URL.

        Converts a CMR REST API query URL into a Python code string that can
        be used to call the MAAP API.

        Parameters
        ----------
        search_url : str
            A CMR REST API search URL. This can be copied directly from
            the CMR API or browser address bar.
        variable_name : str, optional
            The variable name to use in the generated code for the MAAP
            client instance. Default is ``'maap'``.
        limit : int, optional
            Maximum number of records to return. Default is 1000.
        search : str, optional
            Type of search to perform. Either ``'granule'`` (default) or
            ``'collection'``.

        Returns
        -------
        str
            A Python code string that can be executed to perform the
            equivalent MAAP API search.

        Examples
        --------
        Convert a CMR granule search URL::

            >>> url = 'https://cmr.earthdata.nasa.gov/search/granules?short_name=GEDI02_A'
            >>> code = maap.get_call_from_cmr_uri(url)
            >>> print(code)
            maap.search_granule(short_name="GEDI02_A", limit=1000)

        Convert a collection search::

            >>> url = 'https://cmr.earthdata.nasa.gov/search/collections?provider=MAAP'
            >>> code = maap.get_call_from_cmr_uri(url, search='collection')
            >>> print(code)
            maap.search_collection(provider="MAAP", limit=1000)

        Notes
        -----
        This is useful for converting existing CMR queries to MAAP API calls.
        Duplicate query parameters are automatically converted to pipe-delimited
        values.

        See Also
        --------
        :meth:`get_call_from_earthdata_query` : Generate call from Earthdata query
        :meth:`search_granule` : Execute a granule search
        :meth:`search_collection` : Execute a collection search
        """
        return self._CMR.generateCallFromEarthDataQueryString(search_url, variable_name, limit, search)

    def search_collection(self, limit=100, **kwargs):
        """
        Search for collections in the CMR (Common Metadata Repository).

        Queries the CMR database for collections (datasets) matching the
        specified criteria. Collections represent groups of related data files.

        Parameters
        ----------
        limit : int, optional
            Maximum number of results to return. Default is 100.
        **kwargs : dict
            Search parameters to filter results. Common parameters include:

            short_name : str
                Collection short name (e.g., 'GEDI02_A').
            concept_id : str
                Unique CMR collection identifier.
            provider : str
                Data provider (e.g., 'MAAP', 'LPDAAC_ECS').
            keyword : str
                Keyword search across collection metadata.
            instrument : str
                Filter by instrument name.
            platform : str
                Filter by platform name.
            project : str
                Filter by project name.
            processing_level_id : str
                Filter by data processing level.

        Returns
        -------
        list of Collection
            List of :class:`~maap.Result.Collection` objects matching the
            search criteria.

        Examples
        --------
        Search by short name::

            >>> collections = maap.search_collection(short_name='GEDI02_A')
            >>> for c in collections:
            ...     print(c['Collection']['ShortName'])

        Search by provider::

            >>> collections = maap.search_collection(
            ...     provider='MAAP',
            ...     limit=50
            ... )

        Search by keyword::

            >>> collections = maap.search_collection(
            ...     keyword='biomass forest',
            ...     limit=20
            ... )

        Notes
        -----
        Collections contain metadata about datasets but not the actual data
        files. Use :meth:`search_granule` to find individual data files within
        a collection.

        See Also
        --------
        :meth:`search_granule` : Search for granules within collections
        :class:`~maap.Result.Collection` : Collection result class
        """
        results = self._CMR.get_search_results(url=self.config.search_collection_url, limit=limit, **kwargs)
        return [Collection(result, self.config.maap_host) for result in results][:limit]

    def get_queues(self):
        """
        Get available DPS processing queues (resources).

        Retrieves a list of available compute resources (queues) that can be
        used for algorithm execution. Different queues provide different
        amounts of memory and CPU.

        Returns
        -------
        requests.Response
            HTTP response containing JSON list of available queues. Each queue
            entry includes resource specifications like memory and CPU limits.

        Examples
        --------
        List available queues::

            >>> response = maap.get_queues()
            >>> queues = response.json()
            >>> for queue in queues:
            ...     print(f"{queue['name']}: {queue['memory']} RAM")

        Notes
        -----
        Common queue names follow the pattern ``maap-dps-worker-{size}``
        where size indicates memory (e.g., ``8gb``, ``16gb``, ``32gb``).

        See Also
        --------
        :meth:`submit_job` : Submit a job to a queue
        :meth:`register_algorithm` : Register an algorithm to run on queues
        """
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

    def register_algorithm_from_cwl_file(self, file_path):
        """
        Registers an algorithm from a CWL file
        """
        # Read cwl file returns a dict in the format to register an algorithm without a CWL
        process_config = algorithm_utils.read_cwl_file(file_path)
        print("graceal1 returned JSON of metadata extracted from CWL:")
        print(process_config)
        headers = self._get_api_header(content_type='application/json')
        logger.debug('POST request sent to {}'.format(self.config.processes_ogc))
        response = requests.post(
            url=self.config.processes_ogc,
            headers=headers,
            json=process_config
        )
        return response

    def upload_files(self, filenames):
        """
        Upload files to MAAP shared storage.

        Uploads local files to an S3 staging directory where they can be
        accessed by other MAAP users or used as inputs to DPS jobs.

        Parameters
        ----------
        filenames : list of str
            List of local file paths to upload.

        Returns
        -------
        str
            A message containing the UUID of the upload directory. This UUID
            is needed to share the files with other users.

        Examples
        --------
        Upload files to share::

            >>> result = maap.upload_files(['data.csv', 'config.json'])
            >>> print(result)
            Upload file subdirectory: a1b2c3d4-e5f6-... (keep a record of...)

        Upload a single file::

            >>> result = maap.upload_files(['output.tif'])

        Notes
        -----
        - Files are uploaded to a unique subdirectory identified by a UUID
        - Save the UUID to share the upload location with collaborators
        - The upload location can be used as input to DPS jobs

        See Also
        --------
        :meth:`submit_job` : Use uploaded files as job inputs
        """
        bucket = self.config.s3_user_upload_bucket
        prefix = self.config.s3_user_upload_dir
        uuid_dir = uuid.uuid4()
        for filename in filenames:
            basename = os.path.basename(filename)
            response = self._upload_s3(filename, bucket, f"{prefix}/{uuid_dir}/{basename}")
        return f"Upload file subdirectory: {uuid_dir} (keep a record of this if you want to share these files with other users)"

    def _get_browse(self, granule_ur):
        """
        Get browse image metadata for a granule.

        Internal method to retrieve browse image information for visualization.

        Parameters
        ----------
        granule_ur : str
            The Granule Universal Reference identifier.

        Returns
        -------
        requests.Response
            HTTP response containing browse image metadata.
        """
        response = requests.get(
            url=f'{self.config.wmts}/GetTile',
            params=dict(granule_ur=granule_ur),
            headers=dict(Accept='application/json')
        )
        return response

    def _get_capabilities(self, granule_ur):
        """
        Get WMTS capabilities for a granule.

        Internal method to retrieve Web Map Tile Service capabilities
        for visualization.

        Parameters
        ----------
        granule_ur : str
            The Granule Universal Reference identifier.

        Returns
        -------
        requests.Response
            HTTP response containing WMTS capabilities XML.
        """
        response = requests.get(
            url=f'{self.config.wmts}/GetCapabilities',
            params=dict(granule_ur=granule_ur),
            headers=dict(Accept='application/json')
        )
        return response

    def show(self, granule, display_config={}):
        """
        Display a granule on an interactive map.

        Renders the granule data as a tile layer on an interactive Mapbox
        map in a Jupyter notebook environment.

        Parameters
        ----------
        granule : dict
            A granule result dictionary, typically obtained from
            :meth:`search_granule`. Must contain ``Granule.GranuleUR``.
        display_config : dict, optional
            Configuration options for rendering. Common options include:

            rescale : str
                Value range for color scaling (e.g., ``'0,70'``).
            color_map : str
                Color palette name (e.g., ``'schwarzwald'``).

        Examples
        --------
        Display a granule on a map::

            >>> granules = maap.search_granule(short_name='AFLVIS2', limit=1)
            >>> maap.show(granules[0])

        Display with custom rendering::

            >>> maap.show(granule, display_config={
            ...     'rescale': '0,100',
            ...     'color_map': 'viridis'
            ... })

        Notes
        -----
        - Requires ``mapboxgl`` package and a Jupyter notebook environment
        - Uses the MAAP tile server for rendering
        - A Mapbox access token must be configured

        See Also
        --------
        :meth:`search_granule` : Search for granules to visualize
        """
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