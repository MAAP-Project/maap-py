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
    granules = maap.searchGranule(
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

        >>> granules = maap.searchGranule(
        ...     short_name='GEDI02_A',
        ...     bounding_box='-122.5,37.5,-121.5,38.5',
        ...     limit=5
        ... )
        >>> for g in granules:
        ...     print(g.getDescription())

    Submit a job::

        >>> job = maap.submitJob(
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
        This is an internal method primarily used by :meth:`uploadFiles`.
        It uses the boto3 S3 client configured at module level.
        """
        return s3_client.upload_file(filename, bucket, objectKey)

    def searchGranule(self, limit=20, **kwargs):
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

            >>> granules = maap.searchGranule(
            ...     short_name='GEDI02_A',
            ...     limit=10
            ... )

        Search with spatial bounds::

            >>> granules = maap.searchGranule(
            ...     collection_concept_id='C1234567890-MAAP',
            ...     bounding_box='-122.5,37.5,-121.5,38.5',
            ...     limit=5
            ... )

        Search with temporal filter::

            >>> granules = maap.searchGranule(
            ...     short_name='AFLVIS2',
            ...     temporal='2019-01-01T00:00:00Z,2019-12-31T23:59:59Z',
            ...     limit=100
            ... )

        Search with pattern matching::

            >>> granules = maap.searchGranule(
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
        :meth:`searchCollection` : Search for collections
        :class:`~maap.Result.Granule` : Granule result class
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

            >>> local_file = maap.downloadGranule(
            ...     'https://data.maap-project.org/file/data.h5',
            ...     destination_path='/tmp/downloads'
            ... )
            >>> print(f"Downloaded to: {local_file}")

        Force overwrite of existing files::

            >>> local_file = maap.downloadGranule(
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
        :meth:`searchGranule` : Search for granules
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

    def getCallFromEarthdataQuery(self, query, variable_name='maap', limit=1000):
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
            >>> code = maap.getCallFromEarthdataQuery(query)
            >>> print(code)
            maap.searchGranule(instrument="GEDI", bounding_box="-180,-90,180,90", limit=1000)

        Notes
        -----
        This is useful for converting queries from the Earthdata Search
        web interface to MAAP API calls. The generated string can be
        executed using ``eval()`` or used as a reference.

        See Also
        --------
        :meth:`getCallFromCmrUri` : Generate call from CMR URI
        :meth:`searchGranule` : Execute a granule search
        """
        return self._CMR.generateGranuleCallFromEarthDataRequest(query, variable_name, limit)

    def getCallFromCmrUri(self, search_url, variable_name='maap', limit=1000, search='granule'):
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
            >>> code = maap.getCallFromCmrUri(url)
            >>> print(code)
            maap.searchGranule(short_name="GEDI02_A", limit=1000)

        Convert a collection search::

            >>> url = 'https://cmr.earthdata.nasa.gov/search/collections?provider=MAAP'
            >>> code = maap.getCallFromCmrUri(url, search='collection')
            >>> print(code)
            maap.searchCollection(provider="MAAP", limit=1000)

        Notes
        -----
        This is useful for converting existing CMR queries to MAAP API calls.
        Duplicate query parameters are automatically converted to pipe-delimited
        values.

        See Also
        --------
        :meth:`getCallFromEarthdataQuery` : Generate call from Earthdata query
        :meth:`searchGranule` : Execute a granule search
        :meth:`searchCollection` : Execute a collection search
        """
        return self._CMR.generateCallFromEarthDataQueryString(search_url, variable_name, limit, search)

    def searchCollection(self, limit=100, **kwargs):
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

            >>> collections = maap.searchCollection(short_name='GEDI02_A')
            >>> for c in collections:
            ...     print(c['Collection']['ShortName'])

        Search by provider::

            >>> collections = maap.searchCollection(
            ...     provider='MAAP',
            ...     limit=50
            ... )

        Search by keyword::

            >>> collections = maap.searchCollection(
            ...     keyword='biomass forest',
            ...     limit=20
            ... )

        Notes
        -----
        Collections contain metadata about datasets but not the actual data
        files. Use :meth:`searchGranule` to find individual data files within
        a collection.

        See Also
        --------
        :meth:`searchGranule` : Search for granules within collections
        :class:`~maap.Result.Collection` : Collection result class
        """
        results = self._CMR.get_search_results(url=self.config.search_collection_url, limit=limit, **kwargs)
        return [Collection(result, self.config.maap_host) for result in results][:limit]

    def getQueues(self):
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

            >>> response = maap.getQueues()
            >>> queues = response.json()
            >>> for queue in queues:
            ...     print(f"{queue['name']}: {queue['memory']} RAM")

        Notes
        -----
        Common queue names follow the pattern ``maap-dps-worker-{size}``
        where size indicates memory (e.g., ``8gb``, ``16gb``, ``32gb``).

        See Also
        --------
        :meth:`submitJob` : Submit a job to a queue
        :meth:`registerAlgorithm` : Register an algorithm to run on queues
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

    def registerAlgorithm(self, arg):
        """
        Register an algorithm with the MAAP DPS.

        Registers a new algorithm configuration that can be executed on the
        MAAP Data Processing System (DPS).

        Parameters
        ----------
        arg : dict or str
            Algorithm configuration as a dictionary or JSON string. Required
            fields include:

            algorithm_name : str
                Unique name for the algorithm.
            code_version : str
                Version identifier (e.g., Git branch or tag).
            algorithm_description : str
                Human-readable description.
            docker_container_url : str
                URL of the Docker container image.
            script_command : str
                Command to execute inside the container.
            inputs : list of dict
                Input parameter definitions with ``field`` and ``download`` keys. Format should be like 
                {'file': [{'name': 'input_file'}],'config': [{'name': 'config_param'}],'positional': [{'name': 'pos_arg'}]}
            repo_url : str
                Git repository URL for the algorithm source code.

        Returns
        -------
        requests.Response
            HTTP response indicating success or failure of registration.

        Examples
        --------
        Register using a dictionary::

            >>> config = {
            ...     'algorithm_name': 'my_algorithm',
            ...     'code_version': 'main',
            ...     'algorithm_description': 'Processes satellite data',
            ...     'docker_container_url': 'registry/image:tag',
            ...     'script_command': 'python run.py',
            ...     'inputs': {
            ...         'file': [{'name': 'input_file'}],
            ...         'config': [{'name': 'config_param'}],
            ...         'positional': [{'name': 'pos_arg'}]
            ...     },
            ...     'repo_url': 'https://github.com/org/repo'
            ... }
            >>> response = maap.registerAlgorithm(config)

        Register using a JSON string::

            >>> import json
            >>> response = maap.registerAlgorithm(json.dumps(config))

        Notes
        -----
        After registration, algorithms need to be built before they can be
        executed. The build process creates the Docker image on the DPS
        infrastructure.

        See Also
        --------
        :meth:`register_algorithm_from_yaml_file` : Register from YAML file
        :meth:`listAlgorithms` : List registered algorithms
        :meth:`deleteAlgorithm` : Delete an algorithm
        """
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
        """
        Register an algorithm from a YAML configuration file.

        Reads algorithm configuration from a YAML file and registers it with
        the MAAP DPS.

        Parameters
        ----------
        file_path : str
            Path to the YAML configuration file.

        Returns
        -------
        requests.Response
            HTTP response indicating success or failure of registration.

        Examples
        --------
        Register from a YAML file::

            >>> response = maap.register_algorithm_from_yaml_file('algorithm.yaml')

        Example YAML file structure::

            algorithm_name: my_algorithm
            code_version: main
            algorithm_description: Process satellite data
            docker_container_url: registry/image:tag
            script_command: python run.py
            inputs:
              file:
                - name: input_file
              config:
                - name: config_param
              positional:
                - name: pos_arg
            repo_url: https://github.com/org/repo

        See Also
        --------
        :meth:`registerAlgorithm` : Register from dict or JSON
        :meth:`register_algorithm_from_yaml_file_backwards_compatible` : Legacy format
        """
        algo_config = algorithm_utils.read_yaml_file(file_path)
        return self.registerAlgorithm(algo_config)

    def register_algorithm_from_yaml_file_backwards_compatible(self, file_path):
        """
        Register an algorithm from a legacy YAML configuration file.

        Reads algorithm configuration from an older YAML format and converts
        it to the current format before registration.

        Parameters
        ----------
        file_path : str
            Path to the legacy YAML configuration file.

        Returns
        -------
        requests.Response
            HTTP response indicating success or failure of registration.

        Notes
        -----
        This method supports the legacy YAML format with different field names:

        - ``algo_name`` -> ``algorithm_name``
        - ``version`` -> ``code_version``
        - ``environment`` -> ``environment_name``
        - ``description`` -> ``algorithm_description``
        - ``docker_url`` -> ``docker_container_url``
        - ``inputs`` -> ``algorithm_params``
        - ``run_command`` -> ``script_command``
        - ``repository_url`` -> ``repo_url``

        See Also
        --------
        :meth:`register_algorithm_from_yaml_file` : Current format
        :meth:`registerAlgorithm` : Register from dict
        """
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
        """
        List all registered algorithms.

        Retrieves a list of all algorithms registered by the current user
        on the MAAP DPS.

        Returns
        -------
        requests.Response
            HTTP response containing JSON list of algorithms. Each algorithm
            entry includes name, version, description, and status information.

        Examples
        --------
        List all algorithms::

            >>> response = maap.listAlgorithms()
            >>> algorithms = response.json()
            >>> for algo in algorithms:
            ...     print(f"{algo['algorithm_name']}:{algo['code_version']}")

        See Also
        --------
        :meth:`describeAlgorithm` : Get details for specific algorithm
        :meth:`registerAlgorithm` : Register a new algorithm
        :meth:`deleteAlgorithm` : Delete an algorithm
        """
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
        """
        Get detailed information about a registered algorithm.

        Retrieves the full configuration and status of a specific algorithm.

        Parameters
        ----------
        algoid : str
            The algorithm identifier, typically in the format
            ``algorithm_name:code_version``.

        Returns
        -------
        requests.Response
            HTTP response containing JSON with algorithm details including
            configuration, build status, and parameter definitions.

        Examples
        --------
        Get algorithm details::

            >>> response = maap.describeAlgorithm('my_algorithm:main')
            >>> details = response.json()
            >>> print(f"Description: {details['algorithm_description']}")
            >>> print(f"Docker: {details['docker_container_url']}")

        See Also
        --------
        :meth:`listAlgorithms` : List all algorithms
        :meth:`publishAlgorithm` : Publish an algorithm
        """
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
        """
        Publish an algorithm for public use.

        Makes a registered algorithm available for other MAAP users to
        discover and execute.

        Parameters
        ----------
        algoid : str
            The algorithm identifier to publish, typically in the format
            ``algorithm_name:code_version``.

        Returns
        -------
        requests.Response
            HTTP response indicating success or failure of publication.

        Examples
        --------
        Publish an algorithm::

            >>> response = maap.publishAlgorithm('my_algorithm:v1.0')
            >>> if response.ok:
            ...     print("Algorithm published successfully")

        Notes
        -----
        Published algorithms are visible to all MAAP users and can be
        executed by anyone with DPS access.

        See Also
        --------
        :meth:`registerAlgorithm` : Register an algorithm
        :meth:`deleteAlgorithm` : Delete an algorithm
        """
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
        """
        Delete a registered algorithm.

        Removes an algorithm registration from the MAAP DPS. This does not
        affect any completed jobs that used the algorithm.

        Parameters
        ----------
        algoid : str
            The algorithm identifier to delete, typically in the format
            ``algorithm_name:code_version``.

        Returns
        -------
        requests.Response
            HTTP response indicating success or failure of deletion.

        Examples
        --------
        Delete an algorithm::

            >>> response = maap.deleteAlgorithm('my_algorithm:main')
            >>> if response.ok:
            ...     print("Algorithm deleted")

        Warnings
        --------
        This action cannot be undone. The algorithm configuration will be
        permanently removed.

        See Also
        --------
        :meth:`registerAlgorithm` : Register an algorithm
        :meth:`listAlgorithms` : List algorithms
        """
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
        """
        Get a DPS job with all available attributes.

        Retrieves a job object with its current status, results (if available),
        and metrics (if available).

        Parameters
        ----------
        jobid : str
            The unique job identifier (UUID).

        Returns
        -------
        DPSJob
            A :class:`~maap.dps.dps_job.DPSJob` object with populated attributes
            including status, outputs, and metrics.

        Examples
        --------
        Get a job and inspect its status::

            >>> job = maap.getJob('f3780917-92c0-4440-8a84-9b28c2e64fa8')
            >>> print(f"Status: {job.status}")
            >>> print(f"Outputs: {job.outputs}")
            >>> print(f"Duration: {job.job_duration_seconds} seconds")

        See Also
        --------
        :meth:`getJobStatus` : Get status only
        :meth:`getJobResult` : Get results only
        :meth:`getJobMetrics` : Get metrics only
        :meth:`submitJob` : Submit a new job
        """
        job = DPSJob(self.config)
        job.id = jobid
        job.retrieve_attributes()
        return job

    def getJobStatus(self, jobid):
        """
        Get the current status of a DPS job.

        Parameters
        ----------
        jobid : str
            The unique job identifier (UUID).

        Returns
        -------
        str
            The job status. Possible values are:

            - ``'Accepted'``: Job is queued
            - ``'Running'``: Job is executing
            - ``'Succeeded'``: Job completed successfully
            - ``'Failed'``: Job failed
            - ``'Dismissed'``: Job was cancelled

        Examples
        --------
        Check job status::

            >>> status = maap.getJobStatus('f3780917-92c0-4440-8a84-9b28c2e64fa8')
            >>> print(f"Job status: {status}")

        See Also
        --------
        :meth:`getJob` : Get full job object
        :meth:`cancelJob` : Cancel a running job
        """
        job = DPSJob(self.config)
        job.id = jobid
        return job.retrieve_status()

    def getJobResult(self, jobid):
        """
        Get the output URLs from a completed DPS job.

        Parameters
        ----------
        jobid : str
            The unique job identifier (UUID).

        Returns
        -------
        list of str
            List of URLs pointing to job output files. Typically includes
            HTTP, S3, and console URLs for the output directory.

        Examples
        --------
        Get job outputs::

            >>> outputs = maap.getJobResult('f3780917-92c0-4440-8a84-9b28c2e64fa8')
            >>> for url in outputs:
            ...     print(url)

        Notes
        -----
        This method only returns results for jobs that have completed
        (succeeded or failed). For running jobs, the output list will be empty.

        See Also
        --------
        :meth:`getJob` : Get full job object
        :meth:`getJobMetrics` : Get job performance metrics
        """
        job = DPSJob(self.config)
        job.id = jobid
        return job.retrieve_result()

    def getJobMetrics(self, jobid):
        """
        Get performance metrics from a completed DPS job.

        Retrieves resource usage and timing information for a job.

        Parameters
        ----------
        jobid : str
            The unique job identifier (UUID).

        Returns
        -------
        dict
            Dictionary containing job metrics including:

            - ``machine_type``: EC2 instance type used
            - ``job_start_time``: ISO timestamp of job start
            - ``job_end_time``: ISO timestamp of job end
            - ``job_duration_seconds``: Total execution time
            - ``cpu_usage``: CPU time in nanoseconds
            - ``mem_usage``: Memory usage in bytes
            - ``max_mem_usage``: Peak memory usage in bytes
            - ``directory_size``: Output directory size in bytes

        Examples
        --------
        Get job metrics::

            >>> metrics = maap.getJobMetrics('f3780917-92c0-4440-8a84-9b28c2e64fa8')
            >>> print(f"Duration: {metrics['job_duration_seconds']} seconds")
            >>> print(f"Max memory: {metrics['max_mem_usage']} bytes")

        See Also
        --------
        :meth:`getJob` : Get full job object
        :meth:`getJobResult` : Get job outputs
        """
        job = DPSJob(self.config)
        job.id = jobid
        return job.retrieve_metrics()

    def cancelJob(self, jobid):
        """
        Cancel a running or queued DPS job.

        Attempts to stop execution of a job that is currently running or
        waiting in the queue.

        Parameters
        ----------
        jobid : str
            The unique job identifier (UUID) to cancel.

        Returns
        -------
        str
            Response from the DPS indicating the cancellation result.

        Examples
        --------
        Cancel a job::

            >>> result = maap.cancelJob('f3780917-92c0-4440-8a84-9b28c2e64fa8')
            >>> print(result)

        Notes
        -----
        Jobs that are already completed (Succeeded or Failed) cannot be
        cancelled. The job status will be set to ``'Dismissed'`` upon
        successful cancellation.

        See Also
        --------
        :meth:`submitJob` : Submit a job
        :meth:`getJobStatus` : Check job status
        """
        job = DPSJob(self.config)
        job.id = jobid
        return job.cancel_job()

    def listJobs(self, *,
                       algo_id=None,
                       end_time=None,
                       get_job_details=True,
                       offset=0,
                       page_size=10,
                       queue=None,
                       start_time=None,
                       status=None,
                       tag=None,
                       version=None):
        """
        List jobs submitted by the current user.

        Retrieves a paginated list of DPS jobs matching the specified filter
        criteria.

        Parameters
        ----------
        algo_id : str, optional
            Filter by algorithm name. Must be provided together with ``version``.
        end_time : str, optional
            Filter for jobs completed before this time. Format: ISO 8601
            (e.g., ``'2024-01-01'`` or ``'2024-01-01T00:00:00.000000Z'``).
        get_job_details : bool, optional
            If ``True`` (default), return detailed job information. If ``False``,
            return only job IDs and tags for faster response.
        offset : int, optional
            Number of jobs to skip for pagination. Default is 0.
        page_size : int, optional
            Number of jobs to return per page. Default is 10.
        queue : str, optional
            Filter by processing queue name.
        start_time : str, optional
            Filter for jobs started after this time. Format: ISO 8601.
        status : str, optional
            Filter by job status. Valid values:

            - ``'Accepted'``: Queued jobs
            - ``'Running'``: Currently executing
            - ``'Succeeded'``: Completed successfully
            - ``'Failed'``: Completed with errors
            - ``'Dismissed'``: Cancelled jobs

        tag : str, optional
            Filter by user-defined job tag/identifier.
        version : str, optional
            Filter by algorithm version. Must be provided together with ``algo_id``.

        Returns
        -------
        requests.Response
            HTTP response containing JSON list of jobs matching the criteria.

        Raises
        ------
        ValueError
            If only one of ``algo_id`` or ``version`` is provided. Both must
            be provided together or neither should be provided.

        Examples
        --------
        List recent jobs::

            >>> response = maap.listJobs(page_size=20)
            >>> jobs = response.json()
            >>> for job in jobs:
            ...     print(f"{job['job_id']}: {job['status']}")

        Filter by algorithm and version::

            >>> response = maap.listJobs(
            ...     algo_id='my_algorithm',
            ...     version='main',
            ...     status='Succeeded'
            ... )

        Paginate through results::

            >>> response = maap.listJobs(offset=0, page_size=10)
            >>> # Get next page
            >>> response = maap.listJobs(offset=10, page_size=10)

        Filter by time range::

            >>> response = maap.listJobs(
            ...     start_time='2024-01-01',
            ...     end_time='2024-01-31'
            ... )

        See Also
        --------
        :meth:`getJob` : Get details of a specific job
        :meth:`submitJob` : Submit a new job
        """
        url = "/".join(
            segment.strip("/")
            for segment in (self.config.dps_job, endpoints.DPS_JOB_LIST)
        )
        
        params = {
            k: v
            for k, v in (
                ("algo_id", algo_id),
                ("end_time", end_time),
                ("get_job_details", get_job_details),
                ("offset", offset),
                ("page_size", page_size),
                ("queue", queue),
                ("start_time", start_time),
                ("status", status),
                ("tag", tag),
                ("version", version),
            )
            if v is not None
        }
        
        if (not algo_id) != (not version):
            # Either algo_id or version was supplied as a non-empty string, but not both.
            # Either both must be non-empty strings or both must be None.
            raise ValueError("Either supply non-empty strings for both algo_id and version, or supply neither.")

        # DPS requests use 'job_type', which is a concatenation of 'algo_id' and 'version'
        if algo_id and version:
            params['job_type'] = f"{algo_id}:{version}"

        algo_id = params.pop('algo_id', None)
        version = params.pop('version', None)

        if status is not None:
            params['status'] = job.validate_job_status(status)

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
        """
        Submit a job to the MAAP Data Processing System (DPS).

        Submits an algorithm for execution on the DPS infrastructure with the
        specified parameters and compute resources.

        Parameters
        ----------
        identifier : str
            A user-defined tag or identifier for the job. Used for tracking
            and organizing jobs.
        algo_id : str
            The algorithm name to execute.
        version : str
            The algorithm version (e.g., Git branch or tag).
        queue : str
            The compute queue/resource to use (e.g., ``'maap-dps-worker-8gb'``).
            Use :meth:`getQueues` to list available queues.
        retrieve_attributes : bool, optional
            If ``True``, immediately retrieve job status after submission.
            Default is ``False``.
        **kwargs : dict
            Algorithm input parameters. Parameter names must match those
            defined in the algorithm registration.

        Returns
        -------
        DPSJob
            A :class:`~maap.dps.dps_job.DPSJob` object representing the
            submitted job. Use the job's methods to monitor status and
            retrieve results.

        Examples
        --------
        Submit a basic job::

            >>> job = maap.submitJob(
            ...     identifier='my_analysis_run',
            ...     algo_id='my_algorithm',
            ...     version='main',
            ...     queue='maap-dps-worker-8gb',
            ...     input_file='s3://bucket/input.tif'
            ... )
            >>> print(f"Job ID: {job.id}")

        Submit with multiple parameters::

            >>> job = maap.submitJob(
            ...     identifier='batch_processing',
            ...     algo_id='processor',
            ...     version='v2.0',
            ...     queue='maap-dps-worker-32gb',
            ...     input_granule='s3://bucket/data.h5',
            ...     output_format='geotiff',
            ...     resolution=30
            ... )

        Submit and immediately get status::

            >>> job = maap.submitJob(
            ...     identifier='urgent_job',
            ...     algo_id='my_algorithm',
            ...     version='main',
            ...     queue='maap-dps-worker-8gb',
            ...     retrieve_attributes=True
            ... )
            >>> print(f"Status: {job.status}")

        Monitor job completion::

            >>> job = maap.submitJob(...)
            >>> job.wait_for_completion()
            >>> print(f"Final status: {job.status}")
            >>> print(f"Outputs: {job.outputs}")

        Notes
        -----
        - The job executes asynchronously; this method returns immediately
          after submission.
        - Use :meth:`~maap.dps.dps_job.DPSJob.wait_for_completion` to block
          until the job finishes.
        - Input parameters with ``download=True`` in the algorithm config
          will be downloaded to the job's working directory.

        See Also
        --------
        :meth:`getJob` : Retrieve job information
        :meth:`listJobs` : List submitted jobs
        :meth:`cancelJob` : Cancel a running job
        :meth:`getQueues` : List available queues
        :class:`~maap.dps.dps_job.DPSJob` : Job management class
        """
        # Note that this is temporary and will be removed when we remove the API not requiring username to submit a job
        # Also this now overrides passing someone else's username into submitJob since we don't want to allow that
        if self.profile is not None and self.profile.account_info() is not None and 'username' in self.profile.account_info().keys():
            kwargs['username'] = self.profile.account_info()['username']
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

            >>> result = maap.uploadFiles(['data.csv', 'config.json'])
            >>> print(result)
            Upload file subdirectory: a1b2c3d4-e5f6-... (keep a record of...)

        Upload a single file::

            >>> result = maap.uploadFiles(['output.tif'])

        Notes
        -----
        - Files are uploaded to a unique subdirectory identified by a UUID
        - Save the UUID to share the upload location with collaborators
        - The upload location can be used as input to DPS jobs

        See Also
        --------
        :meth:`submitJob` : Use uploaded files as job inputs
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
            :meth:`searchGranule`. Must contain ``Granule.GranuleUR``.
        display_config : dict, optional
            Configuration options for rendering. Common options include:

            rescale : str
                Value range for color scaling (e.g., ``'0,70'``).
            color_map : str
                Color palette name (e.g., ``'schwarzwald'``).

        Examples
        --------
        Display a granule on a map::

            >>> granules = maap.searchGranule(short_name='AFLVIS2', limit=1)
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
        :meth:`searchGranule` : Search for granules to visualize
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


if __name__ == "__main__":
    print("initialized")