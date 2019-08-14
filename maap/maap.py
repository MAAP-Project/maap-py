import logging
import os
import requests
import json
import urllib.parse
import time
from mapboxgl.utils import *
from mapboxgl.viz import *
from datetime import datetime

import xml.etree.ElementTree as ET
from .Result import Collection, Granule
from .Dictlist import Dictlist
from .xmlParser import XmlDictConfig
from maap.utils.Presenter import Presenter
from .errors import QueryTimeout, QueryFailure

logger = logging.getLogger(__name__)

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


class MAAP(object):
    def __init__(self):
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
        self._PAGE_SIZE = self.config.getint("request", "page_size")
        self._CONTENT_TYPE = self.config.get("request", "content_type")
        self._API_HEADER = {'Accept': self._CONTENT_TYPE, 'token': self._MAAP_TOKEN}

        self._SEARCH_GRANULE_URL = self.config.get("service", "search_granule_url")
        self._SEARCH_COLLECTION_URL = self.config.get("service", "search_collection_url")
        self._ALGORITHM_REGISTER = self.config.get("service", "algorithm_register")
        self._ALGORITHM_BUILD = self.config.get("service", "algorithm_build")
        self._JOB_STATUS = self.config.get("service", "job_status")
        self._WMTS = self.config.get("service", "wmts")
        self._TILER_ENDPOINT = self.config.get("service", "tiler_endpoint")
        self._MAAP_HOST = self.config.get("service", "maap_host")
        self._QUERY_ENDPOINT =  self.config.get("service", "query_endpoint")

        self._AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID") or self.config.get("aws", "aws_access_key_id")
        self._AWS_ACCESS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY") or self.config.get("aws", "aws_secret_access_key")
        self._MAPBOX_TOKEN = os.environ.get("MAPBOX_ACCESS_TOKEN") or ''
        self._INDEXED_ATTRIBUTES = json.loads(self.config.get("search", "indexed_attributes"))

    def _get_config_path(self, directory):
        return os.path.join(directory, "maap.cfg")

    def _get_search_params(self, **kwargs):
        mapped = self._map_indexed_attributes(**kwargs)
        parsed = self._parse_terms(mapped, '|')

        return parsed

    # Parse delimited terms into value arrays
    def _parse_terms(self, parms, delimiter):
        res = Dictlist()

        for i in parms:
            if delimiter in parms[i]:
                for j in parms[i].split(delimiter):
                    res[i + '[]'] = j
            else:
                res[i] = parms[i]

        return res

    # Conform attribute searches to the 'additional attribute' method:
    # https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#g-additional-attribute
    def _map_indexed_attributes(self, **kwargs):
        p = Dictlist(kwargs)

        for i in self._INDEXED_ATTRIBUTES:
            search_param = i.split(',')[0]

            if search_param in p:
                search_key = i.split(',')[1]
                data_type = i.split(',')[2]

                p['attribute[]'] = data_type + ',' + search_key + ',' + p[search_param]

                del p[search_param]

        return p

    def _get_search_results(self, url, limit, **kwargs):
        """
        Search the CMR granules
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        logger.info("======== Waiting for response ========")

        page_num = 1
        results = []
        while len(results) < limit:
            parms = self._get_search_params(**kwargs)

            response = requests.get(
                url=url,
                params=dict(parms, page_num=page_num, page_size=self._PAGE_SIZE),
                headers=self._API_HEADER
            )
            unparsed_page = response.text[1:-2].replace("\\", "")
            page = ET.XML(unparsed_page)

            empty_page = True
            for child in list(page):
                if child.tag == 'result':
                    results.append(XmlDictConfig(child))
                    empty_page = False
                elif child.tag == 'error':
                    raise ValueError('Bad search response: {}'.format(unparsed_page))

            if empty_page:
                break
            else:
                page_num += 1
        return results

    def searchGranule(self, limit=20, **kwargs):
        """
            Search the CMR granules

            :param limit: limit of the number of results
            :param kwargs: search parameters
            :return: list of results (<Instance of Result>)
            """
        results = self._get_search_results(url=self._SEARCH_GRANULE_URL, limit=limit, **kwargs)
        return [Granule(result, self._AWS_ACCESS_KEY, self._AWS_ACCESS_SECRET) for result in results][:limit]

    def getCallFromEarthdataQuery(self, query, variable_name='maap', limit=1000):
        """
            Generate a literal string to use for calling the MAAP API

            :param query: a Json-formatted string from an Earthdata search-style query. See: https://github.com/MAAP-Project/earthdata-search/blob/master/app/controllers/collections_controller.rb
            :param variable_name: the name of the MAAP variable to qualify the search call
            :param limit: the max records to return
            :return: string in the form of a MAAP API call
            """
        y = json.loads(query)

        params = []

        for key, value in y.items():
            if key.endswith("_h"):
                params.append(key[:-2] + "=\"" + "|".join(value) + "\"")
            elif key == "bounding_box":
                params.append(key + "=\"" + value + "\"")
            elif key == "p":
                params.append("collection_concept_id=\"" + value.replace("!", "|") + "\"")

        params.append("limit=" + str(limit))

        result = variable_name + ".searchGranule(" + ", ".join(params) + ")"

        return result

    def searchCollection(self, limit=100, **kwargs):
        """
        Search the CMR collections
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        results = self._get_search_results(url=self._SEARCH_COLLECTION_URL, limit=limit, **kwargs)
        return [Collection(result, self._MAAP_HOST) for result in results][:limit]

    def registerAlgorithm(self, arg):
        response = requests.post(
            url=self._ALGORITHM_REGISTER,
            json=arg,
            headers=self._API_HEADER
        )
        return response

    def getJobStatus(self, jobid):
        response = requests.get(
            url=self._JOB_STATUS,
            params=dict(job_id=jobid),
            headers=self._API_HEADER
        )
        return response

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
                - where -- a dict-like object mapping fields to required values,
                    used for filtering query
                - bbox -- optional GeoJSON-compliant bounding box (minX, minY,
                    maxX, maxY) by which to filter data (default [], meaning no
                    filter)
                - fields -- optional list of fields to return in query response
                    (default [], returning all fields)
        poll_results -- system will poll for results and return results response
            if True, otherwise will return response from Query Service (default
            True)
        timeout -- max number of seconds to wait for response, only used if
            results=True (default 180)
        wait_interval -- number of seconds to wait between each poll for
            results, only used if results=True (default .5)
        max_redirectss -- max number of redirects to follow when scheduling
            execution (default 5)
        """
        url = self._QUERY_ENDPOINT
        redirect_count = 0
        while True:
            response = requests.post(
                url=url,
                headers=dict(Accept='application/json'),
                json=dict(src=src, query=query),
                allow_redirects=False
            )

            # By default, requests follows POST redirects with GET request.
            # Instead, we'll make the POST again to the new URL.
            redirect_url = response.headers.get('Location', url)
            if (redirect_url is not url and response.is_redirect and redirect_count < max_redirects):
                logger.debug(f'Received redirect at {url}. Retrying query at {redirect_url}')
                url = redirect_url
                redirect_count += 1
            else:
                break

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
            r = requests.get(url=results)

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

