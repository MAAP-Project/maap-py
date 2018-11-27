import logging
import os
import requests

import xml.etree.ElementTree as ET
from .Result import Collection, Granule
from .xmlParser import XmlDictConfig

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


class MAAP(object):
    def __init__(self, configFilePath=''):
        """
        :param configFilePath: The config file containing the credentials to make CRUD requests to CMR (extention .cfg)
        These con
        """
        print(configFilePath)
        self.config = ConfigParser()
        if os.path.isfile(configFilePath) and os.access(configFilePath, os.R_OK ):
            # Open the config file as normal
            self.config.read(configFilePath)
            self.configFilePath = configFilePath
        else:
            raise IOError("The config file can't be opened for reading")

        self._PAGE_SIZE = self.config.getint("request", "page_size")
        self._SEARCH_GRANULE_URL = self.config.get("request", "search_granule_url")
        self._SEARCH_COLLECTION_URL = self.config.get("request", "search_collection_url")

        self._CONTENT_TYPE = self.config.get("request", "content_type")
        self._SEARCH_HEADER = {'Accept': self._CONTENT_TYPE}
        self._MAAP_HOST = self.config.get("request", "maap_host")
        self._AWS_ACCESS_KEY = self.config.get("request", "aws_access_key_id")
        self._AWS_ACCESS_SECRET = self.config.get("request", "aws_secret_access_key")

    def _get_search_params(self, **kwargs):
        p = dict(kwargs)

        if 'sitename' in p:
            p['attribute[]'] = 'string,Site Name,' + p['sitename']
            del p['sitename']

        return p


    def _get_search_results(self, url, limit, **kwargs):
        """
        Search the CMR granules
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        logging.info("======== Waiting for response ========")

        page_num = 1
        results = []
        while len(results) < limit:
            parms = self._get_search_params(**kwargs)

            response = requests.get(
                url=url,
                params=dict(parms, page_num=page_num, page_size=self._PAGE_SIZE),
                headers=self._SEARCH_HEADER
            )
            unparsed_page = response.text[1:-2].replace("\\", "")
            #unparsed_page = response.content
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

    def searchCollection(self, limit=100, **kwargs):
        """
        Search the CMR collections
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        results = self._get_search_results(url=self._SEARCH_COLLECTION_URL, limit=limit, **kwargs)
        return [Collection(result, self._MAAP_HOST) for result in results][:limit]


if __name__ == "__main__":
    m = MAAP("../maap.cfg")
    print("initialized")
    results = m.searchGranule(sitename='lope', instrument='uavsar')
    for res in results:
        print(res.getDownloadUrl())
        res.download()
    # Make sure that the XML response was actually parsed
    valid = isinstance(results[0], Collection)
