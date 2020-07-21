from maap.Dictlist import Dictlist
import requests
import xml.etree.ElementTree as ET
from maap.xmlParser import XmlDictConfig
import logging


class CMR:
    """
    Functions used for CMR API interfacing
    """
    def __init__(self, indexed_attributes, page_size, api_header):
        self._indexed_attributes = indexed_attributes
        self._page_size = page_size
        self._api_header = api_header
        self._logger = logging.getLogger(__name__)

    def get_search_results(self, url, limit, **kwargs):
        """
        Search the CMR granules
        :param url: request url
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        self._logger.info("======== Waiting for response ========")

        page_num = 1
        results = []
        while len(results) < limit:
            parms = self._get_search_params(**kwargs)

            response = requests.get(
                url=url,
                params=dict(parms, page_num=page_num, page_size=self._page_size),
                headers=self._api_header
            )
            unparsed_page = self._prepare_cmr_response(response)
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

    def _prepare_cmr_response(self, response):

        cmr_output = response.text[1:-2].replace("\\", "")

        if cmr_output.startswith('CMR Error <'):
            cmr_output = cmr_output.replace('CMR Error <', '<')

        return cmr_output

    def _get_search_params(self, **kwargs):
        mapped = self._map_indexed_attributes(**kwargs)
        parsed = self._parse_terms(mapped, '|')

        return parsed

    # Conform attribute searches to the 'additional attribute' method:
    # https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html#g-additional-attribute
    def _map_indexed_attributes(self, **kwargs):
        p = Dictlist(kwargs)

        for i in self._indexed_attributes:
            search_param = i.split(',')[0]

            if search_param in p:
                search_key = i.split(',')[1]
                data_type = i.split(',')[2]

                p['attribute[]'] = data_type + ',' + search_key + ',' + p[search_param]

                del p[search_param]

        return p

    # Parse delimited terms into value arrays
    def _parse_terms(self, parms, delimiter):
        res = Dictlist()

        for i in parms:
            if delimiter in parms[i]:
                for j in parms[i].split(delimiter):
                    res[i + '[]'] = j
            elif '*' in parms[i] or '?' in parms[i]:
                res['options[' + i + '][pattern]'] = 'true'
                res[i] = parms[i]
            else:
                res[i] = parms[i]

        return res
