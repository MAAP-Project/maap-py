import os
from maap.config_reader import MaapConfig
import logging
import requests
from enum import Enum

logger = logging.getLogger(__name__)


# TODO: Replace with http.HTTPMethod once we upgrade to Python>3.11
class HTTPMethod(Enum):
    POST = 'POST'
    GET = 'GET'
    PUT = 'PUT'
    DELETE = 'DELETE'


POST = HTTPMethod.POST
GET = HTTPMethod.GET
PUT = HTTPMethod.PUT
DELETE = HTTPMethod.DELETE


def generate_dps_headers(config: MaapConfig, content_type=None):
    api_header = {
        'Accept': config.content_type,
    }
    if content_type:
        api_header['Content-Type'] = content_type
    if config.maap_token.lower().startswith('basic') or config.maap_token.lower().startswith('bearer'):
        api_header['Authorization'] = config.maap_token
    else:
        api_header['token'] = config.maap_token

    if os.environ.get("MAAP_PGT"):
        api_header['proxy-ticket'] = os.environ.get("MAAP_PGT")
    return api_header


def check_response(dps_response):
    # if dps_response.status_code not in [200, 201]:
    #     raise RuntimeError('response is not 200 or 201. code: {}. details: {}'.format(dps_response.status_code,
    #                                                                                   dps_response.content))
    return dps_response.content.decode('UTF-8')


# TODO: Explore consolidating all requests from maap-py into this class
def make_request(url, config: MaapConfig, content_type=None, request_type: HTTPMethod = HTTPMethod.GET,
                 self_signed=False, **kwargs):
    headers = generate_dps_headers(config, content_type)
    logger.debug(f"{request_type} request sent to {url}")
    logger.debug('headers:')
    logger.debug(headers)
    if request_type not in {POST, GET}:
        # TODO: Add support for request type DELETE
        raise NotImplementedError(f"Request type {request_type} not supported")
    else:
        return requests.request(
            method=request_type.value,
            url=url,
            verify=not self_signed,
            headers=headers,
            ** kwargs
        )


def make_dps_request(url, config: MaapConfig, content_type=None, request_type: HTTPMethod = HTTPMethod.GET,
                     self_signed=False, **kwargs):
    return check_response(make_request(url, config, content_type, request_type, self_signed, **kwargs))
