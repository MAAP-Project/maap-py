import os

from maap.config_reader import ConfigReader


class RequestsUtils:
    @staticmethod
    def generate_dps_headers(content_type=None):
        config = ConfigReader()
        api_header = {
            'Accept': content_type if content_type else config.content_type,
        }
        if config.maap_token.lower().startswith('basic') or config.maap_token.lower().startswith('bearer'):
            api_header['Authorization'] = config.maap_token
        else:
            api_header['token'] = config.maap_token

        if os.environ.get("MAAP_PGT"):
            api_header['proxy-ticket'] = os.environ.get("MAAP_PGT")
        return api_header

    @staticmethod
    def check_response(dps_response):
        if dps_response.status_code not in [200, 201]:
            raise RuntimeError('response is not 200 or 201. code: {}. details: {}'.format(dps_response.status_code,
                                                                                          dps_response.content))
        return dps_response.content.decode('UTF-8')
