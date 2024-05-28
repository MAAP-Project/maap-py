import logging
from datetime import datetime
import xml.etree.ElementTree as ET

import importlib_resources as resources
import requests

from maap.config_reader import MaapConfig
from maap.dps.dps_job import DPSJob
from maap.utils.requests_utils import RequestsUtils

logger = logging.getLogger(__name__)


class DPSJobModel:
    """
    How to submit a job:

    model = DPSJobModel(not_self_signed=False)
    model.algorithm_id = 'hytools_ubuntu'
    model.algorithm_version = 'v-system-test-5'
    job = model.with_param('reflectance_granules', reflectance_granules)\
        .with_param('obs_granules', obs_granules)\
        .with_param('trait_models_repo_url', trait_models_repo_url)\
        .with_param('image_correct_config', image_correct_config)\
        .with_param('trait_estimate_config', trait_estimate_config)\
        .with_param('publish_to_cmr', False)\
        .with_param('cmr_metadata', {})\
        .submit_job()
    print(job)


    """
    def __init__(self, not_self_signed=False):
        self.__not_self_signed = not_self_signed
        self.__algorithm_id = None
        self.__algorithm_version = None
        self.__username = 'anonymous'
        self.__inputs = {}
        self.__xml_file = resources.files("maap.dps").joinpath("execute.xml")
        self.__input_xml = resources.files("maap.dps").joinpath("execute_inputs.xml")

    def with_param(self, key, val):
        self.__inputs[key] = val
        return self

    def __generate_xml_inputs(self):
        input_xml = self.__input_xml.read_text()
        input_xmls = [input_xml.format(name=k, value=v) for k, v in self.__inputs.items()]
        return '\n'.join(input_xmls)

    def generate_request_xml(self):
        if 'username' not in self.__inputs:
            self.__inputs['username'] = self.username
        params = {
            'algo_id': self.algorithm_id,
            'version': self.algorithm_version,
            'timestamp': str(datetime.today()),
            'inputs': '',  # TODO this is needed?
            'other_inputs': self.__generate_xml_inputs(),
        }
        return self.__xml_file.read_text().format(**params)

    def submit_job(self):
        """
        Successful Sample XML:
        <?xml version="1.0" encoding="UTF-8"?>
        <wps:StatusInfo xmlns:ows="http://www.opengis.net/ows/2.0" xmlns:schemaLocation="http://schemas.opengis.net/wps/2.0/wps.xsd" xmlns:wps="http://www.opengis.net/wps/2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
          <wps:JobID>31c9f95e-af4a-4a1c-bd10-f44b031811a9</wps:JobID>
          <wps:Status>Failed</wps:Status>
        </wps:StatusInfo>

        """
        validations = []
        if self.algorithm_id is None:
            validations.append('algorithm_id')
        if self.algorithm_version is None:
            validations.append('algorithm_version')
        if len(validations) > 0:
            raise IOError(f'one or more missing values: {validations}')
        request_xml = self.generate_request_xml()
        logger.debug(f'request_xml: {request_xml}')
        response = requests.post(
            url=MaapConfig().dps_job,
            data=request_xml,
            verify=self.__not_self_signed,
            headers=RequestsUtils.generate_dps_headers(),
        )
        try:
            response_str = RequestsUtils.check_response(response)
        except Exception as e:
            return DPSJob(self.__not_self_signed).set_submitted_job_result({"status": "failed",
                                                                            "http_status_code": response.status_code,
                                                                            "job_id": "", 'details': str(e)})

        if 'Exception' in response_str:
            err_result = [f'Exception XML: {response_str}', 'Bad Request', 'The provided parameters were:',
                          str({'algo_id': self.algorithm_id, 'version': self.algorithm_version})]
            return DPSJob(self.__not_self_signed).set_submitted_job_result({'status_code': 400,
                                                                            'details': '\n'.join(err_result)})
        try:
            response_xml = ET.ElementTree(ET.fromstring(response_str))
        except:
            return DPSJob(self.__not_self_signed).set_submitted_job_result({"status": "failed",
                                                                            "http_status_code": response.status_code,
                                                                            "job_id": "",
                                                                            'details': f'unable to parse XML. raw: {response_str}'})

        for each in response_xml.getroot():
            if each.tag.endswith('JobID'):
                job_id = each.text.strip()
                return DPSJob(self.__not_self_signed).set_submitted_job_result({"status": "success",
                                                                                "http_status_code": response.status_code,
                                                                                "job_id": job_id})
        return DPSJob(self.__not_self_signed).set_submitted_job_result({"status": "success",
                                                                        "http_status_code": response.status_code,
                                                                        "job_id": 'unknown'})

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, val):
        """
        :param val:
        :return: None
        """
        self.__username = val
        return

    @property
    def algorithm_id(self):
        return self.__algorithm_id

    @algorithm_id.setter
    def algorithm_id(self, val):
        """
        :param val:
        :return: None
        """
        self.__algorithm_id = val
        return

    @property
    def algorithm_version(self):
        return self.__algorithm_version

    @algorithm_version.setter
    def algorithm_version(self, val):
        """
        :param val:
        :return: None
        """
        self.__algorithm_version = val
        return
