import datetime
import requests
import xml.etree.ElementTree as ET
import logging
import json
from os.path import exists

import importlib_resources as resources


class DpsHelper:
    DPS_INTERNAL_FILE_JOB = "_job.json"
    DPS_INTERNAL_FILE_DPS_TOKEN = "_maap_dps_token.txt"

    """
    Functions used for DPS API interfacing
    """
    def __init__(self, api_header, dps_token_endpoint):
        self._api_header = api_header
        self._logger = logging.getLogger(__name__)
        self.dps_token_endpoint = dps_token_endpoint
        self.running_in_dps = self._running_in_dps_mode()

        if self.running_in_dps:
            self.dps_machine_token = self._file_contents(self.DPS_INTERNAL_FILE_DPS_TOKEN)
            job_data = json.loads(self._file_contents(self.DPS_INTERNAL_FILE_JOB))
            self.job_id = job_data['job_info']['job_payload']['payload_task_id']

    def _skit(self, lines, kwargs):
        res = {}
        for k in kwargs:
            if k not in lines:
                res[k] = kwargs[k]

        return res

    def submit_job(self, request_url, **kwargs):
        xml_file = resources.files("maap.dps").joinpath("execute.xml")
        input_xml = resources.files("maap.dps").joinpath("execute_inputs.xml")

        # ==================================
        # Part 1: Parse Required Arguments
        # ==================================
        # make sure consistent with submit_jobs/src/fields.json
        fields = ["algo_id", "version", "inputs"]
        input_names = self._skit(fields, kwargs)
        if not 'username' in kwargs:
            input_names['username'] = 'username'

        params = {}
        for f in fields:
            try:
                params[f] = kwargs[f]
            except:
                params[f] = ''

        inputs = {}
        for f in input_names:
            try:
                inputs[f] = kwargs[f]
            except:
                inputs[f] = ''

        logging.debug('fields are')
        logging.debug(fields)

        logging.debug('params are')
        logging.debug(params)

        logging.debug('inputs are')
        logging.debug(inputs)

        params['timestamp'] = str(datetime.datetime.today())
        if 'username' in params.keys() and inputs['username'] == '':
            inputs['username'] = 'anonymous'

        # ==================================
        # Part 2: Build & Send Request
        # ==================================
        req_xml = ''
        ins_xml = ''

        other = ''
        ins_xml = input_xml.read_text()

        # -------------------------------
        # Insert XML for algorithm inputs
        # -------------------------------
        for key in input_names:
            other += ins_xml.format(name=key, value=input_names[key])
            other += '\n'

        # print(other)
        params['other_inputs'] = other

        req_xml = xml_file.read_text().format(**params)

        # log request body
        logging.debug('request is')
        logging.debug(req_xml)

        # log request headers
        logging.debug('headers:')
        logging.debug(self._api_header)

        # -------------------------------
        # Send Request
        # -------------------------------
        try:
            r = requests.post(
                url=request_url,
                data=req_xml,
                headers=self._api_header
            )
            logging.debug('status code {}'.format(r.status_code))
            logging.debug('response text\n{}'.format(r.text))

            # ==================================
            # Part 3: Check & Parse Response
            # ==================================
            # malformed request will still give 200
            if r.status_code == 200:
                try:
                    # parse out JobID from response
                    rt = ET.fromstring(r.text)

                    # if bad request, show provided parameters
                    if 'Exception' in r.text:
                        result = 'Exception: {}\n'.format(rt[0].attrib['exceptionCode'])
                        result += 'Bad Request\nThe provided parameters were:\n'
                        for f in fields:
                            result += '\t{}: {}\n'.format(f, params[f])
                        result += '\n'
                        return {"status_code": 400, "result": result}

                    else:
                        job_id = rt[0].text

                        if job_id is not None:
                            return {"status": "success", "http_status_code": r.status_code, "job_id": job_id}
                except:
                    return {"status": "failed", "http_status_code": r.status_code, "job_id": "", "details": r.text}
            else:
                return {"status": "failed", "http_status_code": r.status_code, "job_id": "", "details": r.text}
        except:
            return {"status": "failed", "http_status_code": r.status_code, "job_id": "", "details": r.text}

    def _file_contents(self, file_name):
        with open(file_name, 'r') as file:
            return file.read().replace('\n', '')

    def _running_in_dps_mode(self):
        return exists(self.DPS_INTERNAL_FILE_JOB) and exists(self.DPS_INTERNAL_FILE_DPS_TOKEN)
