import json
import logging
import os
import xml.etree.ElementTree as ET
import backoff
from urllib.parse import urljoin
from maap.utils import endpoints
from maap.config_reader import MaapConfig
from maap.utils import requests_utils

logger = logging.getLogger(__name__)
BACKOFF_CONF = {}


def _backoff_get_max_time(self):
    return self.__backoff_maxtime


def _backoff_get_max_value(self):
    return self.__max_value


class DPSJob:
    """
    Sample Usage:

    job_id = 'f3780917-92c0-4440-8a84-9b28c2e64fa8'
    job = DPSJob(True)
    job.id = job_id
    print(job.retrieve_status())
    print(job.retrieve_metrics())
    print(job.retrieve_result())
    job.dismiss_job()
    job.delete_job()
    """
    def __init__(self, config: MaapConfig, not_self_signed=True):
        self.config = config
        self.__not_self_signed = not_self_signed
        self.__response_code = None
        self.__error_details = None
        self.__id = None
        self.__status = None
        self.__machine_type = None
        self.__architecture = None
        self.__machine_memory_size = None
        self.__directory_size = None
        self.__operating_system = None
        self.__job_start_time = None
        self.__job_end_time = None
        self.__job_duration_seconds = None
        self.__cpu_usage = None
        self.__cache_usage = None
        self.__mem_usage = None
        self.__max_mem_usage = None
        self.__swap_usage = None
        self.__read_io_stats = None
        self.__write_io_stats = None
        self.__sync_io_stats = None
        self.__async_io_stats = None
        self.__total_io_stats = None
        self.__outputs = []
        self.__traceback = None
        self.__metrics = dict()
        
    def retrieve_status(self):
        # Not using os.path.join just to be safe as this can break if ever run on windows
        # not using urljoin as that requires more preprocessing to avoid dropping api root while joining
        # eg. urljoing("https://api.maap-project.org/api/dps", "id/status") will drop "api/dps" from the output
        url = f"{self.config.dps_job}/{self.id}/{endpoints.DPS_JOB_STATUS}"
        response = requests_utils.make_dps_request(url, self.config)
        self.set_job_status_result(response)
        return self.status

    @backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=172800)
    def wait_for_completion(self):
        self.retrieve_status()
        if self.status.lower() in ["accepted", "running"]:
            logger.debug('Current Status is {}. Backing off.'.format(self.status))
            raise RuntimeError
        return self

    def retrieve_result(self):
        url = f"{self.config.dps_job}/{self.id}"
        response = requests_utils.make_dps_request(url, self.config)
        self.set_job_results_result(response)
        return self.outputs

    def retrieve_metrics(self):
        url = f"{self.config.dps_job}/{self.id}/{endpoints.DPS_JOB_METRICS}"
        response = requests_utils.make_dps_request(url, self.config)
        self.set_job_metrics_result(response)
        return self.metrics

    def retrieve_attributes(self):
        self.retrieve_status()
        if self.status.lower() in ["succeeded", "failed"]:
            try:
                self.retrieve_result()
            except:
                pass
            try:
                # In case job is failed, metrics will throw an error
                self.retrieve_metrics()
            except:
                pass
        return self

    def cancel_job(self):
        url = f"{self.config.dps_job}/{endpoints.DPS_JOB_DISMISS}/{self.id}"
        response = requests_utils.make_dps_request(url, self.config, request_type=requests_utils.POST)
        return response

    def set_submitted_job_result(self, input_json: dict):
        """
        Sample:
        {'status': 'success', 'http_status_code': 200, 'job_id': '50314f32-6099-47fa-8270-c378ac5ff83b'}
        """
        self.status = input_json['status']
        self.id = input_json['job_id']
        self.response_code = input_json['http_status_code']
        if 'details' in input_json:
            self.error_details = input_json['details']
        return self

    def set_job_status_result(self, input_xml_str: str):
        """
        Sample:
        <?xml version="1.0" ?>
        <wps:StatusInfo xmlns:ows="http://www.opengis.net/ows/2.0" xmlns:schemaLocation="http://schemas.opengis.net/wps/2.0/wps.xsd" xmlns:wps="http://www.opengis.net/wps/2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <wps:self.id>50314f32-6099-47fa-8270-c378ac5ff83b</wps:self.id>
            <wps:Status>Succeeded</wps:Status>
        </wps:StatusInfo>
        """
        input_xml = ET.ElementTree(ET.fromstring(input_xml_str))
        for each in input_xml.getroot():
            if each.tag.endswith('self.id'):
                self.id = each.text.strip()
            elif each.tag.endswith('Status'):
                self.status = each.text.strip()
        return self

    def set_job_metrics_result(self, input_xml_str: str):
        """
        Sample:
        <?xml version="1.0" ?>
        <metrics>
            <machine_type>c5.4xlarge</machine_type>
            <architecture/>
            <machine_memory_size>None</machine_memory_size>
            <directory_size>11272048640</directory_size>
            <operating_system/>
            <job_start_time>2020-09-30T15:38:14.958617Z</job_start_time>
            <job_end_time>2020-09-30T15:42:34.019469Z</job_end_time>
            <job_duration_seconds>259.060852</job_duration_seconds>
            <cpu_usage>472452560263</cpu_usage>
            <cache_usage>9911152640</cache_usage>
            <mem_usage>9913106432</mem_usage>
            <max_mem_usage>10723651584</max_mem_usage>
            <swap_usage>0</swap_usage>
            <read_io_stats>0</read_io_stats>
            <write_io_stats>0</write_io_stats>
            <sync_io_stats>0</sync_io_stats>
            <async_io_stats>0</async_io_stats>
            <total_io_stats>0</total_io_stats>
        </metrics>
        """
        input_xml = ET.ElementTree(ET.fromstring(input_xml_str))
        for each in input_xml.getroot():
            name, value = each.tag.strip(), each.text
            self.__metrics.update({name: value})
            if name == 'machine_type':
                self.machine_type = value
            elif name == 'architecture':
                self.architecture = value
            elif name == 'machine_memory_size':
                self.machine_memory_size = value
            elif name == 'directory_size':
                self.directory_size = value
            elif name == 'operating_system':
                self.operating_system = value
            elif name == 'job_start_time':
                self.job_start_time = value
            elif name == 'job_end_time':
                self.job_end_time = value
            elif name == 'job_duration_seconds':
                self.job_duration_seconds = value
            elif name == 'cpu_usage':
                self.cpu_usage = value
            elif name == 'cache_usage':
                self.cache_usage = value
            elif name == 'mem_usage':
                self.mem_usage = value
            elif name == 'max_mem_usage':
                self.max_mem_usage = value
            elif name == 'swap_usage':
                self.swap_usage = value
            elif name == 'read_io_stats':
                self.read_io_stats = value
            elif name == 'write_io_stats':
                self.write_io_stats = value
            elif name == 'sync_io_stats':
                self.sync_io_stats = value
            elif name == 'async_io_stats':
                self.async_io_stats = value
            elif name == 'total_io_stats':
                self.total_io_stats = value
        return self

    def set_job_results_result(self, input_xml_str: str):
        """
        Sample:
        <wps:Result xmlns:ows="http://www.opengis.net/ows/2.0" xmlns:schemaLocation="http://schemas.opengis.net/wps/2.0/wps.xsd" xmlns:wps="http://www.opengis.net/wps/2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><wps:self.id>f3780917-92c0-4440-8a84-9b28c2e64fa8</wps:self.id><wps:Output id="output-2021-05-26T18:39:14.381083"><wps:Data>http://geospec-dataset-bucket-dev.s3-website.amazonaws.com/malarout/dps_output/hytools_ubuntu/v-system-test-5/2021/05/26/18/39/14/381083</wps:Data><wps:Data>s3://s3.amazonaws.com:80/geospec-dataset-bucket-dev/malarout/dps_output/hytools_ubuntu/v-system-test-5/2021/05/26/18/39/14/381083</wps:Data><wps:Data>https://s3.console.aws.amazon.com/s3/buckets/geospec-dataset-bucket-dev/malarout/dps_output/hytools_ubuntu/v-system-test-5/2021/05/26/18/39/14/381083/?region=us-east-1&amp;tab=overview</wps:Data></wps:Output></wps:Result>
        """
        input_xml = ET.ElementTree(ET.fromstring(input_xml_str))
        for each in input_xml.getroot():
            if each.tag.endswith('Output'):
                for eachOutput in each:
                    if eachOutput.tag.endswith('Data'):
                        self.outputs.append(eachOutput.text)
            elif each.tag.endswith('Error'):
                for eachOutput in each:
                    self.traceback.append(eachOutput.text)
        return self

    def __str__(self):
        return str({
            'job_id': self.id,
            'status': self.status,
            'machine_type': self.machine_type,
            'architecture': self.architecture,
            'machine_memory_size': self.machine_memory_size,
            'directory_size': self.directory_size,
            'operating_system': self.operating_system,
            'job_start_time': self.job_start_time,
            'job_end_time': self.job_end_time,
            'job_duration_seconds': self.job_duration_seconds,
            'cpu_usage': self.cpu_usage,
            'cache_usage': self.cache_usage,
            'mem_usage': self.mem_usage,
            'max_mem_usage': self.max_mem_usage,
            'swap_usage': self.swap_usage,
            'read_io_stats': self.read_io_stats,
            'write_io_stats': self.write_io_stats,
            'sync_io_stats': self.sync_io_stats,
            'async_io_stats': self.async_io_stats,
            'total_io_stats': self.total_io_stats,
            'error_details': self.error_details,
            'response_code': self.response_code,
            'outputs': self.outputs
        })

    def __repr__(self):
        return self.__str__()

    @property
    def id(self):
        return self.__id

    @id.setter
    def id(self, val):
        """
        :param val:
        :return: None
        """
        self.__id = val
        return

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, val):
        """
        :param val:
        :return: None
        """
        self.__status = val
        return

    @property
    def machine_type(self):
        return self.__machine_type

    @machine_type.setter
    def machine_type(self, val):
        """
        :param val:
        :return: None
        """
        self.__machine_type = val
        return

    @property
    def architecture(self):
        return self.__architecture

    @architecture.setter
    def architecture(self, val):
        """
        :param val:
        :return: None
        """
        self.__architecture = val
        return

    @property
    def machine_memory_size(self):
        return self.__machine_memory_size

    @machine_memory_size.setter
    def machine_memory_size(self, val):
        """
        :param val:
        :return: None
        """
        self.__machine_memory_size = val
        return

    @property
    def directory_size(self):
        return self.__directory_size

    @directory_size.setter
    def directory_size(self, val):
        """
        :param val:
        :return: None
        """
        self.__directory_size = val
        return

    @property
    def operating_system(self):
        return self.__operating_system

    @operating_system.setter
    def operating_system(self, val):
        """
        :param val:
        :return: None
        """
        self.__operating_system = val
        return

    @property
    def job_start_time(self):
        return self.__job_start_time

    @job_start_time.setter
    def job_start_time(self, val):
        """
        :param val:
        :return: None
        """
        self.__job_start_time = val
        return

    @property
    def job_end_time(self):
        return self.__job_end_time

    @job_end_time.setter
    def job_end_time(self, val):
        """
        :param val:
        :return: None
        """
        self.__job_end_time = val
        return

    @property
    def job_duration_seconds(self):
        return self.__job_duration_seconds

    @job_duration_seconds.setter
    def job_duration_seconds(self, val):
        """
        :param val:
        :return: None
        """
        self.__job_duration_seconds = val
        return

    @property
    def cpu_usage(self):
        return self.__cpu_usage

    @cpu_usage.setter
    def cpu_usage(self, val):
        """
        :param val:
        :return: None
        """
        self.__cpu_usage = val
        return

    @property
    def cache_usage(self):
        return self.__cache_usage

    @cache_usage.setter
    def cache_usage(self, val):
        """
        :param val:
        :return: None
        """
        self.__cache_usage = val
        return

    @property
    def mem_usage(self):
        return self.__mem_usage

    @mem_usage.setter
    def mem_usage(self, val):
        """
        :param val:
        :return: None
        """
        self.__mem_usage = val
        return

    @property
    def max_mem_usage(self):
        return self.__max_mem_usage

    @max_mem_usage.setter
    def max_mem_usage(self, val):
        """
        :param val:
        :return: None
        """
        self.__max_mem_usage = val
        return

    @property
    def swap_usage(self):
        return self.__swap_usage

    @swap_usage.setter
    def swap_usage(self, val):
        """
        :param val:
        :return: None
        """
        self.__swap_usage = val
        return

    @property
    def read_io_stats(self):
        return self.__read_io_stats

    @read_io_stats.setter
    def read_io_stats(self, val):
        """
        :param val:
        :return: None
        """
        self.__read_io_stats = val
        return

    @property
    def write_io_stats(self):
        return self.__write_io_stats

    @write_io_stats.setter
    def write_io_stats(self, val):
        """
        :param val:
        :return: None
        """
        self.__write_io_stats = val
        return

    @property
    def sync_io_stats(self):
        return self.__sync_io_stats

    @sync_io_stats.setter
    def sync_io_stats(self, val):
        """
        :param val:
        :return: None
        """
        self.__sync_io_stats = val
        return

    @property
    def async_io_stats(self):
        return self.__async_io_stats

    @async_io_stats.setter
    def async_io_stats(self, val):
        """
        :param val:
        :return: None
        """
        self.__async_io_stats = val
        return

    @property
    def total_io_stats(self):
        return self.__total_io_stats

    @total_io_stats.setter
    def total_io_stats(self, val):
        """
        :param val:
        :return: None
        """
        self.__total_io_stats = val
        return

    @property
    def outputs(self):
        return self.__outputs

    @outputs.setter
    def outputs(self, val):
        """
        :param val:
        :return: None
        """
        self.__outputs = val
        return

    @property
    def response_code(self):
        return self.__response_code

    @response_code.setter
    def response_code(self, val):
        """
        :param val:
        :return: None
        """
        self.__response_code = val
        return

    @property
    def error_details(self):
        return self.__error_details

    @error_details.setter
    def error_details(self, val):
        """
        :param val:
        :return: None
        """
        self.__error_details = val
        return

    @property
    def metrics(self):
        return self.__metrics
