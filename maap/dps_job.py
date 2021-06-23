import json
import logging
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


class DPSJobProps:
    def __init__(self):
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

    def set_submitted_job_result(self, input_json_str: str):
        """
        Sample:
        {'status': 'success', 'http_status_code': 200, 'job_id': '50314f32-6099-47fa-8270-c378ac5ff83b'}
        """
        input_json = json.loads(input_json_str)
        self.status = input_json['status']
        self.id = input_json['job_id']
        return self

    def set_job_status_result(self, input_xml_str: str):
        """
        Sample:
        <?xml version="1.0" ?>
        <wps:StatusInfo xmlns:ows="http://www.opengis.net/ows/2.0" xmlns:schemaLocation="http://schemas.opengis.net/wps/2.0/wps.xsd" xmlns:wps="http://www.opengis.net/wps/2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <wps:JobID>50314f32-6099-47fa-8270-c378ac5ff83b</wps:JobID>
            <wps:Status>Succeeded</wps:Status>
        </wps:StatusInfo>
        """
        input_xml = ET.ElementTree(ET.fromstring(input_xml_str))
        for each in input_xml.getroot():
            if each.tag.endswith('JobID'):
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
        <wps:Result xmlns:ows="http://www.opengis.net/ows/2.0" xmlns:schemaLocation="http://schemas.opengis.net/wps/2.0/wps.xsd" xmlns:wps="http://www.opengis.net/wps/2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><wps:JobID>f3780917-92c0-4440-8a84-9b28c2e64fa8</wps:JobID><wps:Output id="output-2021-05-26T18:39:14.381083"><wps:Data>http://geospec-dataset-bucket-dev.s3-website.amazonaws.com/malarout/dps_output/hytools_ubuntu/v-system-test-5/2021/05/26/18/39/14/381083</wps:Data><wps:Data>s3://s3.amazonaws.com:80/geospec-dataset-bucket-dev/malarout/dps_output/hytools_ubuntu/v-system-test-5/2021/05/26/18/39/14/381083</wps:Data><wps:Data>https://s3.console.aws.amazon.com/s3/buckets/geospec-dataset-bucket-dev/malarout/dps_output/hytools_ubuntu/v-system-test-5/2021/05/26/18/39/14/381083/?region=us-east-1&amp;tab=overview</wps:Data></wps:Output></wps:Result>
        """
        input_xml = ET.ElementTree(ET.fromstring(input_xml_str))
        for each in input_xml.getroot():
            if each.tag.endswith('Output'):
                for eachOutput in each:
                    if eachOutput.tag.endswith('Data'):
                        self.outputs.append(eachOutput.text)
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
