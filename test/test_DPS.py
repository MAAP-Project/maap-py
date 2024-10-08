from unittest import TestCase

import yaml

from maap.maap import MAAP
import logging
from yaml import load as yaml_load, dump as yaml_dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class TestDPS(TestCase):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    @classmethod
    def setUpClass(cls):
        cls.logger.debug("Initializing MAAP")
        cls.maap = MAAP()

    def test_registerAlgorithm(self):
        self.maap.register_algorithm_from_yaml_file("dps_test_algo_config.yaml")

    def test_deleteAlgorithm(self):
        pass

    def test_deleteJob(self):
        pass

    def test_describeAlgorithm(self):
        pass

    def test_dismissJob(self):
        pass

    def test_getJobMetrics(self):
        pass

    def test_getJobResult(self):
        pass

    def test_getJobStatus(self):
        pass

    def test_getQueues(self):
        pass

    def test_listAlgorithms(self):
        pass

    def test_listJobs(self):
        pass

    def test_publishAlgorithm(self):
        pass


    def test_submitJob(self):
        pass

