from unittest import TestCase
from maap.maap import MAAP


class TestDPS(TestCase):

    @classmethod
    def setUpClass(cls):
        config_file_path = "./maap.cfg"

        cls.maap = MAAP()

        cls._test_job_id = "a80434cc-3d63-4059-9ba6-2cd5ddb7d5ef"

    def test_registerAlgorithm(self):
        self.fail()

    def test_getJobs(self):
        results = self.maap.getJobStatus(self._test_job_id)
        self.assertTrue('concept-id' in results[0].keys())

    def test_getJobSubmit(self):
        results = self.maap.submitJob(identifier="brian", algo_id="hello-world-output_ubuntu", version="master",
                                      username="bsatoriu")
        self.assertTrue(results.status_code == 200)

    def test_getJobStatus(self):
        results = self.maap.getJobStatus(self._test_job_id)
        self.assertTrue(results.status_code == 200)

