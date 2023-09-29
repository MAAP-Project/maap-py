from unittest import TestCase
from maap.maap import MAAP
from maap.utils.TokenHandler import TokenHandler
from unittest.mock import MagicMock
import re


class TestMAAP(TestCase):
    @classmethod
    def setUpClass(cls):
        config_file_path = "./maap.cfg"

        cls.maap = MAAP()

    def test_getProfile(self):
        results = self.maap.profile.account_info()

        self.assertTrue(results['id'] > 0)

    def test_genFromEarthdata(self):
        input = """
            {
             "p": "C1200015068-NASA_MAAP!C1200090707-NASA_MAAP!C1200015148-NASA_MAAP",
             "pg": [
              {
               "exclude": {
                "echo_granule_id": [
                 "G1200015109-NASA_MAAP",
                 "G1200015110-NASA_MAAP"
                ]
               }
              }
             ],
             "m": "-87.55224609375!75.30249023437501!0!1!0!0,2",
             "processing_level_id_h": [
              "1A",
              "1B",
              "2",
              "4"
             ],
             "instrument_h": [
              "LVIS",
              "UAVSAR"
             ],
             "platform_h": [
              "AIRCRAFT",
              "B-200",
              "COMPUTERS"
             ],
             "data_center_h": [
              "MAAP Data Management Team"
             ],
             "bounding_box": "-35.4375,-55.6875,-80.4375,37.6875"
            }
        """

        var_name = 'maapVar'
        testResult = self.maap.getCallFromEarthdataQuery(query=input, variable_name=var_name)
        self.assertTrue(
            testResult == var_name + '.searchGranule('\
                'processing_level_id="1A|1B|2|4", '\
                'instrument="LVIS|UAVSAR", '\
                'platform="AIRCRAFT|B-200|COMPUTERS", '\
                'data_center="MAAP Data Management Team", '\
                'bounding_box="-35.4375,-55.6875,-80.4375,37.6875")')

    def test_TokenHandler(self):
        th = TokenHandler("a-K9YbTr8h112zW5pLV8Fw")
        token = th.get_access_token()
        self.assertTrue(token != 'unauthorized' and len(token) > 0)

    def test_uploadFiles(self):
        self.maap._upload_s3 = MagicMock(return_value=None)
        result = self.maap.uploadFiles(['test/s3-upload-testfile1.txt', 'test/s3-upload-testfile2.txt'])
        upload_msg_regex = re.compile('Upload file subdirectory: .+ \\(keep a record of this if you want to share these files with other users\\)')
        self.assertTrue(re.match(upload_msg_regex, result))

    def test_edc_credentials(self):
        results = self.maap.aws.earthdata_s3_credentials('https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials');
        self.assertTrue(results != 'unauthorized' and len(results) > 0)
