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

        cls._test_instrument_name_uavsar = 'UAVSAR'
        cls._test_instrument_name_lvis= 'lvis'
        cls._test_track_number = '001'
        cls._test_ur = 'uavsar_AfriSAR_v1-cov_lopenp_14043_16008_140_001_160225-geo_cov_4-4.bin'
        cls._test_site_name = 'lope'

    def test_searchGranuleByInstrumentAndTrackNumber(self):
        results = self.maap.searchGranule(
            instrument=self._test_instrument_name_uavsar,
            track_number=self._test_track_number,
            polarization='HH')
        self.assertTrue('concept-id' in results[0].keys())

    def test_searchGranuleByGranuleUR(self):
        results = self.maap.searchGranule(
            granule_ur=self._test_ur)
        self.assertTrue('concept-id' in results[0].keys())

    def test_granuleDownload(self):
        results = self.maap.searchGranule(
            granule_ur=self._test_ur)
        download = results[0].getLocalPath('/Users/satorius/source')
        self.assertTrue(len(download) > 0)

    def test_searchGranuleByInstrumentAndSiteName(self):
        results = self.maap.searchGranule(
            instrument=self._test_instrument_name_lvis,
            site_name=self._test_site_name)
        self.assertTrue('concept-id' in results[0].keys())

    def test_searchGranuleWithPipeDelimiters(self):
        results = self.maap.searchGranule(
            instrument="LVIS|UAVSAR",
            platform="AIRCRAFT")
        self.assertTrue('concept-id' in results[0].keys())

    def test_searchFromEarthdata(self):
        results = self.maap.searchCollection(
            instrument="LVIS|UAVSAR",
            platform="AIRCRAFT|B-200|COMPUTERS",
            data_center="MAAP Data Management Team|ORNL_DAAC")
        self.assertTrue('concept-id' in results[0].keys())

    def test_searchCollection(self):
        results = self.maap.searchCollection(
            instrument=self._test_instrument_name_uavsar)
        self.assertTrue('concept-id' in results[0].keys())

    def test_searchGranuleWithWildcards(self):
        results = self.maap.searchGranule(collection_concept_id="C1200110748-NASA_MAAP",
                                              readable_granule_name='*185*')
        self.assertTrue('concept-id' in results[0].keys())

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

    # Awaiting persistent HySDS cluster availability
    def test_registerAlgorithm(self):
        self.fail()

    # Awaiting persistent HySDS cluster availability
    def test_getJobStatus(self):
        self.fail()

    def test_TokenHandler(self):
        th = TokenHandler("a-K9YbTr8h112zW5pLV8Fw")
        token = th.get_access_token()
        self.assertTrue(token != 'unauthorized' and len(token) > 0)

    def test_uploadFiles(self):
        self.maap._upload_s3 = MagicMock(return_value=None)
        result = self.maap.uploadFiles(['test/s3-upload-testfile1.txt', 'test/s3-upload-testfile2.txt'])
        upload_msg_regex = re.compile('Upload file subdirectory: .+ \\(keep a record of this if you want to share these files with other users\\)')
        self.assertTrue(re.match(upload_msg_regex, result))

    def test_executeQuery(self):
        response = self.maap.executeQuery(
            src={
                "Collection": {
                    "ShortName": "GEDI Cal/Val Field Data_1",
                    "VersionId": "001"
                }
            },
            query={
                "bbox": [
                -122.6,
                38.4,
                -122.5,
                38.5
                ],
                "fields": ['project', 'plot', 'p.geom']
            }
        )
        self.assertEqual(
            response.json(),
            [
                {
                    "project":"usa_sonoma",
                    "plot":"11",
                    "p.geom":"POINT(538336.000000 4257761.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"23",
                    "p.geom":"POINT(538276.000000 4257822.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"49",
                    "p.geom":"POINT(538274.000000 4257876.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"4",
                    "p.geom":"POINT(537433.000000 4257430.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"18",
                    "p.geom":"POINT(537765.000000 4257253.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"48",
                    "p.geom":"POINT(538727.000000 4257633.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"12a",
                    "p.geom":"POINT(538128.000000 4257546.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"4",
                    "p.geom":"POINT(537433.000000 4257430.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"18",
                    "p.geom":"POINT(537765.000000 4257253.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"12a",
                    "p.geom":"POINT(538128.000000 4257546.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"11",
                    "p.geom":"POINT(538336.000000 4257761.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"23",
                    "p.geom":"POINT(538276.000000 4257822.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"49",
                    "p.geom":"POINT(538274.000000 4257876.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"11",
                    "p.geom":"POINT(538336.000000 4257761.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"18",
                    "p.geom":"POINT(537765.000000 4257253.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"49",
                    "p.geom":"POINT(538274.000000 4257876.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"8a",
                    "p.geom":"POINT(537587.000000 4257361.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"11",
                    "p.geom":"POINT(538336.000000 4257761.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"36",
                    "p.geom":"POINT(539266.000000 4257157.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"49",
                    "p.geom":"POINT(538274.000000 4257876.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"4",
                    "p.geom":"POINT(537433.000000 4257430.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"18",
                    "p.geom":"POINT(537765.000000 4257253.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"48",
                    "p.geom":"POINT(538727.000000 4257633.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"8a",
                    "p.geom":"POINT(537587.000000 4257361.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"4",
                    "p.geom":"POINT(537433.000000 4257430.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"18",
                    "p.geom":"POINT(537765.000000 4257253.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"48",
                    "p.geom":"POINT(538727.000000 4257633.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"12a",
                    "p.geom":"POINT(538128.000000 4257546.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"4",
                    "p.geom":"POINT(537433.000000 4257430.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"18",
                    "p.geom":"POINT(537765.000000 4257253.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"36",
                    "p.geom":"POINT(539266.000000 4257157.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"48",
                    "p.geom":"POINT(538727.000000 4257633.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"12a",
                    "p.geom":"POINT(538128.000000 4257546.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"11",
                    "p.geom":"POINT(538336.000000 4257761.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"23",
                    "p.geom":"POINT(538276.000000 4257822.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"45",
                    "p.geom":"POINT(539261.000000 4257132.000000)"
                },
                {
                    "project":"usa_sonoma",
                    "plot":"49",
                    "p.geom": "POINT(538274.000000 4257876.000000)"
                }
            ]
        )
