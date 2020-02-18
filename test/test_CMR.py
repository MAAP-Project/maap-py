from unittest import TestCase
from maap.maap import MAAP


class TestCMR(TestCase):

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

