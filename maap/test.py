import os
import os.path as op
from unittest import TestCase

from .Result import Granule

class TestGranule(TestCase):

    def test_granule(self):
        browse_url = 's3://test-bucket/test-id.cog'
        granule_meta = {
          'Granule': {
              'GrauleUr': 'test-id',
              'OnlineAccessURLs': {
                'OnlineAccessURL': {
                  'URL': 's3://test-bucket/test-id.cmr.xml'
                }
              },
              'OnlineResources': {
                'OnlineResource': {
                  'Type': 'BROWSE',
                  'URL': browse_url
                }
              }              
            }
        }
        granule = Granule(granule_meta, 'aws_access_key_id', 'aws_secret_access_key')
        assert(granule._BrowseUrl == browse_url)
