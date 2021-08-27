import requests
import shutil
import os
import urllib
import boto3
from urllib.parse import urlparse
from maap.utils import endpoints

class Result(dict):
    """
    The class to structure the response xml string from the cmr API
    """
    _location = None

    def getData(self, destpath=".", overwrite=False):
        """
        Download the dataset into file system
        :param destpath: use the current directory as default
        :param overwrite: don't download by default if the target file exists
        :return:
        """
        url = self._location
        destfile = self._downloadname.replace('/', '')

        if not url:
            # Downloadable url does not exist
            return None
        if url.startswith('ftp'):
            if not overwrite and not os.path.isfile(destpath + "/" + destfile):
                urllib.urlretrieve(url, destpath + "/" + destfile)

            return destpath + '/' + destfile
        elif url.startswith('s3'):
            try:
                o = urlparse(url)
                filename = url[url.rfind("/") + 1:]
                if not overwrite and not os.path.isfile(destpath + "/" + filename):
                    s3 = boto3.client('s3')
                    s3.download_file(o.netloc, o.path.lstrip('/'), destpath + "/" + filename)
            except:
                # Fallback to HTTP
                http_url = self._convertS3toHttp(url)
                return self._getHttpData(http_url, overwrite, destpath, destfile)

            return destpath + '/' + filename
        else:
            return self._getHttpData(url, overwrite, destpath, destfile)

    def getLocalPath(self, destpath=".", overwrite=False):
        """
        Deprecated method. User getData() instead.
        """
        return self.getData(destpath, overwrite)

    def _convertS3toHttp(self, url):
        url = url[5:].split('/')
        url[0] += '.s3.amazonaws.com'
        url = 'https://' + '/'.join(url)
        return url

    def _getHttpData(self, url, overwrite, destpath, destfile):
        if not overwrite and not os.path.isfile(destpath + "/" + destfile):
            r = requests.get(url, stream=True)

            # Try with a federated token if unauthorized
            if r.status_code == 401:
                r = requests.get(
                    url=os.path.join(self._cmrFileUrl,
                                     urllib.parse.quote(urllib.parse.quote(url, safe='')),
                                     endpoints.CMR_ALGORITHM_DATA),
                    headers=self._apiHeader,
                    stream=True
                )

                if r.status_code != 200:
                    raise ValueError('Bad search response for url {}: {}'.format(url, r.text))

            r.raw.decode_content = True

            with open(destpath + "/" + destfile, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

        return destpath + '/' + destfile

    def getDownloadUrl(self):
        """
        :return:
        """
        return self._location

    def getDescription(self):
        """

        :return:
        """
        return self['Granule']['GranuleUR'].ljust(70) + 'Updated ' + self['Granule']['LastUpdate'] + ' (' + self['collection-concept-id'] + ')'


class Collection(Result):
    def __init__(self, metaResult, maap_host):
        for k in metaResult:
            self[k] = metaResult[k]

        self._location = 'https://{}/search/concepts/{}.umm-json'.format(maap_host, metaResult['concept-id'])
        self._downloadname = metaResult['Collection']['ShortName']


class Granule(Result):
    def __init__(self, metaResult, awsAccessKey, awsAccessSecret, cmrFileUrl, apiHeader):

        self._awsKey = awsAccessKey
        self._awsSecret = awsAccessSecret
        self._cmrFileUrl = cmrFileUrl
        self._apiHeader = apiHeader

        for k in metaResult:
            self[k] = metaResult[k]

        # Retrieve downloadable url
        try:
            self._location = self['Granule']['OnlineAccessURLs']['OnlineAccessURL']['URL']
            self._downloadname = self._location.split("/")[-1]
        except :
            self._location = None

        # TODO: make self._location an array and consolidate with _relatedUrls
        try:
            self._relatedUrls = self['Granule']['OnlineAccessURLs']['OnlineAccessURL']
            self._location = self['Granule']['OnlineAccessURLs']['OnlineAccessURL'][0]['URL']
            self._downloadname = self._location.split("/")[-1]
        except :
            self._relatedUrls = None

        # Retrieve OPeNDAPUrl
        try:
            urls = self['Granule']['OnlineResources']['OnlineResource']
            # This throws an error "filter object is not subscriptable"
            self._OPeNDAPUrl = filter(lambda x: x["Type"] == "OPeNDAP", urls)['URL']
            self._BrowseUrl = list(filter(lambda x: x["Type"] == "BROWSE", urls))[0]['URL']
        except :
            self._OPeNDAPUrl = None
            self._BrowseUrl = None

    def getOPeNDAPUrl(self):
        return self._OPeNDAPUrl
