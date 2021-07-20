import requests
import shutil
import os
import urllib
import boto3
from urllib.parse import urlparse


class Result(dict):
    """
    The class to structure the response xml string from the cmr API
    """
    _location = None

    # TODO: add destpath as config setting
    def getLocalPath(self, destpath=".", overwrite=False):
        """
        Download the dataset into file system
        :param destpath: use the current directory as default
        :param overwrite: don't download by default if the target file exists
        :return:
        """
        url = self._location
        destfile = self._downloadname.replace('/', '')

        # Downloadable url does not exist
        if not url:
            return None
        if url.startswith('ftp'):
            if not overwrite and not os.path.isfile(destpath + "/" + destfile):
                urllib.urlretrieve(url, destpath + "/" + destfile)

            return destpath + '/' + destfile
        elif url.startswith('s3'):
            o = urlparse(url)
            filename = url[url.rfind("/") + 1:]
            if not overwrite and not os.path.isfile(destpath + "/" + filename):
                s3 = boto3.client('s3', aws_access_key_id=self._awsKey, aws_secret_access_key=self._awsSecret)
                s3.download_file(o.netloc, o.path.lstrip('/'), destpath + "/" + filename)

            return destpath + '/' + filename
        else:
            if not overwrite and not os.path.isfile(destpath + "/" + destfile):
                r = requests.get(url, stream=True, verify=False,)
                r.raw.decode_content = True

                with open(destpath + "/" + destfile, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)

            return destpath + '/' + destfile

    def getRelatedData(self, destpath=".", overwrite=False):
        """
        Download the dataset into file system
        :param destpath: use the current directory as default
        :param overwrite: don't download by default if the target file exists
        :return:
        """

        downloads = self._relatedUrls
        output = []

        # Downloadable url does not exist
        if not downloads:
            return None

        for download in downloads:
            url = download['URL']
            destfile = url.split("/")[-1].replace('/', '')

            if not overwrite and not os.path.isfile(destpath + "/" + destfile):

                # Get granule oauth2 URL
                s = requests.Session()
                response = s.get(url)
                #print(response.status_code, response.url)
                # Get granule
                s.headers.update({'Authorization': f'Bearer {self._ursToken},Basic {os.environ.get("MAAP_APP_CREDS")}',
                                  'Connection':'close'})

                r = s.get(url=response.url, stream=True)
                r.raw.decode_content = True

                with open(destpath + "/" + destfile, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)

                output.append(destpath + "/" + destfile)

        return output


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
    def __init__(self, metaResult, awsAccessKey, awsAccessSecret, ursToken):

        self._awsKey = awsAccessKey
        self._awsSecret = awsAccessSecret
        self._ursToken = ursToken

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
