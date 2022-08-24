import os
import shutil
import urllib
from urllib.parse import urlparse

import boto3
import requests

from maap.utils import endpoints


class Result(dict):
    """
    The class to structure the response xml string from the cmr API
    """

    _location = None
    _fallback = None

    def getData(self, destpath=".", overwrite=False):
        """
        Download the dataset into file system
        :param destpath: use the current directory as default
        :param overwrite: don't download by default if the target file exists
        :return:
        """
        url = self._location
        destfile = self._downloadname.replace("/", "")

        if not url:
            # Downloadable url does not exist
            return None
        if url.startswith("ftp"):
            if not overwrite and not os.path.isfile(f"{destpath}/{destfile}"):
                urllib.urlretrieve(url, f"{destpath}/{destfile}")

            return destpath + "/" + destfile
        elif url.startswith("s3"):
            try:
                o = urlparse(url)
                filename = os.path.basename(o.path)
                if not overwrite and not os.path.isfile(f"{destpath}/{filename}"):
                    s3 = boto3.client("s3")
                    s3.download_file(
                        o.netloc, o.path.lstrip("/"), f"{destpath}/{filename}"
                    )
            except:
                # Fallback to HTTP
                if self._fallback:
                    return self._getHttpData(
                        self._fallback, overwrite, destpath, destfile
                    )
                else:
                    raise

            return f"{destpath}/{filename}"
        else:
            return self._getHttpData(url, overwrite, destpath, destfile)

    def getLocalPath(self, destpath=".", overwrite=False):
        """
        Deprecated method. User getData() instead.
        """
        return self.getData(destpath, overwrite)

    def _convertS3toHttp(self, url):
        url = url[5:].split("/")
        url[0] += ".s3.amazonaws.com"
        url = "https://" + "/".join(url)
        return url

    # When retrieving granule data, always try an unauthenticated HTTPS request first,
    # then fall back to EDL federated login.
    # In the case where an external DAAC is called (which we know from the cmr_host parameter),
    # we may consider skipping the unauthenticated HTTPS request,
    # but this method assumes that granules can both be publicly accessible or EDL-restricted.
    # In the former case, this conditional logic will stream the data directly from CMR,
    # rather than via the MAAP API proxy.
    # This direct interface with CMR is the default method since it reduces traffic to the MAAP API.
    def _getHttpData(self, url, overwrite, destpath, destfile):
        if not overwrite and not os.path.isfile(destpath + "/" + destfile):
            r = requests.get(url, stream=True)

            # Try with a federated token if unauthorized
            if r.status_code == 401:
                r = requests.get(
                    url=os.path.join(
                        self._cmrFileUrl,
                        urllib.parse.quote(urllib.parse.quote(url, safe="")),
                        endpoints.CMR_ALGORITHM_DATA,
                    ),
                    headers=self._apiHeader,
                    stream=True,
                )

            r.raise_for_status()
            r.raw.decode_content = True

            with open(destpath + "/" + destfile, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return destpath + "/" + destfile

    def getDownloadUrl(self):
        """
        :return:
        """
        return self._location

    def getDescription(self):
        """

        :return:
        """
        return (
            self["Granule"]["GranuleUR"].ljust(70)
            + "Updated "
            + self["Granule"]["LastUpdate"]
            + " ("
            + self["collection-concept-id"]
            + ")"
        )


class Collection(Result):
    def __init__(self, metaResult, maap_host):
        for k in metaResult:
            self[k] = metaResult[k]

        self._location = "https://{}/search/concepts/{}.umm-json".format(
            maap_host, metaResult["concept-id"]
        )
        self._downloadname = metaResult["Collection"]["ShortName"]


class Granule(Result):
    def __init__(
        self, metaResult, awsAccessKey, awsAccessSecret, cmrFileUrl, apiHeader
    ):

        self._awsKey = awsAccessKey
        self._awsSecret = awsAccessSecret
        self._cmrFileUrl = cmrFileUrl
        self._apiHeader = apiHeader

        for k in metaResult:
            self[k] = metaResult[k]

        # TODO: make self._location an array and consolidate with _relatedUrls
        try:
            self._relatedUrls = self["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"]

            # XML of singular OnlineAccessURL is an object, convert it to a list of one object
            if isinstance(self["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"], dict):
                self["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"] = [
                    self["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"]
                ]

            # Sets _location to s3 url, defaults to first url in list
            self._location = next(
                (
                    obj["URL"]
                    for obj in self["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"]
                    if obj["URL"].startswith("s3://")
                ),
                self["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"][0]["URL"],
            )

            if self._location:
                o = urlparse(self._location)
                filename = os.path.basename(o.path)

            # Sets _fallback to https url with the same basename as _location
            self._fallback = next(
                (
                    obj["URL"]
                    for obj in self["Granule"]["OnlineAccessURLs"]["OnlineAccessURL"]
                    if obj["URL"].startswith("https://")
                    and obj["URL"].endswith(filename)
                ),
                None,
            )

            self._downloadname = filename
        except:
            self._relatedUrls = None

        # Retrieve OPeNDAPUrl
        try:
            urls = self["Granule"]["OnlineResources"]["OnlineResource"]
            # This throws an error "filter object is not subscriptable"
            self._OPeNDAPUrl = filter(lambda x: x["Type"] == "OPeNDAP", urls)["URL"]
            self._BrowseUrl = list(filter(lambda x: x["Type"] == "BROWSE", urls))[0][
                "URL"
            ]
        except:
            self._OPeNDAPUrl = None
            self._BrowseUrl = None

    def getOPeNDAPUrl(self):
        return self._OPeNDAPUrl
