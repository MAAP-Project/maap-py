import json
import os
import shutil
import sys
import urllib.parse
from urllib.parse import urlparse
import boto3
import requests
from maap.utils import endpoints

if sys.version_info < (3, 0):
    from urllib import urlretrieve
else:
    from urllib.request import urlretrieve


class Result(dict):
    """Class to structure the response XML from a CMR API request."""

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
        dest = os.path.join(destpath, destfile)

        if not url:
            # Downloadable url does not exist
            return None
        if url.startswith("ftp"):
            if overwrite or not os.path.exists(dest):
                urlretrieve(url, dest)

            return dest
        if url.startswith("s3"):
            try:
                filename = url[url.rfind("/") + 1 :]
                dest = os.path.join(destpath, filename)

                if overwrite or not os.path.exists(dest):
                    url = urlparse(url)
                    s3 = boto3.client("s3")
                    s3.download_file(url.netloc, url.path.lstrip("/"), dest)

                return dest
            except:
                # Fallback to HTTP
                if self._fallback:
                    return self._getHttpData(
                        self._fallback, overwrite, dest
                    )
                else:
                    raise

        return self._getHttpData(url, overwrite, dest)

    def getLocalPath(self, destpath=".", overwrite=False):
        """
        Deprecated method. Use getData() instead.
        """
        return self.getData(destpath, overwrite)

    def _convertS3toHttp(self, url):
        url = url[5:].split("/")
        url[0] += ".s3.amazonaws.com"
        url = "https://" + "/".join(url)
        return url

    # When retrieving granule data, always try an unauthenticated HTTPS request first,
    # then fall back to EDL federated login.
    #
    # In the case where an external DAAC is called (which we know from the `cmr_host`
    # parameter), we may consider skipping the unauthenticated HTTPS request, but this
    # method assumes that granules can both be publicly accessible or EDL-restricted.
    # In the former case, this conditional logic will stream the data directly from CMR,
    # rather than via the MAAP API proxy.
    #
    # This direct interface with CMR is the default method since it reduces traffic to
    # the MAAP API.
    def _getHttpData(self, url, overwrite, dest):
        if overwrite or not os.path.exists(dest):
            r = requests.get(url, stream=True)

            # Try with a federated token if unauthorized
            if r.status_code == 401:
                if self._dps.running_in_dps:
                    dps_token_response = requests.get(
                        url=self._dps.dps_token_endpoint,
                        headers={
                            "dps-machine-token": self._dps.dps_machine_token,
                            "dps-job-id": self._dps.job_id,
                            "Accept": "application/json",
                        },
                    )

                    if dps_token_response:
                        # Running inside a DPS job, so call DAAC directly
                        dps_token_info = json.loads(dps_token_response.text)
                        r = requests.get(
                            url=r.url,
                            headers={
                                "Authorization": "Bearer {},Basic {}".format(
                                    dps_token_info["user_token"],
                                    dps_token_info["app_token"],
                                ),
                                "Connection": "close",
                            },
                            stream=True,
                        )
                else:
                    # Running in ADE, so call MAAP API
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

            with open(dest, "wb") as f:
                shutil.copyfileobj(r.raw, f)

        return dest

    def getDownloadUrl(self, s3=True):
        """
        Get granule download url
        :param s3: True returns the s3 url, False the http url
        :return:
        """
        return self.getS3Url() if s3 else self.getHttpUrl()

    def getHttpUrl(self):
        """
        Get granule http url
        :return:
        """
        return self._fallback

    def getS3Url(self):
        """
        Get granule s3 url
        :return:
        """
        return self._location

    def getDescription(self):
        """
        :return:
        """
        return "{} Updated {} ({})".format(
            self["Granule"]["GranuleUR"].ljust(70),
            self["Granule"]["LastUpdate"],
            self["collection-concept-id"],
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
        self, metaResult, awsAccessKey, awsAccessSecret, cmrFileUrl, apiHeader, dps
    ):
        self._awsKey = awsAccessKey
        self._awsSecret = awsAccessSecret
        self._cmrFileUrl = cmrFileUrl
        self._apiHeader = apiHeader
        self._dps = dps

        self._relatedUrls = None
        self._location = None
        self._downloadname = None
        self._OPeNDAPUrl = None
        self._BrowseUrl = None

        self._relatedUrls = None
        self._location = None
        self._downloadname = None
        self._OPeNDAPUrl = None
        self._BrowseUrl = None

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
            pass

    def getOPeNDAPUrl(self):
        return self._OPeNDAPUrl
