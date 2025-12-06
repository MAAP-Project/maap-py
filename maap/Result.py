"""
Result Classes for CMR Search Results
======================================

This module provides classes for handling CMR (Common Metadata Repository)
search results, including granules and collections.

Classes
-------
Result
    Base class for CMR search results with data download capabilities.
Granule
    Represents a CMR granule (individual data file) with multiple access URLs.
Collection
    Represents a CMR collection (dataset) with metadata access.

Example
-------
Search and download granules::

    from maap.maap import MAAP

    maap = MAAP()
    granules = maap.searchGranule(short_name='GEDI02_A', limit=5)

    for granule in granules:
        # Get download URLs
        s3_url = granule.getS3Url()
        http_url = granule.getHttpUrl()

        # Download to local filesystem
        local_path = granule.getData(destpath='/tmp')
        print(f"Downloaded: {local_path}")

See Also
--------
:class:`maap.maap.MAAP` : Main client class for searching
"""

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
    """
    Base class for CMR search result items.

    The Result class extends Python's dict to provide convenient access to
    CMR metadata fields while adding methods for data download.

    This class serves as the base for :class:`Granule` and :class:`Collection`
    classes, which provide type-specific functionality.

    Attributes
    ----------
    _location : str or None
        Primary download URL (typically S3).
    _fallback : str or None
        Fallback download URL (typically HTTPS).
    _downloadname : str or None
        Filename to use when downloading.

    Notes
    -----
    Result objects behave like dictionaries, allowing direct access to CMR
    metadata fields using bracket notation (e.g., ``result['Granule']['GranuleUR']``).

    See Also
    --------
    :class:`Granule` : Granule-specific result class
    :class:`Collection` : Collection-specific result class
    """

    _location = None
    _fallback = None

    def getData(self, destpath=".", overwrite=False):
        """
        Download the data file to the local filesystem.

        Downloads the data file associated with this result to a local
        directory. Supports S3, HTTP/HTTPS, and FTP protocols.

        Parameters
        ----------
        destpath : str, optional
            Destination directory for the download. Default is the current
            working directory (``'.'``).
        overwrite : bool, optional
            If ``True``, overwrite existing files. If ``False`` (default),
            skip download if file already exists.

        Returns
        -------
        str or None
            Local path to the downloaded file, or ``None`` if no download
            URL is available.

        Examples
        --------
        Download to current directory::

            >>> local_path = granule.getData()
            >>> print(f"Downloaded to: {local_path}")

        Download to specific directory::

            >>> local_path = granule.getData(destpath='/tmp/data')

        Force re-download::

            >>> local_path = granule.getData(overwrite=True)

        Notes
        -----
        Download strategy:

        1. If URL is FTP, downloads directly via FTP
        2. If URL is S3, attempts direct S3 download using boto3
        3. If S3 fails, falls back to HTTPS URL if available
        4. Otherwise uses HTTP download with authentication

        See Also
        --------
        :meth:`getDownloadUrl` : Get the download URL without downloading
        :meth:`getS3Url` : Get the S3 URL
        :meth:`getHttpUrl` : Get the HTTP URL
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
        Download data to local filesystem.

        .. deprecated::
            Use :meth:`getData` instead. This method is kept for backwards
            compatibility.

        Parameters
        ----------
        destpath : str, optional
            Destination directory.
        overwrite : bool, optional
            Whether to overwrite existing files.

        Returns
        -------
        str or None
            Local path to the downloaded file.

        See Also
        --------
        :meth:`getData` : Preferred method for downloading data
        """
        return self.getData(destpath, overwrite)

    def _convertS3toHttp(self, url):
        """
        Convert an S3 URL to an HTTPS URL.

        Parameters
        ----------
        url : str
            S3 URL in the format ``s3://bucket/key``.

        Returns
        -------
        str
            HTTPS URL pointing to the same object.
        """
        url = url[5:].split("/")
        url[0] += ".s3.amazonaws.com"
        url = "https://" + "/".join(url)
        return url

    def _getHttpData(self, url, overwrite, dest):
        """
        Download data via HTTP with authentication fallback.

        Downloads data from an HTTP URL, automatically handling authentication
        when required. First attempts an unauthenticated request, then falls
        back to EDL (Earthdata Login) authentication if needed.

        Parameters
        ----------
        url : str
            The HTTP URL to download from.
        overwrite : bool
            Whether to overwrite existing files.
        dest : str
            Local destination path for the downloaded file.

        Returns
        -------
        str
            The local path to the downloaded file.

        Notes
        -----
        Authentication strategy:

        - First attempts unauthenticated request (for public data)
        - If 401 response and running in DPS, uses machine token
        - If 401 response and running in ADE, uses MAAP API proxy

        This direct interface with CMR is preferred as it reduces traffic
        to the MAAP API.
        """
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
        Get the download URL for this result.

        Parameters
        ----------
        s3 : bool, optional
            If ``True`` (default), return the S3 URL. If ``False``, return
            the HTTP URL.

        Returns
        -------
        str or None
            The download URL, or ``None`` if not available.

        Examples
        --------
        Get S3 URL (default)::

            >>> url = granule.getDownloadUrl()
            >>> print(url)
            s3://bucket/path/file.h5

        Get HTTP URL::

            >>> url = granule.getDownloadUrl(s3=False)
            >>> print(url)
            https://data.maap-project.org/path/file.h5

        See Also
        --------
        :meth:`getS3Url` : Get S3 URL directly
        :meth:`getHttpUrl` : Get HTTP URL directly
        :meth:`getData` : Download the file
        """
        return self.getS3Url() if s3 else self.getHttpUrl()

    def getHttpUrl(self):
        """
        Get the HTTP download URL for this result.

        Returns
        -------
        str or None
            The HTTPS URL for downloading, or ``None`` if not available.

        Examples
        --------
        ::

            >>> http_url = granule.getHttpUrl()
            >>> print(http_url)
            https://data.maap-project.org/path/file.h5

        See Also
        --------
        :meth:`getS3Url` : Get S3 URL
        :meth:`getDownloadUrl` : Get either URL type
        """
        return self._fallback

    def getS3Url(self):
        """
        Get the S3 download URL for this result.

        Returns
        -------
        str or None
            The S3 URL for downloading, or ``None`` if not available.

        Examples
        --------
        ::

            >>> s3_url = granule.getS3Url()
            >>> print(s3_url)
            s3://maap-data-store/path/file.h5

        Notes
        -----
        S3 URLs require appropriate AWS credentials to access. When running
        in the MAAP ADE or DPS, credentials are typically configured
        automatically.

        See Also
        --------
        :meth:`getHttpUrl` : Get HTTP URL
        :meth:`getDownloadUrl` : Get either URL type
        """
        return self._location

    def getDescription(self):
        """
        Get a human-readable description of this result.

        Returns
        -------
        str
            A formatted string containing the granule identifier, last update
            time, and collection concept ID.

        Examples
        --------
        ::

            >>> print(granule.getDescription())
            GEDI02_A_2019123_O02389_T05321_02_001_01.h5     Updated 2020-01-15 (C1234567890-MAAP)
        """
        return "{} Updated {} ({})".format(
            self["Granule"]["GranuleUR"].ljust(70),
            self["Granule"]["LastUpdate"],
            self["collection-concept-id"],
        )


class Collection(Result):
    """
    CMR Collection search result.

    Represents a collection (dataset) from the CMR. Collections contain
    metadata about a group of related data files (granules).

    Parameters
    ----------
    metaResult : dict
        The CMR metadata dictionary for this collection.
    maap_host : str
        The MAAP API hostname.

    Attributes
    ----------
    _location : str
        URL to the UMM-JSON metadata for this collection.
    _downloadname : str
        The collection short name.

    Examples
    --------
    Search for collections::

        >>> collections = maap.searchCollection(short_name='GEDI02_A')
        >>> for c in collections:
        ...     print(c['Collection']['ShortName'])
        ...     print(c['Collection']['Description'])

    Access collection metadata::

        >>> collection = collections[0]
        >>> print(collection['concept-id'])
        >>> print(collection['Collection']['ShortName'])

    Notes
    -----
    Collection objects inherit dictionary access from :class:`Result`,
    allowing direct access to CMR metadata fields.

    See Also
    --------
    :class:`Granule` : Individual data file results
    :meth:`maap.maap.MAAP.searchCollection` : Search for collections
    """

    def __init__(self, metaResult, maap_host):
        for k in metaResult:
            self[k] = metaResult[k]

        self._location = "https://{}/search/concepts/{}.umm-json".format(
            maap_host, metaResult["concept-id"]
        )
        self._downloadname = metaResult["Collection"]["ShortName"]


class Granule(Result):
    """
    CMR Granule search result.

    Represents a granule (individual data file) from the CMR. Granules are
    the actual data products that can be downloaded and analyzed.

    Parameters
    ----------
    metaResult : dict
        The CMR metadata dictionary for this granule.
    awsAccessKey : str
        AWS access key for S3 operations.
    awsAccessSecret : str
        AWS secret key for S3 operations.
    cmrFileUrl : str
        Base CMR file URL for authenticated downloads.
    apiHeader : dict
        HTTP headers for API requests.
    dps : DpsHelper
        DPS helper for token management.

    Attributes
    ----------
    _location : str or None
        Primary download URL (preferably S3).
    _fallback : str or None
        Fallback HTTPS download URL.
    _downloadname : str or None
        Filename for downloads.
    _OPeNDAPUrl : str or None
        OPeNDAP data access URL if available.
    _BrowseUrl : str or None
        Browse image URL if available.
    _relatedUrls : list or None
        All available access URLs.

    Examples
    --------
    Search and access granule metadata::

        >>> granules = maap.searchGranule(short_name='GEDI02_A', limit=5)
        >>> granule = granules[0]
        >>> print(granule['Granule']['GranuleUR'])

    Download granule data::

        >>> # Get URLs
        >>> s3_url = granule.getS3Url()
        >>> http_url = granule.getHttpUrl()
        >>>
        >>> # Download to local filesystem
        >>> local_path = granule.getData(destpath='/tmp')

    Access OPeNDAP URL::

        >>> opendap_url = granule.getOPeNDAPUrl()
        >>> if opendap_url:
        ...     print(f"OPeNDAP access: {opendap_url}")

    Notes
    -----
    Granule objects attempt to find both S3 and HTTPS URLs from the
    available OnlineAccessURLs. S3 URLs are preferred for performance
    when running within AWS.

    See Also
    --------
    :class:`Collection` : Dataset metadata results
    :meth:`maap.maap.MAAP.searchGranule` : Search for granules
    """

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
        """
        Get the OPeNDAP data access URL for this granule.

        OPeNDAP (Open-source Project for a Network Data Access Protocol)
        provides a way to access remote data subsets without downloading
        entire files.

        Returns
        -------
        str or None
            The OPeNDAP URL if available, or ``None`` if the granule
            does not support OPeNDAP access.

        Examples
        --------
        ::

            >>> opendap_url = granule.getOPeNDAPUrl()
            >>> if opendap_url:
            ...     # Use xarray or other tools to access data
            ...     import xarray as xr
            ...     ds = xr.open_dataset(opendap_url)

        Notes
        -----
        Not all granules have OPeNDAP URLs. This depends on whether the
        data provider has enabled OPeNDAP access for the dataset.

        See Also
        --------
        :meth:`getS3Url` : Get S3 download URL
        :meth:`getHttpUrl` : Get HTTP download URL
        """
        return self._OPeNDAPUrl
