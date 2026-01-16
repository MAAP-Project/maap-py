"""
MAAP Python Client Library
==========================

**maap-py** is the official Python client for the NASA MAAP (Multi-Mission
Algorithm and Analysis Platform) API.

The library provides a comprehensive interface for:

* **Data Discovery**: Search for granules and collections in CMR
* **Data Access**: Download data files from S3 or HTTP endpoints
* **Algorithm Management**: Register, build, and manage algorithms
* **Job Execution**: Submit and monitor processing jobs on the DPS
* **User Management**: Access profile, secrets, and AWS credentials

Quick Start
-----------

Basic usage::

    from maap.maap import MAAP

    # Initialize the client
    maap = MAAP()

    # Search for granules
    granules = maap.search_granule(
        short_name='GEDI02_A',
        limit=10
    )

    # Download data
    for granule in granules:
        local_path = granule.getData(destpath='/tmp')

    # Submit a job
    job = maap.submit_job(
        identifier='my_job',
        algo_id='my_algorithm',
        version='main',
        queue='maap-dps-worker-8gb'
    )

    # Wait for completion
    job.wait_for_completion()

Main Classes
------------

:class:`~maap.maap.MAAP`
    Main client class for all MAAP operations.

:class:`~maap.Result.Granule`
    Represents a CMR granule (data file).

:class:`~maap.Result.Collection`
    Represents a CMR collection (dataset).

:class:`~maap.dps.dps_job.DPSJob`
    Represents a DPS processing job.

Submodules
----------

:mod:`maap.maap`
    Main MAAP client module.

:mod:`maap.Result`
    CMR search result classes.

:mod:`maap.dps`
    Data Processing System job management.

:mod:`maap.AWS`
    AWS credential management.

:mod:`maap.Profile`
    User profile management.

:mod:`maap.Secrets`
    User secrets management.

:mod:`maap.config_reader`
    Configuration management.

Environment Variables
---------------------

The following environment variables configure the client:

- ``MAAP_API_HOST``: MAAP API hostname
- ``MAAP_PGT``: Proxy granting ticket for authentication
- ``MAAP_AWS_ACCESS_KEY_ID``: AWS access key
- ``MAAP_AWS_SECRET_ACCESS_KEY``: AWS secret key

See Also
--------
- MAAP Documentation: https://docs.maap-project.org
- GitHub Repository: https://github.com/MAAP-Project/maap-py
"""

__version__ = "4.2.0"
__author__ = "NASA MAAP Project / Jet Propulsion Laboratory"
__license__ = "Apache-2.0"

# Import main classes for convenient access
from maap.maap import MAAP

__all__ = ["MAAP", "__version__"]
