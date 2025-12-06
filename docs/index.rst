maap-py Documentation
======================

**maap-py** is the official Python client library for the NASA MAAP
(Multi-Mission Algorithm and Analysis Platform) API.

The library provides a simple and intuitive interface for:

* **Data Discovery**: Search for granules and collections in the CMR
  (Common Metadata Repository)
* **Data Access**: Download data files from S3 or HTTP endpoints
* **Algorithm Management**: Register, build, and manage algorithms on the DPS
* **Job Execution**: Submit and monitor processing jobs
* **User Management**: Access user profile, secrets, and AWS credentials

Quick Start
-----------

Installation
^^^^^^^^^^^^

Install maap-py using pip::

    pip install maap-py

Basic Usage
^^^^^^^^^^^

.. code-block:: python

    from maap.maap import MAAP

    # Initialize the client
    maap = MAAP()

    # Search for granules
    granules = maap.searchGranule(
        short_name='GEDI02_A',
        bounding_box='-122.5,37.5,-121.5,38.5',
        limit=10
    )

    # Download data
    for granule in granules:
        local_path = granule.getData(destpath='/tmp')
        print(f"Downloaded: {local_path}")

    # Submit a job
    job = maap.submitJob(
        identifier='my_analysis',
        algo_id='my_algorithm',
        version='main',
        queue='maap-dps-worker-8gb',
        input_file='s3://bucket/input.tif'
    )

    # Wait for completion
    job.wait_for_completion()
    print(f"Status: {job.status}")
    print(f"Outputs: {job.outputs}")

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart
   searching
   algorithms
   jobs

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/maap
   api/result
   api/dps
   api/aws
   api/profile
   api/secrets
   api/config

.. toctree::
   :maxdepth: 1
   :caption: Development

   contributing
   changelog


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
