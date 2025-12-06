AWS Credentials
===============

.. automodule:: maap.AWS
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: maap.AWS.AWS
   :members:
   :special-members: __init__

Example Usage
-------------

.. code-block:: python

    from maap.maap import MAAP

    maap = MAAP()

    # Get requester-pays credentials
    creds = maap.aws.requester_pays_credentials()
    print(f"Access Key: {creds['accessKeyId']}")

    # Generate a signed URL
    signed = maap.aws.s3_signed_url('bucket', 'path/to/file.h5')
    print(f"Signed URL: {signed['url']}")

    # Get Earthdata DAAC credentials
    daac_creds = maap.aws.earthdata_s3_credentials(
        'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials'
    )

    # Get workspace bucket credentials
    workspace_creds = maap.aws.workspace_bucket_credentials()
