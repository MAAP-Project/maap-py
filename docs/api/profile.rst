User Profile
============

.. automodule:: maap.Profile
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: maap.Profile.Profile
   :members:
   :special-members: __init__

Example Usage
-------------

.. code-block:: python

    from maap.maap import MAAP

    maap = MAAP()

    # Get user account information
    info = maap.profile.account_info()
    if info:
        print(f"User ID: {info['id']}")
        print(f"Username: {info['username']}")
