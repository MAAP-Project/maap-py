# Result Classes

```{eval-rst}
.. automodule:: maap.Result
   :members:
   :undoc-members:
   :show-inheritance:
```

The Result module provides classes for handling CMR search results.

## Classes

### Result

Base class for CMR results.

```{eval-rst}
.. autoclass:: maap.Result.Result
   :members:
   :special-members: __init__
```

### Granule

Represents a CMR granule (individual data file).

```{eval-rst}
.. autoclass:: maap.Result.Granule
   :members:
   :special-members: __init__
```

### Collection

Represents a CMR collection (dataset).

```{eval-rst}
.. autoclass:: maap.Result.Collection
   :members:
   :special-members: __init__
```

## Example Usage

```python
from maap.maap import MAAP

maap = MAAP()

# Search granules
granules = maap.search_granule(short_name='GEDI02_A', limit=5)

for granule in granules:
    # Get URLs
    s3_url = granule.getS3Url()
    http_url = granule.getHttpUrl()

    # Download
    local_path = granule.getData(destpath='/tmp')

    # Get description
    print(granule.getDescription())
```
