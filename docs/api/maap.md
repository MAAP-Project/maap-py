# MAAP Client

```{eval-rst}
.. automodule:: maap.maap
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
```

The {class}`~maap.maap.MAAP` class is the main entry point for all MAAP operations.

## Example Usage

```python
from maap.maap import MAAP

# Initialize
maap = MAAP()

# Search granules
granules = maap.searchGranule(short_name='GEDI02_A', limit=10)

# Search collections
collections = maap.searchCollection(provider='MAAP')

# Submit a job
job = maap.submitJob(
    identifier='analysis',
    algo_id='my_algo',
    version='main',
    queue='maap-dps-worker-8gb'
)
```
