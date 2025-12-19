# DPS (Data Processing System)

The DPS module provides classes for submitting and managing processing jobs.

## DPSJob

```{eval-rst}
.. autoclass:: maap.dps.dps_job.DPSJob
   :members:
   :special-members: __init__
```

## DpsHelper

```{eval-rst}
.. autoclass:: maap.dps.DpsHelper.DpsHelper
   :members:
   :special-members: __init__
```

## Example Usage

```python
from maap.maap import MAAP

maap = MAAP()

# Submit a job
job = maap.submitJob(
    identifier='my_analysis',
    algo_id='my_algorithm',
    version='main',
    queue='maap-dps-worker-8gb',
    input_file='s3://bucket/input.tif'
)

# Check status
print(f"Job ID: {job.id}")
print(f"Status: {job.status}")

# Wait for completion
job.wait_for_completion()

# Get results
if job.status == 'Succeeded':
    print(f"Outputs: {job.outputs}")
    print(f"Duration: {job.job_duration_seconds}s")

# Cancel a job
job.cancel_job()
```
