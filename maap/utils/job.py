# Valid job statuses (loosely based on OGC job status types)
JOB_STATUSES = {'Accepted', 'Running', 'Succeeded', 'Failed', 'Dismissed', 'Deduped', 'Offline'}

def validate_job_status(status):
    '''
    Validates job status

    Args:
        status (str): Job status. Accepted values are: 'Accepted', 'Running', 'Succeeded', 'Failed, 'Dismissed', 'Deduped', and 'Offline'.

    Returns:
        status (str): Returns unmodified job status if job status is valid.

    Raises: 
        ValueError: If invalid job status is provided.
    '''
    if status not in JOB_STATUSES:
        valid_statuses = ", ".join(str(status) for status in JOB_STATUSES)
        raise ValueError("Invalid job status: '{}'. Job status must be one of the following: {}".format(status, valid_statuses))
        
    return status