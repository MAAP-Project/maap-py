# User Secrets

```{eval-rst}
.. automodule:: maap.Secrets
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: maap.Secrets.Secrets
   :members:
   :special-members: __init__
```

## Example Usage

```python
from maap.maap import MAAP

maap = MAAP()

# List all secrets
secrets_list = maap.secrets.get_secrets()
for secret in secrets_list:
    print(f"Secret: {secret['secret_name']}")

# Get a specific secret
value = maap.secrets.get_secret('my_api_key')
print(f"Value: {value}")

# Add a new secret
maap.secrets.add_secret('my_new_key', 'my_secret_value')

# Delete a secret
maap.secrets.delete_secret('my_old_key')
```
