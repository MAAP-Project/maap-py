# Configuration

```{eval-rst}
.. autoclass:: maap.config_reader.MaapConfig
   :members:
   :special-members: __init__
```

## Environment Variables

The following environment variables can be used to configure the MAAP client:

| Variable | Description |
|----------|-------------|
| `MAAP_API_HOST` | Override the default MAAP API hostname |
| `MAAP_API_HOST_SCHEME` | URL scheme (http/https) for API connections |
| `MAAP_CMR_PAGE_SIZE` | Number of results per page for CMR queries |
| `MAAP_CMR_CONTENT_TYPE` | Content type for CMR requests |
| `MAAP_PGT` | Proxy granting ticket for authentication |
| `MAAP_AWS_ACCESS_KEY_ID` | AWS access key for S3 operations |
| `MAAP_AWS_SECRET_ACCESS_KEY` | AWS secret key for S3 operations |
| `MAAP_S3_USER_UPLOAD_BUCKET` | S3 bucket for user file uploads |
| `MAAP_S3_USER_UPLOAD_DIR` | S3 directory for user file uploads |
| `MAAP_MAPBOX_ACCESS_TOKEN` | Mapbox token for visualization |

## Example Usage

```python
from maap.maap import MAAP

maap = MAAP()

# Access configuration
print(f"API Root: {maap.config.maap_api_root}")
print(f"Page Size: {maap.config.page_size}")
print(f"Granule Search URL: {maap.config.search_granule_url}")
```
