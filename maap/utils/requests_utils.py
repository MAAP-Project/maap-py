import os
import threading
from typing import Optional, Union
from maap.config_reader import MaapConfig
import logging
import requests
from enum import Enum
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


# TODO: Replace with http.HTTPMethod once we upgrade to Python>3.11
class HTTPMethod(Enum):
    POST = 'POST'
    GET = 'GET'
    PUT = 'PUT'
    DELETE = 'DELETE'


POST = HTTPMethod.POST
GET = HTTPMethod.GET
PUT = HTTPMethod.PUT
DELETE = HTTPMethod.DELETE


# Default (connect, read) timeout in seconds applied to every request unless overridden.
DEFAULT_TIMEOUT = (10, 60)

RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
IDEMPOTENT_METHODS = frozenset({'GET', 'HEAD', 'OPTIONS', 'PUT', 'DELETE'})
RETRY_TOTAL = 3
RETRY_BACKOFF_FACTOR = 0.5

_REDACTED_HEADERS = {'token', 'authorization', 'proxy-ticket'}

_sessions = {}
_sessions_lock = threading.Lock()


def _build_retry(allowed_methods) -> Retry:
    return Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_CODES,
        allowed_methods=allowed_methods,
        respect_retry_after_header=True,
        # Hand the last response back to the caller instead of raising RetryError
        raise_on_status=False,
    )


def _build_session(retry_post: bool) -> requests.Session:
    methods = IDEMPOTENT_METHODS | {'POST'} if retry_post else IDEMPOTENT_METHODS
    adapter = HTTPAdapter(max_retries=_build_retry(methods))
    session = requests.Session()
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


def get_session(retry_post: bool = False) -> requests.Session:
    """
    Return the shared, lazily created ``requests.Session`` used by maap-py.

    The session pools connections and retries transient failures (connection
    errors and HTTP 429/500/502/503/504) with exponential backoff, honouring
    ``Retry-After``. Only idempotent methods (GET, HEAD, OPTIONS, PUT, DELETE)
    are retried. A second session that also retries POST is available with
    ``retry_post=True`` for endpoints known to be safe to repeat.
    """
    with _sessions_lock:
        if retry_post not in _sessions:
            _sessions[retry_post] = _build_session(retry_post)
        return _sessions[retry_post]


def reset_sessions():
    """Close and discard the shared sessions (mainly useful for tests)."""
    with _sessions_lock:
        for session in _sessions.values():
            session.close()
        _sessions.clear()


def generate_dps_headers(config: MaapConfig, content_type=None):
    api_header = {
        'Accept': config.content_type,
    }
    if content_type:
        api_header['Content-Type'] = content_type
    if config.maap_token.lower().startswith('basic') or config.maap_token.lower().startswith('bearer'):
        api_header['Authorization'] = config.maap_token
    else:
        api_header['token'] = config.maap_token

    if os.environ.get("MAAP_PGT"):
        api_header['proxy-ticket'] = os.environ.get("MAAP_PGT")
    return api_header


def _redact(headers: dict) -> dict:
    return {k: ('***' if k.lower() in _REDACTED_HEADERS else v) for k, v in headers.items()}


def check_response(dps_response):
    # if dps_response.status_code not in [200, 201]:
    #     raise RuntimeError('response is not 200 or 201. code: {}. details: {}'.format(dps_response.status_code,
    #                                                                                   dps_response.content))
    return dps_response.content.decode('UTF-8')


def request(method: Union[HTTPMethod, str], url, config: Optional[MaapConfig] = None, content_type=None,
            auth: bool = True, headers: Optional[dict] = None, timeout=DEFAULT_TIMEOUT,
            retry_post: bool = False, self_signed: bool = False, **kwargs) -> requests.Response:
    """
    Send an HTTP request through the shared retrying session.

    Parameters
    ----------
    method : HTTPMethod or str
        HTTP method.
    url : str
        Request URL.
    config : MaapConfig, optional
        Source of the token and default content type. Required when ``auth`` is True.
        The token is read on every call, so a refreshed token is picked up automatically.
    content_type : str, optional
        Content-Type header to send.
    auth : bool
        If True, MAAP auth headers (token/Authorization, MAAP_PGT) are added. Set to False
        for third-party hosts (e.g. DAAC downloads) so MAAP credentials are never sent.
    headers : dict, optional
        Extra headers; these take precedence over the generated ones. Never mutated.
    timeout : float or tuple
        ``requests`` timeout; defaults to ``DEFAULT_TIMEOUT``. Pass ``None`` to disable.
    retry_post : bool
        Also retry POST requests. Off by default as POSTs (e.g. job submission) may not be idempotent.
    self_signed : bool
        Disable TLS verification.
    **kwargs
        Passed through to ``requests.Session.request`` (``params``, ``json``, ``data``, ``stream``...).
    """
    method_name = method.value if isinstance(method, HTTPMethod) else str(method).upper()
    if auth:
        if config is None:
            raise ValueError("config is required when auth=True")
        merged = generate_dps_headers(config, content_type)
    else:
        merged = {'Content-Type': content_type} if content_type else {}
    if headers:
        merged.update(headers)

    logger.debug(f"{method_name} request sent to {url}")
    logger.debug(f"headers: {_redact(merged)}")
    return get_session(retry_post).request(
        method=method_name,
        url=url,
        headers=merged,
        timeout=timeout,
        verify=not self_signed,
        **kwargs
    )


def make_request(url, config: MaapConfig, content_type=None, request_type: HTTPMethod = HTTPMethod.GET,
                 self_signed=False, **kwargs):
    return request(request_type, url, config, content_type=content_type, self_signed=self_signed, **kwargs)


def make_dps_request(url, config: MaapConfig, content_type=None, request_type: HTTPMethod = HTTPMethod.GET,
                     self_signed=False, **kwargs):
    return check_response(make_request(url, config, content_type, request_type, self_signed, **kwargs))
