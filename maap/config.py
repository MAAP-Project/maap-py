import os
from collections.abc import Callable

TokenProvider = str | Callable[[], str]

HOST_ENV_VAR = "MAAP_API_HOST"
"""The environment variable the API host is read from when none is given."""

TOKEN_ENV_VAR = "MAAP_TOKEN"
"""The environment variable the bearer token is read from when none is given."""


def resolve_token(token: TokenProvider | None) -> str | None:
    if token is None:
        return None
    if callable(token):
        return token()
    return token


def host_from_environment() -> str:
    host = os.environ.get(HOST_ENV_VAR)
    if not host:
        raise ValueError(f"{HOST_ENV_VAR} environment variable is not set")
    return host.rstrip("/")


def token_from_environment() -> str:
    token = os.environ.get(TOKEN_ENV_VAR)
    if not token:
        raise ValueError(f"{TOKEN_ENV_VAR} environment variable is not set")
    return token
