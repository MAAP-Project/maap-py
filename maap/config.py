import os
from collections.abc import Callable

TokenProvider = str | Callable[[], str]


def resolve_token(token: TokenProvider | None) -> str | None:
    if token is None:
        return None
    if callable(token):
        return token()
    return token


def host_from_environment() -> str:
    host = os.environ.get("MAAP_API_HOST")
    if not host:
        raise ValueError("MAAP_API_HOST environment variable is not set")
    return host.rstrip("/")


def token_from_environment() -> str:
    token = os.environ.get("MAAP_TOKEN")
    if not token:
        raise ValueError("MAAP_TOKEN environment variable is not set")
    return token
