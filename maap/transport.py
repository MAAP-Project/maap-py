from typing import Any

import requests

from maap.config import TokenProvider, resolve_token
from maap.exceptions import APIError, AuthenticationError, NotFoundError
from maap.exceptions.base import ForbiddenError


class Transport:
    def __init__(self, host: str, token: TokenProvider | None) -> None:
        self._host = host
        self._token = token
        self._session = requests.Session()

    def _headers(self, authenticated: bool = True) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        if authenticated:
            token = resolve_token(self._token)
            if token:
                headers["Authorization"] = f"Bearer {token}"
        return headers

    def _build_url(self, path: str) -> str:
        return f"{self._host}/{path.lstrip('/')}"

    def _handle_response(self, response: requests.Response) -> Any:
        if response.status_code == 401:
            raise AuthenticationError(response)
        if response.status_code == 403:
            raise ForbiddenError(response)
        if response.status_code == 404:
            raise NotFoundError(response)
        if not response.ok:
            raise APIError(response)
        return response.json()

    def get(
        self, path: str, params: dict[str, Any] | None = None, authenticated: bool = True
    ) -> Any:
        response = self._session.get(
            self._build_url(path),
            headers=self._headers(authenticated),
            params=params,
        )
        return self._handle_response(response)

    def post(
        self, path: str, json: dict[str, Any] | None = None, authenticated: bool = True
    ) -> Any:
        response = self._session.post(
            self._build_url(path),
            headers=self._headers(authenticated),
            json=json,
        )
        return self._handle_response(response)

    def put(self, path: str, json: dict[str, Any] | None = None, authenticated: bool = True) -> Any:
        response = self._session.put(
            self._build_url(path),
            headers=self._headers(authenticated),
            json=json,
        )
        return self._handle_response(response)

    def delete(self, path: str, authenticated: bool = True) -> Any:
        response = self._session.delete(
            self._build_url(path),
            headers=self._headers(authenticated),
        )
        return self._handle_response(response)
