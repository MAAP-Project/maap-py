from __future__ import annotations

from typing import TYPE_CHECKING

from maap.exceptions.base import MAAPError

if TYPE_CHECKING:
    from maap.exceptions.base import APIErrorResponse


class SecretNotFoundError(MAAPError):
    def __init__(self, secret_name: str, error: APIErrorResponse | None = None) -> None:
        self.secret_name = secret_name
        self.error = error
        detail = error.detail if error else f"No secret found matching: {secret_name}"
        super().__init__(f"{detail} (secret_name={secret_name})")


class SecretAlreadyExistsError(MAAPError):
    def __init__(self, secret_name: str, error: APIErrorResponse | None = None) -> None:
        self.secret_name = secret_name
        self.error = error
        detail = (
            error.detail
            if error
            else (
                f"Secret already exists with name `{secret_name}`. Delete and re-create the "
                "secret to update its value."
            )
        )
        super().__init__(f"{detail} (secret_name={secret_name})")
