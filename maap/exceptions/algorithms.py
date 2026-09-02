from __future__ import annotations

from typing import TYPE_CHECKING

from maap.exceptions.base import MAAPError

if TYPE_CHECKING:
    from maap.exceptions.base import APIErrorResponse


class AlgorithmNotFoundError(MAAPError):
    def __init__(self, algorithm_id: str | int, error: APIErrorResponse | None = None) -> None:
        self.algorithm_id = algorithm_id
        self.error = error
        detail = error.detail if error else f"No algorithm found matching: {algorithm_id}"
        super().__init__(f"{detail} (algorithm_id={algorithm_id})")


class AlgorithmPermissionError(MAAPError):
    def __init__(self, algorithm_id: str | int, error: APIErrorResponse | None = None) -> None:
        self.algorithm_id = algorithm_id
        self.error = error
        detail = (
            error.detail if error else "You can only modify processes that you posted originally"
        )
        super().__init__(f"{detail} (algorithm_id={algorithm_id})")
