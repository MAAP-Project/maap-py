from __future__ import annotations

from typing import TYPE_CHECKING

from maap.exceptions.base import MAAPError

if TYPE_CHECKING:
    from maap.exceptions.base import APIErrorResponse


class JobNotFoundError(MAAPError):
    def __init__(self, job_id: str, error: APIErrorResponse | None = None) -> None:
        self.job_id = job_id
        self.error = error
        detail = error.detail if error else f"No job found matching: {job_id}"
        super().__init__(f"{detail} (job_id={job_id})")


class JobPermissionError(MAAPError):
    def __init__(self, job_id: str, error: APIErrorResponse | None = None) -> None:
        self.job_id = job_id
        self.error = error
        detail = error.detail if error else "You can only modify jobs that you submitted originally"
        super().__init__(f"{detail} (job_id={job_id})")
