from collections.abc import Sequence
from typing import Literal, overload

from maap.exceptions import JobNotFoundError, NotFoundError
from maap.services.base import BaseService
from maap.types.jobs import (
    Job,
    JobCancellation,
    JobDetails,
    JobList,
    JobMetrics,
    JobRequest,
    JobResults,
    JobStatus,
    JobSummary,
)


class JobsService(BaseService):
    @overload
    def submit(self, request: JobRequest) -> Job: ...
    @overload
    def submit(self, request: Sequence[JobRequest]) -> list[Job]: ...

    def submit(self, request: JobRequest | Sequence[JobRequest]) -> Job | list[Job]:
        """Submit one or more jobs for execution.

        Args:
            request: A single JobRequest, or a sequence of JobRequest objects to
                submit as a batch. Each JobRequest contains an algorithm_id and inputs.

        Returns:
            A Job object if a single JobRequest was provided, or a list of Job objects
            (in the same order as the requests) if a sequence was provided.

        Examples:
            >>> request = JobRequest(algorithm_id=96, inputs={"SHORT_NAME": "test"})
            >>> job = maap.jobs.submit(request)
            >>> job.status
            'accepted'
            >>> job.job_id
            '168f3ceb-362f-4a2e-aa15-a0ec94da6a8b'

            >>> jobs = maap.jobs.submit([
            ...     JobRequest(algorithm_id=96, inputs={"SHORT_NAME": "a"}),
            ...     JobRequest(algorithm_id=96, inputs={"SHORT_NAME": "b"}),
            ... ])
            >>> [job.status for job in jobs]
            ['accepted', 'accepted']
        """
        if isinstance(request, JobRequest):
            return self._submit_one(request)
        return [self._submit_one(r) for r in request]

    def list(
        self,
        *,
        status: str | None = None,
        min_duration: float | None = None,
        max_duration: float | None = None,
        process_id: int | None = None,
        type: str | None = None,  # noqa: A002
        datetime: str | None = None,  # noqa: A002
        priority: int | None = None,
        queue: str | None = None,
        tag: str | None = None,
        fields: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[JobSummary]:
        """Retrieve the current user's jobs, optionally filtered.

        Args:
            status: Only include jobs with this status (e.g. accepted, running,
                successful, failed, dismissed, deduped, offline).
            min_duration: Only include jobs with a duration of at least this many seconds.
            max_duration: Only include jobs with a duration of at most this many seconds.
            process_id: Only include jobs for this process (algorithm) ID.
            type: Only include jobs of this type (currently only "process" is available).
            datetime: Only include jobs within this date-time or interval. Date and time
                expressions adhere to RFC 3339. Half-bounded intervals are expressed using
                double-dots (e.g. "2026-01-01T00:00:00Z/..").
            priority: Only include jobs with this priority.
            queue: Only include jobs run on this worker queue.
            tag: Only include jobs with this user-defined tag.
            fields: Additional job fields to include in each result, beyond the base
                fields (e.g. ["keywords", "job_queue"]). Requested fields are available
                as attributes on the returned JobSummary objects.
            limit: Maximum number of jobs to return.
            offset: Number of jobs to skip before returning results, for pagination.

        Returns:
            A list of JobSummary objects, one per job. If no filters are provided, all
            of the current user's jobs are returned.

        Examples:
            >>> maap.jobs.list()
            >>> maap.jobs.list(status="running")
            >>> maap.jobs.list(queue="maap-dps-worker-8gb", tag="my-run")
            >>> jobs = maap.jobs.list(fields=["keywords", "job_queue"])
            >>> jobs[0].job_queue
            'maap-dps-worker-8gb'
            >>> maap.jobs.list(limit=10, offset=20)
            >>> for job in maap.jobs.list():
            ...     print(job.job_id, job.status)
        """
        params = {
            "status": status,
            "minDuration": min_duration,
            "maxDuration": max_duration,
            "processID": process_id,
            "type": type,
            "datetime": datetime,
            "priority": priority,
            "queue": queue,
            "tag": tag,
            "limit": limit,
            "offset": offset,
            "fields": ",".join(fields) if fields else None,
        }
        params = {key: value for key, value in params.items() if value is not None}

        data = self._transport.get("/api/ogc/jobs", params=params or None)
        result = JobList.model_validate(data)
        return result.jobs

    @overload
    def get(self, *, job_id: str, get_job_details: Literal[False] = False) -> JobStatus: ...
    @overload
    def get(self, *, job_id: str, get_job_details: Literal[True]) -> JobDetails: ...

    def get(self, *, job_id: str, get_job_details: bool = False) -> JobStatus | JobDetails:
        """Retrieve a single job by its ID.

        Args:
            job_id: The ID of the job to retrieve.
            get_job_details: If True, retrieve full job details (title, timestamps,
                inputs, etc.) instead of just the job's status.

        Returns:
            A JobStatus object with the job's current status, or a JobDetails object
            if get_job_details is True.

        Raises:
            JobNotFoundError: If no job exists matching the given job_id.

        Examples:
            >>> job = maap.jobs.get(job_id="168f3ceb-362f-4a2e-aa15-a0ec94da6a8b")
            >>> job.status
            'successful'

            >>> details = maap.jobs.get(
            ...     job_id="168f3ceb-362f-4a2e-aa15-a0ec94da6a8b", get_job_details=True
            ... )
            >>> details.title
            'write-string-to-file'
        """
        params = {"getJobDetails": "true"} if get_job_details else None
        try:
            data = self._transport.get(f"/api/ogc/jobs/{job_id}", params=params)
        except NotFoundError as e:
            raise JobNotFoundError(job_id, e.error) from None
        if get_job_details:
            return JobDetails.model_validate(data)
        return JobStatus.model_validate(data)

    def get_metrics(self, *, job_id: str) -> JobMetrics:
        """Retrieve execution metrics for a single job by its ID.

        Args:
            job_id: The ID of the job whose metrics to retrieve.

        Returns:
            A JobMetrics object with the job's execution metrics.

        Raises:
            JobNotFoundError: If no job exists matching the given job_id.

        Examples:
            >>> metrics = maap.jobs.get_metrics(job_id="168f3ceb-362f-4a2e-aa15-a0ec94da6a8b")
            >>> metrics.job_duration_seconds
            4.029139
        """
        try:
            data = self._transport.get(f"/api/ogc/jobs/{job_id}/metrics")
        except NotFoundError as e:
            raise JobNotFoundError(job_id, e.error) from None
        return JobMetrics.model_validate(data)

    def get_results(self, *, job_id: str) -> JobResults:
        """Retrieve the results of a single job by its ID.

        Args:
            job_id: The ID of the job whose results to retrieve.

        Returns:
            A JobResults object with the job's output locations. For a failed job,
            the ``detail`` attribute holds the failure traceback.

        Raises:
            JobNotFoundError: If no job exists matching the given job_id.

        Examples:
            >>> results = maap.jobs.get_results(job_id="168f3ceb-362f-4a2e-aa15-a0ec94da6a8b")
            >>> for output in results.outputs.values():
            ...     for link in output.links:
            ...         print(link.href)
        """
        try:
            data = self._transport.get(f"/api/ogc/jobs/{job_id}/results")
        except NotFoundError as e:
            raise JobNotFoundError(job_id, e.error) from None
        return JobResults.model_validate(data)

    def cancel(self, *, job_id: str) -> JobCancellation:
        """Cancel (dismiss) a running job by its ID.

        Args:
            job_id: The ID of the job to cancel.

        Returns:
            A JobCancellation object reflecting the job's dismissed status.

        Raises:
            JobNotFoundError: If no job exists matching the given job_id.
            APIError: If the job cannot be cancelled (e.g. it has already completed).

        Examples:
            >>> result = maap.jobs.cancel(job_id="80f361d5-209a-4665-af0d-7229c7fc535c")
            >>> result.status
            'dismissed'
        """
        try:
            data = self._transport.delete(f"/api/ogc/jobs/{job_id}")
        except NotFoundError as e:
            raise JobNotFoundError(job_id, e.error) from None
        return JobCancellation.model_validate(data)

    def _submit_one(self, request: JobRequest) -> Job:
        payload: dict[str, object] = {"inputs": request.inputs}
        if request.queue is not None:
            payload["queue"] = request.queue
        if request.tag is not None:
            payload["tag"] = request.tag
        data = self._transport.post(
            f"/api/ogc/processes/{request.algorithm_id}/execution",
            json=payload,
        )
        return Job.model_validate(data)
