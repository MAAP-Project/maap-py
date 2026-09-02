from typing import Any

from pydantic import BaseModel, Field, model_validator

from maap.types.algorithms import Link


class JobRequest(BaseModel):
    """Request model for submitting a job."""

    model_config = {"use_attribute_docstrings": True}

    algorithm_id: str | int
    """The numeric ID of the algorithm (process) to run."""
    inputs: dict[str, str]
    """The input values to pass to the algorithm, keyed by input name."""
    queue: str | None = None
    """The name of the worker queue to run the job on."""
    tag: str | None = None
    """An optional user-defined tag to associate with the job."""


class JobStatus(BaseModel):
    """Status information for a job, as returned when retrieving a job by its ID."""

    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    job_id: str = Field(alias="jobID")
    """Unique identifier of the job."""
    process_id: int = Field(alias="processID")
    """Numeric identifier of the algorithm (process) the job runs."""
    type: str | None = None
    """The type of the job."""
    status: str
    """Current execution status of the job (e.g. accepted, running, successful, failed)."""


class JobSummary(BaseModel):
    """Summary of a job as returned in a job listing.

    Additional fields requested via the list() method's ``fields`` argument (e.g.
    ``keywords``, ``job_queue``) are not declared below since they're only present
    on demand, but are accessible as attributes on the returned object.
    """

    model_config = {
        "populate_by_name": True,
        "use_attribute_docstrings": True,
        "extra": "allow",
    }

    job_id: str = Field(alias="jobID")
    """Unique identifier of the job."""
    type: str | None = None
    """The type of the job."""
    status: str
    """Current execution status of the job (e.g. accepted, running, successful, dismissed)."""
    job_type: str | None = None
    """Identifier of the algorithm the job runs (e.g. 'job-<name>:<branch>')."""


class JobList(BaseModel):
    """Response model for a job listing."""

    model_config = {"use_attribute_docstrings": True}

    jobs: list[JobSummary]
    """The jobs matching the request."""
    links: list[Link] = []
    """Related hypermedia links for the listing."""


class JobCancellation(BaseModel):
    """Response model for a cancelled (dismissed) job."""

    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    job_id: str = Field(alias="jobID")
    """Unique identifier of the job that was cancelled."""
    type: str | None = None
    """The type of the job."""
    status: str
    """Execution status after cancellation (e.g. dismissed)."""


class Job(JobStatus):
    """Response model for a submitted job, extending JobStatus with full metadata."""

    title: str
    """Human-readable name of the algorithm the job runs."""
    description: str | None = None
    """Description of the job or its algorithm."""
    keywords: list[str] = []
    """Keywords/tags associated with the job."""
    metadata: list[str] = []
    """Additional metadata entries for the job."""
    request: str | None = None
    """The original request payload, if returned by the API."""
    message: str | None = None
    """Status or error message associated with the job."""
    created: str
    """Timestamp of when the job was created."""
    updated: str | None = None
    """Timestamp of the job's last status update."""
    links: list[Link] = []
    """Related hypermedia links, including a link to monitor the job."""


class JobInputValue(BaseModel):
    """A single input value a job was run with, as returned in job details."""

    model_config = {"use_attribute_docstrings": True}

    name: str
    """The name of the input."""
    value: str
    """The value provided for the input."""
    destination: str | None = None
    """How the input was passed to the algorithm (e.g. positional)."""


class JobDetails(JobStatus):
    """Detailed job information, returned when get_job_details=True."""

    title: str
    """Human-readable name of the algorithm the job runs."""
    description: str | None = None
    """Description of the job or its algorithm."""
    keywords: list[str] = []
    """Keywords/tags associated with the job."""
    request: str | None = None
    """The original request payload, if returned by the API."""
    message: str | None = None
    """Status or error message associated with the job."""
    created: str
    """Timestamp of when the job was created."""
    started: str | None = None
    """Timestamp of when the job started executing."""
    finished: str | None = None
    """Timestamp of when the job finished executing."""
    updated: str | None = None
    """Timestamp of the job's last status update."""
    progress: int | None = None
    """Execution progress of the job, if available."""
    tags: list[str] = []
    """User-defined tags associated with the job."""
    job_queue: str | None = None
    """The worker queue the job ran on."""
    process_name: str | None = None
    """Identifier of the algorithm the job runs (e.g. 'name:branch')."""
    products: str | None = None
    """URL to the job's output products directory."""
    links: list[Link] = []
    """Related hypermedia links, including a link to monitor the job."""
    inputs: list[JobInputValue] = []
    """The input values the job was run with."""


class JobMetrics(BaseModel):
    """Execution metrics for a job."""

    model_config = {"use_attribute_docstrings": True}

    machine_type: str | None = None
    """The type of machine the job ran on."""
    architecture: str | None = None
    """The CPU architecture of the machine the job ran on."""
    machine_memory_size: int | None = None
    """The total memory of the machine the job ran on, in bytes."""
    directory_size: int | None = None
    """The size of the job's working directory, in bytes."""
    operating_system: str | None = None
    """The operating system of the machine the job ran on."""
    job_start_time: str | None = None
    """Timestamp of when the job started executing."""
    job_end_time: str | None = None
    """Timestamp of when the job finished executing."""
    job_duration_seconds: float | None = None
    """Total execution time of the job, in seconds."""
    cpu_usage: Any = None
    """CPU usage statistics for the job, if available."""
    cache_usage: Any = None
    """Cache usage statistics for the job, if available."""
    mem_usage: Any = None
    """Memory usage statistics for the job, if available."""
    max_mem_usage: Any = None
    """Peak memory usage statistics for the job, if available."""
    swap_usage: Any = None
    """Swap usage statistics for the job, if available."""
    read_io_stats: Any = None
    """Read I/O statistics for the job, if available."""
    write_io_stats: Any = None
    """Write I/O statistics for the job, if available."""
    sync_io_stats: Any = None
    """Synchronous I/O statistics for the job, if available."""
    async_io_stats: Any = None
    """Asynchronous I/O statistics for the job, if available."""
    total_io_stats: Any = None
    """Total I/O statistics for the job, if available."""


class JobResultLink(BaseModel):
    """A link to a job result output location."""

    model_config = {"use_attribute_docstrings": True}

    href: str
    """The URL of the result output location (e.g. an S3, HTTP, or console URL)."""


class JobResultOutput(BaseModel):
    """A single named output produced by a job."""

    model_config = {"use_attribute_docstrings": True}

    id: str | None = None
    """Identifier of the output dataset."""
    links: list[JobResultLink] = []
    """Links to the locations where the output can be accessed."""


class JobResults(BaseModel):
    """Results of a completed job, including output locations and any failure detail."""

    model_config = {"use_attribute_docstrings": True}

    detail: str | None = None
    """Failure detail/traceback, present only when the job failed."""
    outputs: dict[str, JobResultOutput] = {}
    """The job's named outputs, keyed by output name."""

    @model_validator(mode="before")
    @classmethod
    def _split_detail_from_outputs(cls, data: Any) -> Any:
        """Separate the reserved ``detail`` key from the dynamically-named output entries."""
        if not isinstance(data, dict):
            return data
        remaining = dict(data)
        detail = remaining.pop("detail", None)
        return {"detail": detail, "outputs": remaining}
