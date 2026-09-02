import pytest
import responses

from maap import MAAP
from maap.exceptions import APIError, JobNotFoundError
from maap.types.jobs import (
    Job,
    JobCancellation,
    JobDetails,
    JobMetrics,
    JobRequest,
    JobResults,
    JobStatus,
    JobSummary,
)

JOB_SUBMISSION_RESPONSE = {
    "title": "operawatermask1",
    "description": "None",
    "keywords": ["null"],
    "metadata": [],
    "jobID": "168f3ceb-362f-4a2e-aa15-a0ec94da6a8b",
    "processID": 96,
    "type": None,
    "request": None,
    "status": "accepted",
    "message": None,
    "created": "2026-07-11T00:43:54.165873",
    "updated": None,
    "links": [
        {
            "href": "/ogc/processes/96/execution",
            "rel": "self",
            "type": "application/json",
            "hreflang": "en",
            "title": "Process Execution",
        },
        {
            "href": "/ogc/jobs/168f3ceb-362f-4a2e-aa15-a0ec94da6a8b",
            "rel": "monitor",
            "type": "application/json",
            "hreflang": "en",
            "title": "Job",
        },
    ],
}


def test_submit_job(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes/96/execution",
        json=JOB_SUBMISSION_RESPONSE,
    )

    request = JobRequest(algorithm_id=96, inputs={"SHORT_NAME": "test"})
    job = client.jobs.submit(request)

    assert isinstance(job, Job)
    assert job.job_id == "168f3ceb-362f-4a2e-aa15-a0ec94da6a8b"
    assert job.process_id == 96
    assert job.status == "accepted"
    assert len(job.links) == 2


def test_submit_job_with_queue_and_tag(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes/96/execution",
        json=JOB_SUBMISSION_RESPONSE,
        match=[
            responses.matchers.json_params_matcher(
                {
                    "inputs": {"SHORT_NAME": "test"},
                    "queue": "maap-dps-worker-8gb",
                    "tag": "my-run",
                }
            )
        ],
    )

    request = JobRequest(
        algorithm_id=96,
        inputs={"SHORT_NAME": "test"},
        queue="maap-dps-worker-8gb",
        tag="my-run",
    )
    job = client.jobs.submit(request)

    assert isinstance(job, Job)
    assert job.job_id == "168f3ceb-362f-4a2e-aa15-a0ec94da6a8b"


def test_submit_job_omits_unset_queue_and_tag(
    mock_api: responses.RequestsMock, client: MAAP
) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes/96/execution",
        json=JOB_SUBMISSION_RESPONSE,
        match=[responses.matchers.json_params_matcher({"inputs": {"SHORT_NAME": "test"}})],
    )

    request = JobRequest(algorithm_id=96, inputs={"SHORT_NAME": "test"})
    job = client.jobs.submit(request)

    assert isinstance(job, Job)


def test_list_jobs(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs",
        json={
            "status": 200,
            "jobs": [
                {
                    "jobID": "80f361d5-209a-4665-af0d-7229c7fc535c",
                    "type": "process",
                    "status": "dismissed",
                    "job_type": "job-write-string-to-file_18:main",
                },
                {
                    "jobID": "d6145449-96fc-423f-8269-44e9e645df1c",
                    "type": "process",
                    "status": "dismissed",
                    "job_type": "job-mlucas-dps-tutorial:main",
                },
            ],
            "links": [
                {
                    "href": "/ogc/job/80f361d5-209a-4665-af0d-7229c7fc535c",
                    "rel": "self",
                    "type": "application/json",
                    "hreflang": "en",
                    "title": "Job",
                },
            ],
        },
    )

    jobs = client.jobs.list()

    assert isinstance(jobs, list)
    assert len(jobs) == 2
    assert all(isinstance(job, JobSummary) for job in jobs)
    assert jobs[0].job_id == "80f361d5-209a-4665-af0d-7229c7fc535c"
    assert jobs[0].status == "dismissed"
    assert jobs[0].job_type == "job-write-string-to-file_18:main"
    assert jobs[1].job_id == "d6145449-96fc-423f-8269-44e9e645df1c"
    assert mock_api.calls[0].request.method == "GET"
    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_list_jobs_with_filters(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs",
        json={"status": 200, "jobs": [], "links": []},
        match=[
            responses.matchers.query_param_matcher(
                {
                    "status": "running",
                    "minDuration": "1.5",
                    "maxDuration": "60",
                    "processID": "25",
                    "type": "process",
                    "datetime": "2026-01-01T00:00:00Z/..",
                    "priority": "5",
                    "queue": "maap-dps-worker-8gb",
                    "tag": "my-run",
                    "limit": "10",
                    "offset": "20",
                }
            )
        ],
    )

    jobs = client.jobs.list(
        status="running",
        min_duration=1.5,
        max_duration=60,
        process_id=25,
        type="process",
        datetime="2026-01-01T00:00:00Z/..",
        priority=5,
        queue="maap-dps-worker-8gb",
        tag="my-run",
        limit=10,
        offset=20,
    )

    assert jobs == []


def test_list_jobs_omits_unset_filters(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs",
        json={"status": 200, "jobs": [], "links": []},
        match=[responses.matchers.query_param_matcher({})],
    )

    jobs = client.jobs.list()

    assert jobs == []


def test_list_jobs_with_fields(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs",
        json={
            "status": 200,
            "jobs": [
                {
                    "jobID": "80f361d5-209a-4665-af0d-7229c7fc535c",
                    "type": "process",
                    "status": "dismissed",
                    "job_type": "job-write-string-to-file_18:main",
                    "keywords": ["ogc"],
                    "job_queue": "maap-dps-worker-8gb",
                }
            ],
            "links": [],
        },
        match=[
            responses.matchers.query_param_matcher({"fields": "keywords,job_queue"})
        ],
    )

    jobs = client.jobs.list(fields=["keywords", "job_queue"])

    assert len(jobs) == 1
    assert jobs[0].keywords == ["ogc"]
    assert jobs[0].job_queue == "maap-dps-worker-8gb"


def test_cancel_job(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/ogc/jobs/80f361d5-209a-4665-af0d-7229c7fc535c",
        json={
            "jobID": "80f361d5-209a-4665-af0d-7229c7fc535c",
            "type": "process",
            "status": "dismissed",
        },
    )

    result = client.jobs.cancel(job_id="80f361d5-209a-4665-af0d-7229c7fc535c")

    assert isinstance(result, JobCancellation)
    assert result.job_id == "80f361d5-209a-4665-af0d-7229c7fc535c"
    assert result.type == "process"
    assert result.status == "dismissed"
    assert mock_api.calls[0].request.method == "DELETE"
    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_cancel_job_not_allowed(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/ogc/jobs/80f361d5-209a-4665-af0d-7229c7fc535c",
        json={
            "type": None,
            "title": "Not allowed to cancel job with status job-completed",
            "status": 400,
            "detail": "Not allowed to cancel job with status job-completed",
            "instance": "",
        },
        status=400,
    )

    with pytest.raises(APIError) as exc_info:
        client.jobs.cancel(job_id="80f361d5-209a-4665-af0d-7229c7fc535c")

    assert exc_info.value.status_code == 400
    assert not isinstance(exc_info.value, JobNotFoundError)


def test_cancel_job_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/ogc/jobs/does-not-exist",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
            "title": "No job with that job ID found",
            "status": 404,
            "detail": "No job with that job ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(JobNotFoundError) as exc_info:
        client.jobs.cancel(job_id="does-not-exist")

    assert exc_info.value.job_id == "does-not-exist"


def test_get_job(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/250404f4-63c0-4831-9598-00f5656bc208",
        json={
            "jobID": "250404f4-63c0-4831-9598-00f5656bc208",
            "processID": 25,
            "type": None,
            "status": "successful",
        },
    )

    job = client.jobs.get(job_id="250404f4-63c0-4831-9598-00f5656bc208")

    assert isinstance(job, JobStatus)
    assert job.job_id == "250404f4-63c0-4831-9598-00f5656bc208"
    assert job.process_id == 25
    assert job.status == "successful"
    assert job.type is None
    assert mock_api.calls[0].request.method == "GET"


def test_get_job_details(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/250404f4-63c0-4831-9598-00f5656bc208",
        json={
            "jobID": "250404f4-63c0-4831-9598-00f5656bc208",
            "processID": 25,
            "type": None,
            "status": "successful",
            "title": "write-string-to-file",
            "description": "Writes an input string to a text file.",
            "keywords": ["ogc"],
            "request": None,
            "message": None,
            "created": "2026-09-01T20:28:31.889400Z",
            "started": "2026-09-01T20:30:03.633477Z",
            "finished": "2026-09-01T20:30:38.792016Z",
            "updated": None,
            "progress": None,
            "tags": ["write-string-to-file:main"],
            "job_queue": "maap-dps-worker-8gb",
            "process_name": "write-string-to-file:main",
            "products": (
                "http://maap-ops-workspace.s3-website-us-west-2.amazonaws.com/mlucas/"
                "dps_output/write-string-to-file_18/main/2026/09/01/20/30/38/184165"
            ),
            "links": [
                {
                    "href": "/ogc/jobs/250404f4-63c0-4831-9598-00f5656bc208",
                    "rel": "self",
                    "type": "application/json",
                    "hreflang": "en",
                    "title": "Job Status",
                }
            ],
            "inputs": [
                {"name": "text", "value": "Hello, world!", "destination": "positional"},
                {"name": "output_file", "value": "output.txt", "destination": "positional"},
            ],
        },
        match=[responses.matchers.query_param_matcher({"getJobDetails": "true"})],
    )

    details = client.jobs.get(
        job_id="250404f4-63c0-4831-9598-00f5656bc208", get_job_details=True
    )

    assert isinstance(details, JobDetails)
    assert details.job_id == "250404f4-63c0-4831-9598-00f5656bc208"
    assert details.title == "write-string-to-file"
    assert details.job_queue == "maap-dps-worker-8gb"
    assert details.tags == ["write-string-to-file:main"]
    assert len(details.inputs) == 2
    assert details.inputs[0].name == "text"
    assert details.inputs[0].value == "Hello, world!"
    assert details.inputs[0].destination == "positional"
    assert len(details.links) == 1
    assert details.links[0].rel == "self"


def test_get_job_metrics(mock_api: responses.RequestsMock, client: MAAP) -> None:
    metrics_response = {
        "machine_type": None,
        "architecture": None,
        "machine_memory_size": None,
        "directory_size": 26107,
        "operating_system": None,
        "job_start_time": "2026-09-01T16:41:36.645602Z",
        "job_end_time": "2026-09-01T16:41:40.674741Z",
        "job_duration_seconds": 4.029139,
        "cpu_usage": {"total": 1200, "system": 300},
        "mem_usage": 524288,
        "total_io_stats": "1.2MB",
    }
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/168f3ceb-362f-4a2e-aa15-a0ec94da6a8b/metrics",
        json=metrics_response,
    )

    metrics = client.jobs.get_metrics(job_id="168f3ceb-362f-4a2e-aa15-a0ec94da6a8b")

    assert isinstance(metrics, JobMetrics)
    assert metrics.directory_size == 26107
    assert metrics.job_duration_seconds == 4.029139
    assert metrics.job_start_time == "2026-09-01T16:41:36.645602Z"
    assert metrics.machine_type is None
    assert metrics.cpu_usage == {"total": 1200, "system": 300}
    assert metrics.mem_usage == 524288
    assert metrics.total_io_stats == "1.2MB"
    assert metrics.swap_usage is None
    assert mock_api.calls[0].request.method == "GET"
    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_get_job_metrics_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/does-not-exist/metrics",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
            "title": "No job with that job ID found",
            "status": 404,
            "detail": "No job with that job ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(JobNotFoundError) as exc_info:
        client.jobs.get_metrics(job_id="does-not-exist")

    assert exc_info.value.job_id == "does-not-exist"


def test_get_job_results_succeeded(mock_api: responses.RequestsMock, client: MAAP) -> None:
    results_response = {
        "additionalProp1": {
            "links": [
                {"href": "http://example.com/output"},
                {"href": "s3://bucket/output"},
                {"href": "https://console.example.com/output"},
            ],
            "id": "output-2026-09-01T19:39:16.629699",
        }
    }
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/168f3ceb-362f-4a2e-aa15-a0ec94da6a8b/results",
        json=results_response,
    )

    results = client.jobs.get_results(job_id="168f3ceb-362f-4a2e-aa15-a0ec94da6a8b")

    assert isinstance(results, JobResults)
    assert results.detail is None
    assert set(results.outputs) == {"additionalProp1"}
    output = results.outputs["additionalProp1"]
    assert output.id == "output-2026-09-01T19:39:16.629699"
    assert len(output.links) == 3
    assert output.links[0].href == "http://example.com/output"
    assert mock_api.calls[0].request.method == "GET"
    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_get_job_results_failed(mock_api: responses.RequestsMock, client: MAAP) -> None:
    results_response = {
        "detail": "Job failed and traceback is ...ModuleNotFoundError: No module named 'numpy'...",
        "additionalProp1": {
            "links": [
                {"href": "http://example.com/triaged"},
                {"href": "s3://bucket/triaged"},
                {"href": "https://console.example.com/triaged"},
            ],
            "id": "triaged_job-abc-task-5a24793d",
        },
    }
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/168f3ceb-362f-4a2e-aa15-a0ec94da6a8b/results",
        json=results_response,
    )

    results = client.jobs.get_results(job_id="168f3ceb-362f-4a2e-aa15-a0ec94da6a8b")

    assert isinstance(results, JobResults)
    assert results.detail is not None
    assert "ModuleNotFoundError" in results.detail
    assert results.outputs["additionalProp1"].id == "triaged_job-abc-task-5a24793d"


def test_get_job_results_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/does-not-exist/results",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
            "title": "No job with that job ID found",
            "status": 404,
            "detail": "No job with that job ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(JobNotFoundError) as exc_info:
        client.jobs.get_results(job_id="does-not-exist")

    assert exc_info.value.job_id == "does-not-exist"


def test_get_job_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/jobs/does-not-exist",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-job",
            "title": "No job with that job ID found",
            "status": 404,
            "detail": "No job with that job ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(JobNotFoundError) as exc_info:
        client.jobs.get(job_id="does-not-exist")

    assert exc_info.value.job_id == "does-not-exist"


def test_submit_jobs_batch(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes/96/execution",
        json=JOB_SUBMISSION_RESPONSE,
    )
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes/96/execution",
        json=JOB_SUBMISSION_RESPONSE | {"jobID": "second-job-id"},
    )

    requests = [
        JobRequest(algorithm_id=96, inputs={"SHORT_NAME": "a"}),
        JobRequest(algorithm_id=96, inputs={"SHORT_NAME": "b"}),
    ]
    jobs = client.jobs.submit(requests)

    assert isinstance(jobs, list)
    assert len(jobs) == 2
    assert all(isinstance(job, Job) for job in jobs)
    assert jobs[0].job_id == "168f3ceb-362f-4a2e-aa15-a0ec94da6a8b"
    assert jobs[1].job_id == "second-job-id"
    assert len(mock_api.calls) == 2
