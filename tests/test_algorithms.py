import json

import pytest
import responses

from maap import MAAP
from maap.exceptions import AlgorithmNotFoundError, AlgorithmPermissionError
from maap.types.algorithms import Algorithm, AlgorithmDeployment

ALGORITHM_RESPONSE = {
    "title": "operawatermask1",
    "description": "None",
    "keywords": ["null"],
    "metadata": [],
    "id": "operawatermask1",
    "processID": "96",
    "version": "ogc",
    "jobControlOptions": [],
    "author": "OPERA",
    "deployedBy": "mlucas",
    "githubUrl": "https://github.com/MAAP-Project/OPERA_DPS_JOB.git",
    "gitCommitHash": "4405d394d19a375c9d2b5dc0211714f2c01f5f1f",
    "cwlLink": "https://raw.githubusercontent.com/MAAP-Project/OPERA_DPS_JOB/main/process.cwl",
    "ramMin": 10,
    "coresMin": 1,
    "baseCommand": "/OPERA_DPS_JOB/run.sh",
    "links": [
        {
            "href": "/ogc/processes/96",
            "rel": "self",
            "type": "application/json",
            "hreflang": "en",
            "title": "OGC Process Description",
        }
    ],
    "inputs": {
        "SHORT_NAME": {
            "name": "SHORT_NAME",
            "from": "submitter",
            "placeholder": "SHORT_NAME",
            "description": "SHORT_NAME",
            "type": "text",
        }
    },
}

LIST_RESPONSE = {
    "processes": [
        {
            "title": "gedi-subset",
            "description": "Subset GEDI granules within an AOI.",
            "keywords": ["OGC", "GEDI"],
            "metadata": [],
            "processID": "8",
            "id": "gedi-subset",
            "version": "0.13.0",
            "jobControlOptions": [],
            "author": "MAAP",
            "deployedBy": "dschuck",
            "lastModifiedTime": "2026-04-14T22:36:34.330601",
            "cwlLink": "https://repo.uat.maap-project.org/process.cwl",
            "links": [],
        },
        {
            "title": "test_alg",
            "description": "test",
            "keywords": [],
            "metadata": [],
            "processID": "71",
            "id": "test_alg",
            "version": "main",
            "jobControlOptions": [],
            "author": "null",
            "deployedBy": "grace.llewellyn",
            "lastModifiedTime": "2026-04-17T04:13:39.030229",
            "cwlLink": "https://repo.uat.maap-project.org/process.cwl",
            "links": [],
        },
    ],
    "links": [],
}


def test_get_algorithm(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes/96",
        json=ALGORITHM_RESPONSE,
    )

    algorithm = client.algorithms.get(algorithm_id="96")

    assert isinstance(algorithm, Algorithm)
    assert algorithm.process_id == 96
    assert algorithm.id == "operawatermask1"
    assert algorithm.title == "operawatermask1"
    assert algorithm.author == "OPERA"
    assert algorithm.ram_min == 10
    assert "SHORT_NAME" in algorithm.inputs


def test_get_algorithm_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes/999",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
            "title": "No process with that process ID found",
            "status": 404,
            "detail": "No process with that process ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(AlgorithmNotFoundError) as exc_info:
        client.algorithms.get(algorithm_id="999")

    assert exc_info.value.algorithm_id == "999"


def test_get_algorithm_by_name_version_deployer(
    mock_api: responses.RequestsMock, client: MAAP
) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=LIST_RESPONSE,
    )
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes/8",
        json=ALGORITHM_RESPONSE | {"processID": "8", "title": "gedi-subset"},
    )

    algorithm = client.algorithms.get(name="gedi-subset", version="0.13.0", deployer="dschuck")

    assert isinstance(algorithm, Algorithm)
    assert algorithm.process_id == 8


def test_get_algorithm_by_name_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=LIST_RESPONSE,
    )

    with pytest.raises(AlgorithmNotFoundError):
        client.algorithms.get(name="nonexistent", version="1.0", deployer="nobody")


def test_delete_algorithm(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/ogc/processes/96",
        json={},
    )

    client.algorithms.delete(algorithm_id="96")

    assert mock_api.calls[0].request.method == "DELETE"


def test_delete_algorithm_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/ogc/processes/999",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
            "title": "No process with that process ID found",
            "status": 404,
            "detail": "No process with that process ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(AlgorithmNotFoundError):
        client.algorithms.delete(algorithm_id="999")


def test_delete_algorithm_permission_denied(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/ogc/processes/96",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-2/1.0/immutable-process",
            "title": "You can only modify processes that you posted originally",
            "status": 403,
            "detail": "You can only modify processes that you posted originally",
            "instance": "",
        },
        status=403,
    )

    with pytest.raises(AlgorithmPermissionError) as exc_info:
        client.algorithms.delete(algorithm_id="96")

    assert exc_info.value.algorithm_id == "96"


DEPLOYMENT_RESPONSE = {
    "id": "sardem-sarsen",
    "version": "mlucas_test_v1",
    "links": [
        {
            "href": "/ogc/deploymentJobs/271",
            "rel": "monitor",
            "type": "application/json",
            "hreflang": "en",
            "title": "Deploying process status link",
        },
        {
            "href": "/ogc/processes/51",
            "rel": "self",
            "type": "application/json",
            "hreflang": "en",
            "title": "Process",
        },
    ],
    "processPipelineLink": {
        "href": "https://repo.uat.maap-project.org/root/deploy-ogc-hysds/-/pipelines/4143",
        "rel": "monitor",
        "type": "text/html",
        "hreflang": "en",
        "title": "Link to process pipeline",
    },
}


def test_put_algorithm_with_href(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.put(
        "https://api.test.maap.xyz/api/ogc/processes/96",
        json=DEPLOYMENT_RESPONSE,
    )

    deployment = client.algorithms.put(
        algorithm_id="96",
        cwl_href="https://raw.githubusercontent.com/MAAP-Project/sardem-sarsen/refs/heads/nasa-ogc/cwl_workflows/process_sardem-sarsen_nasa-ogc.cwl",
    )

    assert isinstance(deployment, AlgorithmDeployment)
    assert deployment.id == "sardem-sarsen"
    assert deployment.version == "mlucas_test_v1"
    assert deployment.links[0].href == "/ogc/deploymentJobs/271"
    assert deployment.process_pipeline_link is not None
    assert deployment.process_pipeline_link.rel == "monitor"
    assert mock_api.calls[0].request.method == "PUT"
    assert json.loads(mock_api.calls[0].request.body) == {
        "executionUnit": {
            "href": "https://raw.githubusercontent.com/MAAP-Project/sardem-sarsen/refs/heads/nasa-ogc/cwl_workflows/process_sardem-sarsen_nasa-ogc.cwl"
        }
    }


def test_put_algorithm_with_raw_text(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.put(
        "https://api.test.maap.xyz/api/ogc/processes/96",
        json=DEPLOYMENT_RESPONSE,
    )

    deployment = client.algorithms.put(algorithm_id="96", cwl_raw_text="cwlVersion: v1.0\n")

    assert isinstance(deployment, AlgorithmDeployment)
    assert json.loads(mock_api.calls[0].request.body) == {"cwlRawText": "cwlVersion: v1.0\n"}


def test_put_algorithm_by_name_version_deployer(
    mock_api: responses.RequestsMock, client: MAAP
) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=LIST_RESPONSE,
    )
    mock_api.put(
        "https://api.test.maap.xyz/api/ogc/processes/8",
        json=DEPLOYMENT_RESPONSE | {"id": "gedi-subset"},
    )

    deployment = client.algorithms.put(
        name="gedi-subset",
        version="0.13.0",
        deployer="dschuck",
        cwl_href="https://repo.uat.maap-project.org/process.cwl",
    )

    assert deployment.id == "gedi-subset"
    assert mock_api.calls[1].request.method == "PUT"


def test_put_algorithm_requires_body(client: MAAP) -> None:
    with pytest.raises(ValueError, match="cwl_href or cwl_raw_text"):
        client.algorithms.put(algorithm_id="96")


def test_put_algorithm_rejects_both_bodies(client: MAAP) -> None:
    with pytest.raises(ValueError, match="not both"):
        client.algorithms.put(algorithm_id="96", cwl_href="https://x/process.cwl", cwl_raw_text="x")


def test_put_algorithm_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.put(
        "https://api.test.maap.xyz/api/ogc/processes/999",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
            "title": "No process with that process ID found",
            "status": 404,
            "detail": "No process with that process ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(AlgorithmNotFoundError):
        client.algorithms.put(algorithm_id="999", cwl_href="https://x/process.cwl")


def test_put_algorithm_permission_denied(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.put(
        "https://api.test.maap.xyz/api/ogc/processes/96",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-2/1.0/immutable-process",
            "title": "You can only modify processes that you posted originally",
            "status": 403,
            "detail": "You can only modify processes that you posted originally",
            "instance": "",
        },
        status=403,
    )

    with pytest.raises(AlgorithmPermissionError) as exc_info:
        client.algorithms.put(algorithm_id="96", cwl_href="https://x/process.cwl")

    assert exc_info.value.algorithm_id == "96"


def test_deploy_algorithm_with_href(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=DEPLOYMENT_RESPONSE,
    )

    deployment = client.algorithms.deploy(
        cwl_href="https://raw.githubusercontent.com/marjo-luc/ogc-app-pack-examples/refs/heads/main/stac_clip/cwl_workflows/process_stac-clip_main.cwl",
    )

    assert isinstance(deployment, AlgorithmDeployment)
    assert deployment.id == "sardem-sarsen"
    assert mock_api.calls[0].request.method == "POST"
    assert json.loads(mock_api.calls[0].request.body) == {
        "executionUnit": {
            "href": "https://raw.githubusercontent.com/marjo-luc/ogc-app-pack-examples/refs/heads/main/stac_clip/cwl_workflows/process_stac-clip_main.cwl"
        }
    }
    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_deploy_algorithm_with_raw_text(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=DEPLOYMENT_RESPONSE,
    )

    deployment = client.algorithms.deploy(cwl_raw_text="cwlVersion: v1.0\n")

    assert isinstance(deployment, AlgorithmDeployment)
    assert json.loads(mock_api.calls[0].request.body) == {"cwlRawText": "cwlVersion: v1.0\n"}


def test_deploy_algorithm_conflict_falls_back_to_put(
    mock_api: responses.RequestsMock, client: MAAP
) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/ogc/processes",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-2/1.0/duplicated-process",
            "title": "Duplicate process. Use PUT to modify existing process with process ID 20",
            "status": 409,
            "detail": "Duplicate process. Use PUT to modify existing process with process ID 20",
            "instance": "",
            "additionalProperties": {"processID": 20},
        },
        status=409,
    )
    mock_api.put(
        "https://api.test.maap.xyz/api/ogc/processes/20",
        json=DEPLOYMENT_RESPONSE,
    )

    deployment = client.algorithms.deploy(cwl_href="https://x/process.cwl")

    assert isinstance(deployment, AlgorithmDeployment)
    assert mock_api.calls[0].request.method == "POST"
    assert mock_api.calls[1].request.method == "PUT"
    assert mock_api.calls[1].request.url == "https://api.test.maap.xyz/api/ogc/processes/20"
    assert json.loads(mock_api.calls[1].request.body) == {
        "executionUnit": {"href": "https://x/process.cwl"}
    }


def test_deploy_algorithm_requires_body(client: MAAP) -> None:
    with pytest.raises(ValueError, match="cwl_href or cwl_raw_text"):
        client.algorithms.deploy()


def test_deploy_algorithm_rejects_both_bodies(client: MAAP) -> None:
    with pytest.raises(ValueError, match="not both"):
        client.algorithms.deploy(cwl_href="https://x/process.cwl", cwl_raw_text="x")


PACKAGE_RESPONSE = {
    "processDescription": "None",
    "executionUnit": {
        "href": "https://raw.githubusercontent.com/MAAP-Project/OPERA_DPS_JOB/cc23c8a7c0a5907475ff6eae672e6b5c1d209997/cwl_workflows/process_opera_dps_job_ogc.cwl",
        "rel": "monitor-desc",
        "type": "text/html",
        "hreflang": "en",
        "title": "Process Reference",
    },
}


def test_get_package(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes/96/package",
        json=PACKAGE_RESPONSE,
    )

    package = client.algorithms.get_package(algorithm_id="96")

    assert package.process_description == "None"
    assert package.execution_unit.rel == "monitor-desc"
    assert package.execution_unit.href.endswith("process_opera_dps_job_ogc.cwl")
    assert "Authorization" not in mock_api.calls[0].request.headers


def test_get_package_by_name_version_deployer(
    mock_api: responses.RequestsMock, client: MAAP
) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=LIST_RESPONSE,
    )
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes/8/package",
        json=PACKAGE_RESPONSE,
    )

    package = client.algorithms.get_package(
        name="gedi-subset", version="0.13.0", deployer="dschuck"
    )

    assert package.execution_unit.type == "text/html"


def test_get_package_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes/999/package",
        json={
            "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/no-such-process",
            "title": "No process with that process ID found",
            "status": 404,
            "detail": "No process with that process ID found",
            "instance": "",
        },
        status=404,
    )

    with pytest.raises(AlgorithmNotFoundError) as exc_info:
        client.algorithms.get_package(algorithm_id="999")

    assert exc_info.value.algorithm_id == "999"


def test_list_algorithms(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=LIST_RESPONSE,
    )

    algorithms = client.algorithms.list()

    assert len(algorithms) == 2
    assert algorithms[0].id == "gedi-subset"
    assert algorithms[1].deployed_by == "grace.llewellyn"


def test_list_algorithms_with_filters(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/ogc/processes",
        json=LIST_RESPONSE,
        match=[
            responses.matchers.query_param_matcher(
                {
                    "deployer": "dschuck",
                    "algorithmName": "gedi-subset",
                    "algorithmVersion": "0.13.0",
                }
            )
        ],
    )

    algorithms = client.algorithms.list(
        deployer="dschuck", name="gedi-subset", version="0.13.0"
    )

    assert len(algorithms) == 2
