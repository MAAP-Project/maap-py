import pytest
import responses

from maap import MAAP
from maap.exceptions import AuthenticationError
from maap.types.user import Organization, UserProfile

PROFILE_RESPONSE = {
    "last_name": "Lucas",
    "creation_date": "2021-06-15T21:57:49.022699",
    "public_ssh_key_modified_date": "2025-01-29T23:10:00.451066",
    "public_ssh_key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC+lS3I mlucas@MT-210295\n",
    "public_ssh_key_name": "id_rsa.pub",
    "status": "active",
    "session_key": "",
    "urs_token": "",
    "id": 18,
    "username": "mlucas",
    "email": "mlucas@example.test",
    "first_name": "Marjorie",
    "organizations": [
        {"id": 2, "name": "JPL Dev Team"},
    ],
}

UNAUTHORIZED_RESPONSE = {
    "code": 401,
    "message": "Unauthorized",
}


def test_get_profile(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self",
        json=PROFILE_RESPONSE,
    )

    profile = client.user.get_profile()

    assert isinstance(profile, UserProfile)
    assert profile.id == 18
    assert profile.username == "mlucas"
    assert profile.first_name == "Marjorie"
    assert profile.last_name == "Lucas"
    assert profile.email == "mlucas@example.test"
    assert profile.status == "active"
    assert profile.creation_date == "2021-06-15T21:57:49.022699"
    assert profile.session_key == ""
    assert profile.urs_token == ""


def test_get_profile_ssh_key(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self",
        json=PROFILE_RESPONSE,
    )

    profile = client.user.get_profile()

    assert profile.public_ssh_key is not None
    assert profile.public_ssh_key.startswith("ssh-rsa ")
    assert profile.public_ssh_key_name == "id_rsa.pub"
    assert profile.public_ssh_key_modified_date == "2025-01-29T23:10:00.451066"


def test_get_profile_organizations(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self",
        json=PROFILE_RESPONSE,
    )

    profile = client.user.get_profile()

    assert len(profile.organizations) == 1
    assert all(isinstance(org, Organization) for org in profile.organizations)
    assert profile.organizations[0].id == 2
    assert profile.organizations[0].name == "JPL Dev Team"


def test_get_profile_no_ssh_key(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self",
        json={
            **PROFILE_RESPONSE,
            "public_ssh_key": None,
            "public_ssh_key_name": None,
            "public_ssh_key_modified_date": None,
        },
    )

    profile = client.user.get_profile()

    assert profile.public_ssh_key is None
    assert profile.public_ssh_key_name is None
    assert profile.public_ssh_key_modified_date is None


def test_get_profile_no_organizations(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self",
        json={**PROFILE_RESPONSE, "organizations": []},
    )

    assert client.user.get_profile().organizations == []


def test_get_profile_is_authenticated(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self",
        json=PROFILE_RESPONSE,
    )

    client.user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_get_profile_unauthorized(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self",
        json=UNAUTHORIZED_RESPONSE,
        status=401,
    )

    with pytest.raises(AuthenticationError) as exc_info:
        client.user.get_profile()

    assert exc_info.value.status_code == 401


QUEUES_RESPONSE = {
    "code": 200,
    "message": "success",
    "queues": [
        "maap-dps-sandbox",
        "maap-dps-cuny-worker-64gb",
        "maap-dps-worker-64gb",
        "maap-dps-worker-32vcpu-64gb",
        "maap-dps-gpu-worker-16gb",
        "maap-dps-worker-32gb",
        "maap-dps-worker-8gb",
        "maap-dps-gedi_boreal_worker-16vcpu-32gb",
        "maap-dps-worker-16gb",
    ],
}


def test_get_queues(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/mas/algorithm/resource",
        json=QUEUES_RESPONSE,
    )

    queues = client.user.get_queues()

    assert len(queues) == 9
    assert all(isinstance(queue, str) for queue in queues)
    # The API's order is preserved.
    assert queues[0] == "maap-dps-sandbox"
    assert queues[-1] == "maap-dps-worker-16gb"
    assert "maap-dps-worker-8gb" in queues


def test_get_queues_none_available(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/mas/algorithm/resource",
        json={"code": 200, "message": "success", "queues": []},
    )

    assert client.user.get_queues() == []


def test_get_queues_is_authenticated(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/mas/algorithm/resource",
        json=QUEUES_RESPONSE,
    )

    client.user.get_queues()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_get_queues_unauthorized(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/mas/algorithm/resource",
        json=UNAUTHORIZED_RESPONSE,
        status=401,
    )

    with pytest.raises(AuthenticationError):
        client.user.get_queues()
