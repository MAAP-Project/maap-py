import pytest
import responses

from maap import MAAP
from maap.config import HOST_ENV_VAR, TOKEN_ENV_VAR

PROFILE_URL = "https://api.env.maap.xyz/api/members/self"
PROFILE_RESPONSE = {
    "id": 18,
    "username": "mlucas",
    "email": "mlucas@example.test",
    "first_name": "Marjorie",
    "last_name": "Lucas",
    "status": "active",
    "creation_date": "2021-06-15T21:57:49.022699",
    "organizations": [],
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    monkeypatch.delenv(HOST_ENV_VAR, raising=False)
    monkeypatch.delenv(TOKEN_ENV_VAR, raising=False)
    return monkeypatch


def test_reads_host_and_token_from_the_environment(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    env.setenv(HOST_ENV_VAR, "https://api.env.maap.xyz")
    env.setenv(TOKEN_ENV_VAR, "env-token")
    mock_api.add(responses.GET, PROFILE_URL, json=PROFILE_RESPONSE)

    MAAP().user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer env-token"


def test_explicit_arguments_take_precedence_over_the_environment(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    env.setenv(HOST_ENV_VAR, "https://api.env.maap.xyz")
    env.setenv(TOKEN_ENV_VAR, "env-token")
    explicit_url = "https://api.explicit.maap.xyz/api/members/self"
    mock_api.add(responses.GET, explicit_url, json=PROFILE_RESPONSE)

    client = MAAP(host="https://api.explicit.maap.xyz", token="explicit-token")
    client.user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer explicit-token"


def test_host_may_be_explicit_while_the_token_comes_from_the_environment(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    env.setenv(TOKEN_ENV_VAR, "env-token")
    mock_api.add(responses.GET, PROFILE_URL, json=PROFILE_RESPONSE)

    MAAP(host="https://api.env.maap.xyz").user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer env-token"


def test_token_may_be_explicit_while_the_host_comes_from_the_environment(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    env.setenv(HOST_ENV_VAR, "https://api.env.maap.xyz")
    mock_api.add(responses.GET, PROFILE_URL, json=PROFILE_RESPONSE)

    MAAP(token="explicit-token").user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer explicit-token"


def test_token_is_read_per_request_not_at_construction(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    env.setenv(HOST_ENV_VAR, "https://api.env.maap.xyz")
    client = MAAP()
    env.setenv(TOKEN_ENV_VAR, "late-token")
    mock_api.add(responses.GET, PROFILE_URL, json=PROFILE_RESPONSE)

    client.user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer late-token"


def test_a_callable_token_is_still_accepted(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    mock_api.add(responses.GET, PROFILE_URL, json=PROFILE_RESPONSE)

    client = MAAP(host="https://api.env.maap.xyz", token=lambda: "callable-token")
    client.user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer callable-token"


def test_missing_host_is_reported_at_construction(env: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match=HOST_ENV_VAR):
        MAAP()


def test_missing_token_is_reported_when_a_request_needs_one(env: pytest.MonkeyPatch) -> None:
    env.setenv(HOST_ENV_VAR, "https://api.env.maap.xyz")
    client = MAAP()

    with pytest.raises(ValueError, match=TOKEN_ENV_VAR):
        client.user.get_profile()


def test_trailing_slashes_are_trimmed_from_either_source(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    mock_api.add(responses.GET, PROFILE_URL, json=PROFILE_RESPONSE)

    MAAP(host="https://api.env.maap.xyz/", token="t").user.get_profile()

    assert mock_api.calls[0].request.url == PROFILE_URL


def test_from_environment_matches_the_default_constructor(
    env: pytest.MonkeyPatch, mock_api: responses.RequestsMock
) -> None:
    env.setenv(HOST_ENV_VAR, "https://api.env.maap.xyz")
    env.setenv(TOKEN_ENV_VAR, "env-token")
    mock_api.add(responses.GET, PROFILE_URL, json=PROFILE_RESPONSE)

    MAAP.from_environment().user.get_profile()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer env-token"
