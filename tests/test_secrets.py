import json

import pytest
import responses

from maap import MAAP
from maap.exceptions import APIError, SecretAlreadyExistsError, SecretNotFoundError
from maap.types.secrets import Secret, SecretSummary

LIST_RESPONSE = [
    {"secret_name": "MAAP_PGT"},
    {"secret_name": "my_api_key"},
]

SECRET_RESPONSE = {
    "secret_name": "my_api_key",
    "secret_value": "s3cr3t",
}

NOT_FOUND_RESPONSE = {
    "code": 404,
    "message": "No secret exists with name my_api_key",
}

DELETE_RESPONSE = {
    "code": 200,
    "message": "Successfully deleted secret my_api_key",
}


def test_list_secrets(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json=LIST_RESPONSE,
    )

    secrets = client.secrets.list()

    assert len(secrets) == 2
    assert all(isinstance(secret, SecretSummary) for secret in secrets)
    assert [secret.name for secret in secrets] == ["MAAP_PGT", "my_api_key"]


def test_list_secrets_empty(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json=[],
    )

    assert client.secrets.list() == []


def test_list_secrets_is_authenticated(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json=LIST_RESPONSE,
    )

    client.secrets.list()

    assert mock_api.calls[0].request.headers["Authorization"] == "Bearer test-token"


def test_get_secret(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets/my_api_key",
        json=SECRET_RESPONSE,
    )

    secret = client.secrets.get(name="my_api_key")

    assert isinstance(secret, Secret)
    assert secret.name == "my_api_key"
    assert secret.value == "s3cr3t"


def test_get_secret_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets/my_api_key",
        json=NOT_FOUND_RESPONSE,
        status=404,
    )

    with pytest.raises(SecretNotFoundError) as exc_info:
        client.secrets.get(name="my_api_key")

    assert exc_info.value.secret_name == "my_api_key"
    assert "No secret exists with name my_api_key" in str(exc_info.value)


def test_get_secret_quotes_name(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets/my%2Fkey",
        json={"secret_name": "my/key", "secret_value": "s3cr3t"},
    )

    secret = client.secrets.get(name="my/key")

    assert secret.name == "my/key"


def test_get_secret_empty_name(client: MAAP) -> None:
    with pytest.raises(ValueError, match="name must not be empty"):
        client.secrets.get(name="")


def test_has_true(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json=LIST_RESPONSE,
    )

    assert client.secrets.has(name="my_api_key") is True


def test_has_false(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json=LIST_RESPONSE,
    )

    assert client.secrets.has(name="nope") is False


def test_has_no_secrets(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json=[],
    )

    assert client.secrets.has(name="my_api_key") is False


def test_has_never_reads_the_value(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.get(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json=LIST_RESPONSE,
    )

    client.secrets.has(name="my_api_key")

    # One listing request; the per-secret endpoint that decrypts the value is untouched.
    assert len(mock_api.calls) == 1
    assert mock_api.calls[0].request.url.endswith("/api/members/self/secrets")


def test_has_empty_name(client: MAAP) -> None:
    with pytest.raises(ValueError, match="name must not be empty"):
        client.secrets.has(name="")


def test_add_secret(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json={"secret_name": "my_api_key"},
    )

    secret = client.secrets.add(name="my_api_key", value="s3cr3t")

    assert isinstance(secret, SecretSummary)
    assert secret.name == "my_api_key"

    body = json.loads(mock_api.calls[0].request.body)
    assert body == {"secret_name": "my_api_key", "secret_value": "s3cr3t"}


def test_add_secret_already_exists(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json={
            "code": 400,
            "message": (
                "Secret already exists with name my_api_key. Please delete and re-create "
                "the secret to update it's value. "
            ),
        },
        status=400,
    )

    with pytest.raises(SecretAlreadyExistsError) as exc_info:
        client.secrets.add(name="my_api_key", value="s3cr3t")

    assert exc_info.value.secret_name == "my_api_key"
    assert "Secret already exists with name my_api_key" in str(exc_info.value)


def test_add_secret_other_error_propagates(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json={"code": 400, "message": "secret_value is required."},
        status=400,
    )

    with pytest.raises(APIError) as exc_info:
        client.secrets.add(name="my_api_key", value="s3cr3t")

    assert not isinstance(exc_info.value, SecretAlreadyExistsError)
    assert exc_info.value.status_code == 400


def test_add_secrets_batch(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json={"secret_name": "first"},
    )
    mock_api.post(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json={"secret_name": "second"},
    )

    added = client.secrets.add(
        [
            {"name": "first", "value": "one"},
            {"name": "second", "value": "two"},
        ]
    )

    assert isinstance(added, list)
    assert all(isinstance(secret, SecretSummary) for secret in added)
    assert [secret.name for secret in added] == ["first", "second"]

    bodies = [json.loads(call.request.body) for call in mock_api.calls]
    assert bodies == [
        {"secret_name": "first", "secret_value": "one"},
        {"secret_name": "second", "secret_value": "two"},
    ]


def test_add_secrets_batch_empty(client: MAAP) -> None:
    assert client.secrets.add([]) == []


def test_add_secrets_batch_stops_at_first_failure(
    mock_api: responses.RequestsMock, client: MAAP
) -> None:
    mock_api.post(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json={"secret_name": "first"},
    )
    mock_api.post(
        "https://api.test.maap.xyz/api/members/self/secrets",
        json={
            "code": 400,
            "message": "Secret already exists with name second. Please delete and re-create.",
        },
        status=400,
    )

    with pytest.raises(SecretAlreadyExistsError) as exc_info:
        client.secrets.add(
            [
                {"name": "first", "value": "one"},
                {"name": "second", "value": "two"},
                {"name": "third", "value": "three"},
            ]
        )

    assert exc_info.value.secret_name == "second"
    # "first" was already added and stays added; "third" is never attempted.
    assert len(mock_api.calls) == 2


def test_add_secrets_batch_duplicate_names(client: MAAP) -> None:
    # Validation runs before any request, so nothing is added.
    with pytest.raises(ValueError, match=r"secrets\[1\] repeats an earlier secret name: dupe"):
        client.secrets.add(
            [
                {"name": "dupe", "value": "one"},
                {"name": "dupe", "value": "two"},
            ]
        )


def test_add_secrets_batch_wrong_keys(client: MAAP) -> None:
    with pytest.raises(ValueError, match=r"secrets\[1\] must have exactly the keys"):
        client.secrets.add(
            [
                {"name": "first", "value": "one"},
                {"name1": "second", "value1": "two"},
            ]
        )


def test_add_secrets_batch_extra_keys(client: MAAP) -> None:
    with pytest.raises(ValueError, match=r"secrets\[0\] must have exactly the keys"):
        client.secrets.add([{"name": "first", "value": "one", "note": "extra"}])


def test_add_secrets_batch_missing_key(client: MAAP) -> None:
    with pytest.raises(ValueError, match=r"secrets\[0\] must have exactly the keys"):
        client.secrets.add([{"name": "first"}])


def test_add_secrets_batch_non_string_value(client: MAAP) -> None:
    with pytest.raises(ValueError, match=r"secrets\[0\] .* must both be strings"):
        client.secrets.add([{"name": "first", "value": 1}])  # type: ignore[list-item]


def test_add_secrets_batch_not_a_mapping(client: MAAP) -> None:
    with pytest.raises(ValueError, match=r"secrets\[0\] must be a mapping, got str"):
        client.secrets.add(["first"])  # type: ignore[list-item]


def test_add_secrets_batch_empty_name(client: MAAP) -> None:
    with pytest.raises(ValueError, match=r"secrets\[1\] name must not be empty"):
        client.secrets.add([{"name": "ok", "value": "one"}, {"name": "", "value": "two"}])


def test_add_secrets_batch_empty_value(client: MAAP) -> None:
    with pytest.raises(ValueError, match=r"secrets\[1\] value must not be empty"):
        client.secrets.add([{"name": "ok", "value": "one"}, {"name": "bad", "value": ""}])


def test_add_secrets_both_forms(client: MAAP) -> None:
    with pytest.raises(ValueError, match="not both"):
        client.secrets.add(  # type: ignore[call-overload]
            [{"name": "first", "value": "one"}], name="second", value="two"
        )


def test_add_secrets_neither_form(client: MAAP) -> None:
    with pytest.raises(ValueError, match="Provide either secrets or both name and value"):
        client.secrets.add()  # type: ignore[call-overload]


def test_add_secret_empty_name(client: MAAP) -> None:
    with pytest.raises(ValueError, match="name must not be empty"):
        client.secrets.add(name="", value="s3cr3t")


def test_add_secret_empty_value(client: MAAP) -> None:
    with pytest.raises(ValueError, match="value must not be empty"):
        client.secrets.add(name="my_api_key", value="")


def test_delete_secret(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/members/self/secrets/my_api_key",
        json=DELETE_RESPONSE,
    )

    assert client.secrets.delete(name="my_api_key") is None
    assert mock_api.calls[0].request.method == "DELETE"


def test_delete_secret_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    # The members API reports a missing secret on delete as a 400, not a 404.
    mock_api.delete(
        "https://api.test.maap.xyz/api/members/self/secrets/my_api_key",
        json={"code": 400, "message": "No secret exists with name my_api_key"},
        status=400,
    )

    with pytest.raises(SecretNotFoundError) as exc_info:
        client.secrets.delete(name="my_api_key")

    assert exc_info.value.secret_name == "my_api_key"
    assert "No secret exists with name my_api_key" in str(exc_info.value)


def test_delete_secret_404_not_found(mock_api: responses.RequestsMock, client: MAAP) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/members/self/secrets/my_api_key",
        json=NOT_FOUND_RESPONSE,
        status=404,
    )

    with pytest.raises(SecretNotFoundError):
        client.secrets.delete(name="my_api_key")


def test_delete_secret_other_error_propagates(
    mock_api: responses.RequestsMock, client: MAAP
) -> None:
    mock_api.delete(
        "https://api.test.maap.xyz/api/members/self/secrets/my_api_key",
        json={"code": 400, "message": "Valid JSON body object required."},
        status=400,
    )

    with pytest.raises(APIError) as exc_info:
        client.secrets.delete(name="my_api_key")

    assert not isinstance(exc_info.value, SecretNotFoundError)


def test_delete_secret_empty_name(client: MAAP) -> None:
    with pytest.raises(ValueError, match="name must not be empty"):
        client.secrets.delete(name="")
