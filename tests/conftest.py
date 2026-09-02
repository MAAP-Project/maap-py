import pytest
import responses

from maap import MAAP


@pytest.fixture
def mock_api():
    with responses.RequestsMock() as rsps:
        yield rsps


@pytest.fixture
def client() -> MAAP:
    return MAAP(host="https://api.test.maap.xyz", token="test-token")
