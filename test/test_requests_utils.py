import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import pytest
import requests

from maap.utils import requests_utils as ru


@pytest.fixture(autouse=True)
def fresh_sessions(monkeypatch):
    # Keep tests fast: no real backoff sleeping
    monkeypatch.setattr(ru, "RETRY_BACKOFF_FACTOR", 0)
    ru.reset_sessions()
    yield
    ru.reset_sessions()


@pytest.fixture
def config():
    return SimpleNamespace(maap_token="abc123", content_type="application/json")


@pytest.fixture
def server():
    """Local server replying with each queued status in turn (last one repeats)."""
    state = {"statuses": [200], "calls": [], "headers": []}

    class Handler(BaseHTTPRequestHandler):
        def _respond(self):
            state["calls"].append(self.command)
            state["headers"].append(dict(self.headers))
            idx = min(len(state["calls"]) - 1, len(state["statuses"]) - 1)
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            self.send_response(state["statuses"][idx])
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        do_GET = do_POST = do_PUT = do_DELETE = _respond

        def log_message(self, *args):
            pass

    httpd = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    state["url"] = f"http://127.0.0.1:{httpd.server_port}/x"
    yield state
    httpd.shutdown()
    httpd.server_close()


def test_session_is_shared_and_pooled():
    assert ru.get_session() is ru.get_session()
    assert ru.get_session(retry_post=True) is not ru.get_session()


def test_retry_policy_methods():
    get_retry = ru.get_session().get_adapter("https://x").max_retries
    assert get_retry.total == ru.RETRY_TOTAL
    assert set(get_retry.status_forcelist) == set(ru.RETRY_STATUS_CODES)
    assert get_retry.respect_retry_after_header
    assert get_retry.allowed_methods == ru.IDEMPOTENT_METHODS
    post_retry = ru.get_session(retry_post=True).get_adapter("https://x").max_retries
    assert "POST" in post_retry.allowed_methods


def test_get_retries_transient_errors(server, config):
    server["statuses"] = [503, 502, 200]
    r = ru.request(ru.GET, server["url"], config)
    assert r.status_code == 200
    assert server["calls"] == ["GET"] * 3


@pytest.mark.parametrize("method", [ru.PUT, ru.DELETE])
def test_idempotent_methods_retry(server, config, method):
    server["statuses"] = [429, 200]
    assert ru.request(method, server["url"], config).status_code == 200
    assert len(server["calls"]) == 2


def test_post_not_retried_by_default(server, config):
    server["statuses"] = [503, 200]
    r = ru.request(ru.POST, server["url"], config, json={"a": 1})
    assert r.status_code == 503
    assert server["calls"] == ["POST"]


def test_post_retried_when_opted_in(server, config):
    server["statuses"] = [503, 200]
    r = ru.request(ru.POST, server["url"], config, retry_post=True, json={"a": 1})
    assert r.status_code == 200
    assert server["calls"] == ["POST", "POST"]


def test_exhausted_retries_return_last_response(server, config):
    server["statuses"] = [503]
    r = ru.request(ru.GET, server["url"], config)
    assert r.status_code == 503
    assert len(server["calls"]) == ru.RETRY_TOTAL + 1


def test_client_errors_not_retried(server, config):
    server["statuses"] = [404]
    assert ru.request(ru.GET, server["url"], config).status_code == 404
    assert len(server["calls"]) == 1


def test_default_timeout_applied(monkeypatch, config):
    seen = {}

    def fake_request(self, **kwargs):
        seen.update(kwargs)
        return requests.Response()

    monkeypatch.setattr(requests.Session, "request", fake_request)
    ru.request("get", "http://example.com", config)
    assert seen["timeout"] == ru.DEFAULT_TIMEOUT
    ru.request("get", "http://example.com", config, timeout=5)
    assert seen["timeout"] == 5


def test_token_header_read_per_call(server, config):
    ru.request(ru.GET, server["url"], config)
    config.maap_token = "refreshed"
    ru.request(ru.GET, server["url"], config)
    assert server["headers"][0]["token"] == "abc123"
    assert server["headers"][1]["token"] == "refreshed"


def test_bearer_token_uses_authorization_header(server, config, monkeypatch):
    monkeypatch.setenv("MAAP_PGT", "pgt-ticket")
    config.maap_token = "Bearer xyz"
    ru.request(ru.GET, server["url"], config)
    assert server["headers"][0]["Authorization"] == "Bearer xyz"
    assert "token" not in server["headers"][0]
    assert server["headers"][0]["proxy-ticket"] == "pgt-ticket"


def test_auth_false_sends_no_maap_credentials(server, config, monkeypatch):
    monkeypatch.setenv("MAAP_PGT", "pgt-ticket")
    ru.request(ru.GET, server["url"], auth=False)
    sent = {k.lower() for k in server["headers"][0]}
    assert not sent & {"token", "authorization", "proxy-ticket"}


def test_auth_requires_config():
    with pytest.raises(ValueError):
        ru.request(ru.GET, "http://example.com")


def test_extra_headers_override_and_are_not_mutated(server, config):
    extra = {"Accept": "text/plain"}
    ru.request(ru.GET, server["url"], config, headers=extra)
    assert server["headers"][0]["Accept"] == "text/plain"
    assert extra == {"Accept": "text/plain"}


def test_tokens_not_logged(server, config, caplog):
    caplog.set_level("DEBUG", logger=ru.logger.name)
    ru.request(ru.GET, server["url"], config)
    assert "abc123" not in caplog.text


def test_make_request_supports_all_methods(server, config):
    for method in (ru.GET, ru.POST, ru.PUT, ru.DELETE):
        assert ru.make_request(server["url"], config, request_type=method).status_code == 200
    assert server["calls"] == ["GET", "POST", "PUT", "DELETE"]
    assert ru.make_dps_request(server["url"], config) == "ok"
