import os
from unittest import TestCase
import yaml
from maap.maap import MAAP, MaapConfig
from maap import config_reader
import logging
from yaml import load as yaml_load, dump as yaml_dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


class TestConfig(TestCase):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    def test_get_config_url_with_scheme(self):
        maap_host = "https://api.maap-project.org/"
        config_url = config_reader._get_config_url(maap_host)
        self.assertEqual("https://api.maap-project.org/api/environment/config", config_url)
        maap_host = "http://api.maap-project.org/"
        config_url = config_reader._get_config_url(maap_host)
        self.assertEqual("http://api.maap-project.org/api/environment/config", config_url)
        maap_host = "http://localhost:5000/"
        config_url = config_reader._get_config_url(maap_host)
        self.assertEqual("http://localhost:5000/api/environment/config", config_url)

    def test_get_config_url_without_scheme(self):
        maap_host = "api.maap-project.org"
        config_url = config_reader._get_config_url(maap_host)
        self.assertEqual("https://api.maap-project.org/api/environment/config", config_url)

    def test_get_config_url_raise_error(self):
        maap_host = "s3://api.maap-project.org"
        with self.assertRaises(ValueError):
            config_reader._get_config_url(maap_host)

    def test_get_config_url_with_env_override(self):
        maap_host = "api.dit.maap-project.org"
        os.environ['MAAP_API_HOST_SCHEME'] = 'http'
        config_url = config_reader._get_config_url(maap_host)
        self.assertEqual("http://api.dit.maap-project.org/api/environment/config", config_url)
        maap_host = "http://localhost:5000"
        os.environ['MAAP_API_CONFIG_ENDPOINT'] = 'config'
        config_url = config_reader._get_config_url(maap_host)
        self.assertEqual("http://localhost:5000/config", config_url)

    def test_read_conf_hosted_api(self):
        maap = MAAP(maap_host="api.dit.maap-project.org")
        print(maap.config.requester_pays)
        self.assertEqual(str, type(maap.config.requester_pays))

    def test_read_conf_env_var(self):
        os.environ.setdefault("MAAP_API_HOST", "api.dit.maap-project.org")
        maap = MAAP()
        conf = maap.config
        print(dir(maap.config))
        conf = MaapConfig()
        print(conf.workspace_bucket_credentials)

    def test_read_conf_localhost_without_scheme_api(self):
        maap = MAAP(maap_host="localhost:5000")
        conf = MaapConfig()
        print(conf.requester_pays)
        conf = MaapConfig()
        print(conf.workspace_bucket_credentials)

    def test_read_conf_default(self):
        maap = MAAP()
        conf = MaapConfig()
        print(conf.requester_pays)
        conf = MaapConfig()
        print(conf.workspace_bucket_credentials)

    def test_read_conf_uninitialized(self):
        with TestConfig().assertRaises(expected_exception=EnvironmentError, msg="Successfully raised exception") as err:
            MaapConfig()


class TestClientConfigFetch(TestCase):
    CONFIG = {"service": {"maap_api_root": "https://api.example.org/api"}}

    def setUp(self):
        config_reader._CONFIG_CACHE.clear()

    def _resp(self, status=200):
        from unittest import mock
        resp = mock.Mock(status_code=status)
        resp.json.return_value = {"service": dict(self.CONFIG["service"])}
        if status >= 400:
            import requests
            resp.raise_for_status.side_effect = requests.HTTPError(str(status))
        return resp

    def test_transient_failure_then_success(self):
        from unittest import mock
        import requests
        with mock.patch("requests.Session.get",
                        side_effect=[requests.ConnectionError("boom"), self._resp()]) as get:
            with self.assertRaises(RuntimeError):
                config_reader._get_client_config("https://api.example.org")
            config = config_reader._get_client_config("https://api.example.org")
        self.assertEqual(2, get.call_count)
        self.assertEqual("https://api.example.org/api/", config["service"]["maap_api_root"])

    def test_permanent_failure_not_cached(self):
        from unittest import mock
        with mock.patch("requests.Session.get", return_value=self._resp(503)) as get:
            for _ in range(2):
                with self.assertRaises(RuntimeError):
                    config_reader._get_client_config("https://api.example.org")
        self.assertEqual(2, get.call_count)
        self.assertNotIn("https://api.example.org", config_reader._CONFIG_CACHE)

    def test_success_is_cached_and_timeout_used(self):
        from unittest import mock
        with mock.patch("requests.Session.get", return_value=self._resp()) as get:
            first = config_reader._get_client_config("https://api.example.org")
            second = config_reader._get_client_config("https://api.example.org")
        self.assertIs(first, second)
        self.assertEqual(1, get.call_count)
        self.assertEqual(config_reader._CONFIG_TIMEOUT, get.call_args.kwargs["timeout"])
