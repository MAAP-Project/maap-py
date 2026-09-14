from maap.config import TokenProvider, host_from_environment, token_from_environment
from maap.help import Help
from maap.services.algorithms import AlgorithmsService
from maap.services.jobs import JobsService
from maap.services.secrets import SecretsService
from maap.services.user import UserService
from maap.transport import Transport


class MAAP:
    """The MAAP client.

    Args:
        host: The API host to talk to, e.g. https://api.maap-project.org. Defaults to
            the MAAP_API_HOST environment variable, and raises ValueError if neither
            is given.
        token: The bearer token to authenticate with, or a callable returning one.
            Defaults to reading MAAP_TOKEN from the environment when a request is
            made, so a token exported after the client is built is still picked up.
    """

    def __init__(self, host: str | None = None, token: TokenProvider | None = None) -> None:
        self._transport = Transport(
            host=host_from_environment() if host is None else host.rstrip("/"),
            # Passed as a callable rather than a resolved string so that the
            # environment is read per request, not once at construction.
            token=token_from_environment if token is None else token,
        )
        self.jobs = JobsService(self._transport)
        self.algorithms = AlgorithmsService(self._transport)
        self.secrets = SecretsService(self._transport)
        self.user = UserService(self._transport)
        self.help = Help(self)

    @classmethod
    def from_environment(cls) -> "MAAP":
        """Build a client from MAAP_API_HOST and MAAP_TOKEN.

        Equivalent to calling MAAP() with no arguments.
        """
        return cls()
