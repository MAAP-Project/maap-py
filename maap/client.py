from maap.config import TokenProvider, host_from_environment, token_from_environment
from maap.help import Help
from maap.services.algorithms import AlgorithmsService
from maap.services.jobs import JobsService
from maap.services.secrets import SecretsService
from maap.services.user import UserService
from maap.transport import Transport


class MAAP:
    def __init__(self, host: str, token: TokenProvider | None = None) -> None:
        self._transport = Transport(host=host, token=token)
        self.jobs = JobsService(self._transport)
        self.algorithms = AlgorithmsService(self._transport)
        self.secrets = SecretsService(self._transport)
        self.user = UserService(self._transport)
        self.help = Help(self)

    @classmethod
    def from_environment(cls) -> "MAAP":
        return cls(
            host=host_from_environment(),
            token=token_from_environment,
        )
