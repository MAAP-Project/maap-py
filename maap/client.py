from maap.config import TokenProvider, host_from_environment, token_from_environment
from maap.services.algorithms import AlgorithmsService
from maap.services.jobs import JobsService
from maap.transport import Transport


class MAAP:
    def __init__(self, host: str, token: TokenProvider | None = None) -> None:
        self._transport = Transport(host=host, token=token)
        self.jobs = JobsService(self._transport)
        self.algorithms = AlgorithmsService(self._transport)

    @classmethod
    def from_environment(cls) -> "MAAP":
        return cls(
            host=host_from_environment(),
            token=token_from_environment,
        )
