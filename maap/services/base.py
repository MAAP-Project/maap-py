from maap.help import ServiceHelp
from maap.transport import Transport


class BaseService:
    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def help(self) -> ServiceHelp:
        """List the functions this service area supports."""
        return ServiceHelp(self)
