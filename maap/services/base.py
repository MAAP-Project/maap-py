from maap.transport import Transport


class BaseService:
    def __init__(self, transport: Transport) -> None:
        self._transport = transport
