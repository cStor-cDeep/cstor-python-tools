import logging
import asyncio
from typing import MutableSet, Optional, Generic, TypeVar


T = TypeVar('T')


class AioEventQueue(Generic[T], asyncio.Queue):

    def __init__(self, name: str, broker):
        super().__init__()
        self._name = name
        self._broker = broker

    @property
    def name(self):
        return self._name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._broker.unregister(self)


class AioEventBroker(Generic[T]):
    """A broker for events, it sends the events to the registered EventQueues"""

    def __init__(self, name: str, logger: logging.Logger = None):
        # this condition will be update later to allow different logger base names
        if logger is None:
            logger = logging.getLogger(f'aioeventbroker.{name}')

        self._logger = logger
        self._name = name
        self._listener_names: MutableSet[str] = set()
        self._listeners: Optional[MutableSet[AioEventQueue]] = None

    def has_listeners(self) -> bool:
        """Returns whether this broker has listeners"""
        return bool(self._listeners)

    def send_now(self, data: T):
        """Sends the event, if a EventQueue is full, it won't receive the event"""
        if self._listeners is None:
            return

        for listener in self._listeners:
            try:
                listener.put_nowait(data)
            except asyncio.QueueFull:
                self._logger.warning("EventQueue '%s' is full, skip: %s", listener.name, data)

    async def send(self, data: T):
        """Sends the event, this might block if a EventQueue is full"""
        if self._listeners is None:
            return

        for listener in self._listeners:
            await listener.put(data)

    def register(self, name: str) -> AioEventQueue[T]:

        if name in self._listener_names:
            self._logger.warning("Register listener %s: already exist", name)
            raise RuntimeWarning(f"{name} already registered")

        self._logger.info("Register listener %s", name)

        q = AioEventQueue(name, self)

        self._listener_names.add(name)

        if self._listeners is None:
            self._listeners = {q}
        else:
            self._listeners.add(q)

        return q

    def unregister(self, queue: AioEventQueue[T]):
        name = queue.name
        if name not in self._listener_names:
            self._logger.warning(f"Unregister listener '{name}': it was NOT registered")
            return

        self._logger.info("Unregister listener '%s'", name)

        self._listener_names.discard(name)
        self._listeners.discard(queue)
