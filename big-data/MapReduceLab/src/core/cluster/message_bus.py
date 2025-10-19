import queue
from typing import Any, Dict


class MessageBus:
    def __init__(self) -> None:
        self.queues: Dict[str, queue.Queue] = {}

    def register(self, name: str) -> None:
        self.queues[name] = queue.Queue()

    def send(self, name: str, message: Any) -> None:
        self.queues[name].put(message)

    def recv(self, name: str, block: bool = True, timeout: float | None = None) -> Any:
        return self.queues[name].get(block=block, timeout=timeout)


