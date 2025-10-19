from abc import ABC, abstractmethod
from typing import Any, Callable


class Mapper(ABC):
    @abstractmethod
    def map(self, record: Any, emit: Callable[[Any, Any], None]) -> None:
        raise NotImplementedError


