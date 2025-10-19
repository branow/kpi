from abc import ABC, abstractmethod
from typing import Any, Iterable, Callable


class Reducer(ABC):
    @abstractmethod
    def reduce(self, key: Any, values: Iterable[Any], emit: Callable[[Any, Any], None]) -> None:
        raise NotImplementedError


