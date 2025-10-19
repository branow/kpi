from typing import Any, Callable, Iterable, List, Tuple


class MapTaskExecutor:
    def __init__(self, mapper_factory: Callable[[], Any]):
        self.mapper_factory = mapper_factory

    def execute(self, records: Iterable[Any]) -> List[Tuple[Any, Any]]:
        out: List[Tuple[Any, Any]] = []

        def emit(key: Any, value: Any) -> None:
            out.append((key, value))

        mapper = self.mapper_factory()
        for rec in records:
            mapper.map(rec, emit)
        return out


