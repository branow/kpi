from typing import Any, Callable, Dict, Iterable, List, Tuple


class ReduceTaskExecutor:
    def __init__(self, reducer_factory: Callable[[], Any]):
        self.reducer_factory = reducer_factory

    def execute(self, grouped: Dict[Any, List[Any]]) -> List[Tuple[Any, Any]]:
        out: List[Tuple[Any, Any]] = []

        def emit(key: Any, value: Any) -> None:
            out.append((key, value))

        reducer = self.reducer_factory()
        for key, values in grouped.items():
            reducer.reduce(key, values, emit)
        return out


