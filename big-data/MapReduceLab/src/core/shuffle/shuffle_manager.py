from collections import defaultdict
from typing import Dict, List, Any, Iterable


class ShuffleManager:
    def __init__(self) -> None:
        pass

    def group_by_key(self, mapped_items: Iterable[tuple]) -> Dict[Any, List[Any]]:
        grouped: Dict[Any, List[Any]] = defaultdict(list)
        for k, v in mapped_items:
            grouped[k].append(v)
        return grouped


