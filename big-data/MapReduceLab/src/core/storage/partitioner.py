from typing import Any


class Partitioner:
    def shard_for_key(self, key: Any, num_reducers: int) -> int:
        return hash(str(key)) % max(1, num_reducers)


