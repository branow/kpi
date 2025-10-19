from typing import Any, Callable, Dict, List, Tuple
from pathlib import Path

from src.core.cluster.message_bus import MessageBus
from src.core.cluster.scheduler import Scheduler
from src.core.storage.partitioner import Partitioner
from src.core.shuffle.shuffle_manager import ShuffleManager


class Coordinator:
    def __init__(
        self,
        bus: MessageBus,
        worker_names: List[str],
        num_reducers: int,
        mapper_factory: Callable[[], Any],
        reducer_factory: Callable[[], Any],
    ) -> None:
        self.bus = bus
        self.scheduler = Scheduler(worker_names)
        self.num_reducers = num_reducers
        self.mapper_factory = mapper_factory
        self.reducer_factory = reducer_factory
        self.partitioner = Partitioner()
        self.shuffle = ShuffleManager()

    def run(self, input_files: List[Path]) -> List[Tuple[Any, Any]]:
        map_results: List[List[Tuple[Any, Any]]] = []
        for input_file in input_files:
            worker = self.scheduler.next_worker()
            self.bus.send(worker, ("MAP", input_file, self.mapper_factory))
        for _ in input_files:
            result = self.bus.recv("coordinator")
            map_results.append(result)

        flattened: List[Tuple[Any, Any]] = [item for sub in map_results for item in sub]
        by_shard: Dict[int, List[Tuple[Any, Any]]] = {i: [] for i in range(self.num_reducers)}
        for key, value in flattened:
            shard = self.partitioner.shard_for_key(key, self.num_reducers)
            by_shard[shard].append((key, value))

        reduce_results: List[List[Tuple[Any, Any]]] = []
        for shard, items in by_shard.items():
            worker = self.scheduler.next_worker()
            self.bus.send(worker, ("REDUCE", shard, items, self.reducer_factory))
        for _ in range(self.num_reducers):
            reduce_results.append(self.bus.recv("coordinator"))

        final_out: List[Tuple[Any, Any]] = [item for sub in reduce_results for item in sub]
        return final_out


