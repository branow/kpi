import threading
from pathlib import Path
from typing import Any, Callable, Iterable, List, Tuple

from src.core.cluster.message_bus import MessageBus
from src.core.storage.local_block_fs import LocalBlockFileSystem
from src.core.worker.map_task_executor import MapTaskExecutor
from src.core.worker.reduce_task_executor import ReduceTaskExecutor
from src.core.shuffle.shuffle_manager import ShuffleManager


class WorkerRuntime:
    def __init__(self, name: str, bus: MessageBus, assigned_files: List[Path]) -> None:
        self.name = name
        self.bus = bus
        self.fs = LocalBlockFileSystem(assigned_files)
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self._stop = threading.Event()

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            msg = self.bus.recv(self.name)
            tag = msg[0]
            if tag == "MAP":
                _, input_file, mapper_factory = msg
                execu = MapTaskExecutor(mapper_factory)
                records = self._read_records_from_file(input_file)
                out = execu.execute(records)
                self.bus.send("coordinator", out)
            elif tag == "REDUCE":
                _, shard, items, reducer_factory = msg
                shuffle = ShuffleManager()
                grouped = shuffle.group_by_key(items)
                execu = ReduceTaskExecutor(reducer_factory)
                out = execu.execute(grouped)
                self.bus.send("coordinator", out)
            elif tag == "STOP":
                break

    def _read_records_from_file(self, path: Path) -> Iterable[str]:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                yield line.rstrip("\n")


