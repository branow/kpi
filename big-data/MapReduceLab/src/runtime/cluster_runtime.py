from pathlib import Path
from typing import List

from src.core.cluster.message_bus import MessageBus
from src.runtime.worker_runtime import WorkerRuntime


class ClusterRuntime:
    def __init__(self, num_workers: int, data_dir: Path) -> None:
        self.num_workers = num_workers
        self.data_dir = data_dir
        self.bus = MessageBus()
        self.workers: List[WorkerRuntime] = []

    def start(self) -> None:
        self.bus.register("coordinator")
        files = sorted(self.data_dir.glob("*.txt"))
        splits = [files[i::self.num_workers] for i in range(self.num_workers)]
        for idx in range(self.num_workers):
            name = f"worker-{idx}"
            self.bus.register(name)
            worker = WorkerRuntime(name, self.bus, splits[idx])
            worker.start()
            self.workers.append(worker)

    def stop(self) -> None:
        for idx in range(self.num_workers):
            name = f"worker-{idx}"
            self.bus.send(name, ("STOP",))


