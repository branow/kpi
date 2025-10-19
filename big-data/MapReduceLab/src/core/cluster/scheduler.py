from typing import List


class Scheduler:
    def __init__(self, workers: List[str]):
        self.workers = workers
        self._idx = 0

    def next_worker(self) -> str:
        if not self.workers:
            raise RuntimeError("No workers available")
        name = self.workers[self._idx % len(self.workers)]
        self._idx += 1
        return name


