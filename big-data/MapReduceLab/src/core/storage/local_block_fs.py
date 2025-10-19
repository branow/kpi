from pathlib import Path
from typing import Iterable, List


class LocalBlockFileSystem:
    def __init__(self, assigned_files: List[Path]):
        self.assigned_files = assigned_files

    def read_records(self) -> Iterable[str]:
        for file_path in self.assigned_files:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    yield line.rstrip("\n")


