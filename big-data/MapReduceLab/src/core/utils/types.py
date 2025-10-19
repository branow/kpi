from typing import Any, Iterable, Tuple, Dict, List


Key = Any
Value = Any
Record = Any
KeyValue = Tuple[Key, Value]
ShardId = int
ShardToGrouped = Dict[ShardId, Dict[Key, List[Value]]]


