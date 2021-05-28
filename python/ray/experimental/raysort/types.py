from typing import NamedTuple, Tuple

ByteCount = int
NodeAddress = str
PartId = int
Path = str
RecordCount = int

ChunkInfo = Tuple[int, int]
PartitionInfo = NamedTuple(
    "PartitionInfo", [("part_id", PartId), ("node", NodeAddress), ("path", Path)]
)  # yapf: disable
