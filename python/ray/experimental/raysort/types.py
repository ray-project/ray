from typing import NamedTuple

ByteCount = int
NodeAddress = str
PartId = int
Path = str
RecordCount = int

PartitionInfo = NamedTuple(
    "PartitionInfo", [("part_id", PartId), ("node", NodeAddress), ("path", Path)]
)  # yapf: disable
