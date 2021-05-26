from typing import NamedTuple

ByteCount = int
NodeAddress = str
PartId = int
Path = str
RecordCount = int

PartitionInfo = NamedTuple("PartitionInfo", [("part_id", PartId),
                                             ("node", NodeAddress),
                                             ("mnt", Path), ("path", Path)])
