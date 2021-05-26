from collections import namedtuple

ByteCount = int
PartId = int
RecordCount = int
PartitionInfo = namedtuple("PartitionInfo", ["part_id", "node", "mnt", "path"])
