from typing import NamedTuple, Tuple

ByteCount = int
NodeAddress = str
PartId = int
Path = str
RecordCount = int

BlockInfo = Tuple[int, int]


class PartInfo(NamedTuple):
    part_id: PartId
    node: NodeAddress
    path: Path

    def __repr__(self):
        return f"Part({self.node}:{self.path})"
