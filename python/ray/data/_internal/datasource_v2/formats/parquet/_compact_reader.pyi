from typing import List, Optional, Type, Union

from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    LeafSizeStats,
)

class ThriftDesync(Exception): ...

class CompactReader:
    def __init__(
        self, buf: Union[bytes, memoryview], leaf_stats: Type[LeafSizeStats]
    ) -> None: ...
    def read(self) -> List[List[Optional[LeafSizeStats]]]: ...
