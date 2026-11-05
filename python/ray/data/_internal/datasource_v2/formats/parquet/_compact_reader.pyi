from typing import List, Optional, Tuple, Type, Union

from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    LeafSizeStats,
)

class ThriftDesync(Exception): ...

class CompactReader:
    def __init__(
        self,
        buf: Union[bytes, memoryview],
        leaf_stats: Optional[Type[LeafSizeStats]] = None,
    ) -> None: ...
    def read(
        self,
    ) -> Tuple[List[List[Optional[LeafSizeStats]]], List[List[Optional[int]]]]: ...
    def dictionary_page(self) -> Optional[Tuple[int, int]]: ...
