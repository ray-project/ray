from typing import TYPE_CHECKING, List, Optional

from ray.data.datasource.parquet_meta_provider import (
    ParquetMetadataProvider,
    _ParquetFileFragmentMetaData,
)

if TYPE_CHECKING:
    import pyarrow


class AnyscaleParquetMetadataProvider(ParquetMetadataProvider):
    def prefetch_file_metadata(
        self,
        fragments: List["pyarrow.dataset.ParquetFileFragment"],
        **ray_remote_args,
    ) -> Optional[List[_ParquetFileFragmentMetaData]]:
        return None
