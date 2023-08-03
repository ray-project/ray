from ray.data.datasource import (
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.image_datasource import _ImageFileMetadataProvider

# Used by `read_parquet`
DEFAULT_PARQUET_META_PROVIDER = DefaultParquetMetadataProvider()
# Used by `read_images`
DEFAULT_IMAGE_META_PROVIDER = _ImageFileMetadataProvider()
# Used by `read_parquet_bulk`
DEFAULT_PARQUET_BULK_META_PROVIDER = FastFileMetadataProvider()
# Used by all other file-based `read_*` APIs
DEFAULT_GENERIC_META_PROVIDER = DefaultFileMetadataProvider()
