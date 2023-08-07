from ray.data.datasource import (
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.image_datasource import _ImageFileMetadataProvider

# Used by `read_parquet`
DEFAULT_PARQUET_METADATA_PROVIDER = DefaultParquetMetadataProvider()
# Used by `read_images`
DEFAULT_IMAGE_METADATA_PROVIDER = _ImageFileMetadataProvider()
# Used by `read_parquet_bulk`
DEFAULT_BULK_PARQUET_METADATA_PROVIDER = FastFileMetadataProvider()
# Used by all other file-based `read_*` APIs
DEFAULT_GENERIC_METADATA_PROVIDER = DefaultFileMetadataProvider()


__all__ = [
    "DEFAULT_PARQUET_METADATA_PROVIDER",
    "DEFAULT_IMAGE_METADATA_PROVIDER",
    "DEFAULT_BULK_PARQUET_METADATA_PROVIDER",
    "DEFAULT_GENERIC_METADATA_PROVIDER",
]
