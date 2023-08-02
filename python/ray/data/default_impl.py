from ray.data.datasource import (
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.image_datasource import _ImageFileMetadataProvider

DEFAULT_PARQUET_METADATA_PROVIDER = DefaultParquetMetadataProvider()
DEFAULT_IMAGE_METADATA_PROVIDER = _ImageFileMetadataProvider()
DEFAULT_BULK_PARQUET_METADATA_PROVIDER = FastFileMetadataProvider()
DEFAULT_GENERIC_METADATA_PROVIDER = DefaultFileMetadataProvider()
