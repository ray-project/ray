from typing import List, Optional

from ray.data.datasource import (
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.image_datasource import _ImageFileMetadataProvider


def get_generic_metadata_provider(file_extensions: Optional[List[str]]):
    # Used by all other file-based `read_*` APIs
    return DefaultFileMetadataProvider()


def get_parquet_metadata_provider():
    # Used by `read_parquet`
    return DefaultParquetMetadataProvider()


def get_parquet_bulk_metadata_provider():
    # Used by `read_parquet_bulk`
    return FastFileMetadataProvider()


def get_image_metadata_provider():
    # Used by `read_images`
    return _ImageFileMetadataProvider()
