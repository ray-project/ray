from typing import List, Optional

from ray.data._internal.datasource.image_datasource import _ImageFileMetadataProvider
from ray.data.datasource.file_meta_provider import (
    DefaultFileMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.parquet_meta_provider import ParquetMetadataProvider


def get_generic_metadata_provider(file_extensions: Optional[List[str]]):
    # Used by all other file-based `read_*` APIs
    return DefaultFileMetadataProvider()


def get_parquet_metadata_provider(override_num_blocks: Optional[int] = None):
    # Used by `read_parquet`
    return ParquetMetadataProvider()


def get_parquet_bulk_metadata_provider():
    # Used by `read_parquet_bulk`
    return FastFileMetadataProvider()


def get_image_metadata_provider():
    # Used by `read_images`
    return _ImageFileMetadataProvider()
