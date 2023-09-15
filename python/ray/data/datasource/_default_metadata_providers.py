from typing import List, Optional

from ray._private.ray_constants import env_bool
from ray.data.datasource import (
    DefaultFileMetadataProvider,
    DefaultParquetMetadataProvider,
    FastFileMetadataProvider,
    ImageDatasource,
)
from ray.data.datasource.image_datasource import _ImageFileMetadataProvider

ANYSCALE_METADATA_PROVIDER_ENABLED = env_bool(
    "ANYSCALE_METADATA_PROVIDER_ENABLED", True
)


def get_generic_metadata_provider(file_extensions: Optional[List[str]]):
    # Used by all other file-based `read_*` APIs
    from ray.anyscale.data import AnyscaleFileMetadataProvider

    if ANYSCALE_METADATA_PROVIDER_ENABLED:
        return AnyscaleFileMetadataProvider(file_extensions)
    else:
        return DefaultFileMetadataProvider()


def get_parquet_metadata_provider():
    # Used by `read_parquet`
    return DefaultParquetMetadataProvider()


def get_parquet_bulk_metadata_provider():
    # Used by `read_parquet_bulk`
    return FastFileMetadataProvider()


def get_image_metadata_provider():
    # Used by `read_images`
    from ray.anyscale.data import AnyscaleFileMetadataProvider

    if ANYSCALE_METADATA_PROVIDER_ENABLED:
        return AnyscaleFileMetadataProvider(ImageDatasource._FILE_EXTENSION)
    else:
        return _ImageFileMetadataProvider()
