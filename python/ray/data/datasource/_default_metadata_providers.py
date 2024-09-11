import logging
from typing import List, Optional

from ray._private.ray_constants import env_bool
from ray.data._internal.datasource.image_datasource import (
    ImageDatasource,
    _ImageFileMetadataProvider,
)
from ray.data.datasource.file_meta_provider import (
    DefaultFileMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.datasource.parquet_meta_provider import ParquetMetadataProvider

ANYSCALE_METADATA_PROVIDER_ENABLED = env_bool(
    "ANYSCALE_METADATA_PROVIDER_ENABLED", True
)

PARQUET_METADATA_FETCHING_ENABLED = env_bool("PARQUET_METADATA_FETCHING_ENABLED", False)

logger = logging.getLogger(__name__)


def get_generic_metadata_provider(file_extensions: Optional[List[str]]):
    # Used by all other file-based `read_*` APIs
    from ray.anyscale.data import AnyscaleFileMetadataProvider

    if ANYSCALE_METADATA_PROVIDER_ENABLED:
        return AnyscaleFileMetadataProvider(file_extensions)
    else:
        return DefaultFileMetadataProvider()


def get_parquet_metadata_provider(override_num_blocks: Optional[int] = None):
    # Used by `read_parquet`
    from ray.anyscale.data import AnyscaleParquetMetadataProvider

    if override_num_blocks is not None:
        logger.warning(
            "You configured `override_num_blocks`. To produce the requested number of "
            "blocks, Ray Data fetches metadata for all Parquet files. This might be "
            "slow."
        )
        return ParquetMetadataProvider()
    elif PARQUET_METADATA_FETCHING_ENABLED:
        return ParquetMetadataProvider()
    else:
        # `AnyscaleParquetMetadataProvider` doesn't perform metadata fetching.
        return AnyscaleParquetMetadataProvider()


def get_parquet_bulk_metadata_provider():
    # Used by `read_parquet_bulk`
    return FastFileMetadataProvider()


def get_image_metadata_provider():
    # Used by `read_images`
    from ray.anyscale.data import AnyscaleFileMetadataProvider

    if ANYSCALE_METADATA_PROVIDER_ENABLED:
        return AnyscaleFileMetadataProvider(ImageDatasource._FILE_EXTENSIONS)
    else:
        return _ImageFileMetadataProvider()
