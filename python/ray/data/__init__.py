# Short term workaround for https://github.com/ray-project/ray/issues/32435
# Dataset has a hard dependency on pandas, so it doesn't need to be delayed.
import pandas  # noqa

from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.datasource.tfrecords_datasource import TFXReadOptions
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
)
from ray.data._internal.logging import configure_logging
from ray.data.context import DataContext, DatasetContext
from ray.data.dataset import Dataset, Schema, SinkMode, ClickHouseTableSettings
from ray.data.datasource import (
    BlockBasedFileDatasink,
    Datasink,
    Datasource,
    FileShuffleConfig,
    ReadTask,
    RowBasedFileDatasink,
)
from ray.data.iterator import DataIterator, DatasetIterator
from ray.data.preprocessor import Preprocessor
from ray.data.read_api import (  # noqa: F401
    from_arrow,
    from_arrow_refs,
    from_blocks,
    from_daft,
    from_dask,
    from_huggingface,
    from_items,
    from_mars,
    from_modin,
    from_numpy,
    from_numpy_refs,
    from_pandas,
    from_pandas_refs,
    from_spark,
    from_tf,
    from_torch,
    range,
    range_tensor,
    read_audio,
    read_avro,
    read_bigquery,
    read_binary_files,
    read_clickhouse,
    read_csv,
    read_databricks_tables,
    read_datasource,
    read_delta,
    read_delta_sharing_tables,
    read_hudi,
    read_iceberg,
    read_images,
    read_json,
    read_lance,
    read_mongo,
    read_numpy,
    read_parquet,
    read_parquet_bulk,
    read_snowflake,
    read_sql,
    read_text,
    read_tfrecords,
    read_unity_catalog,
    read_videos,
    read_webdataset,
)

# Module-level cached global functions for callable classes. It needs to be defined here
# since it has to be process-global across cloudpickled funcs.
_map_actor_context = None

configure_logging()


__all__ = [
    "ActorPoolStrategy",
    "BlockBasedFileDatasink",
    "ClickHouseTableSettings",
    "Dataset",
    "DataContext",
    "DatasetContext",  # Backwards compatibility alias.
    "DataIterator",
    "DatasetIterator",  # Backwards compatibility alias.
    "Datasink",
    "Datasource",
    "ExecutionOptions",
    "ExecutionResources",
    "FileShuffleConfig",
    "NodeIdStr",
    "ReadTask",
    "RowBasedFileDatasink",
    "Schema",
    "SinkMode",
    "from_daft",
    "from_dask",
    "from_items",
    "from_arrow",
    "from_arrow_refs",
    "from_mars",
    "from_modin",
    "from_numpy",
    "from_numpy_refs",
    "from_pandas",
    "from_pandas_refs",
    "from_spark",
    "from_tf",
    "from_torch",
    "from_huggingface",
    "range",
    "range_tensor",
    "read_audio",
    "read_avro",
    "read_text",
    "read_binary_files",
    "read_clickhouse",
    "read_csv",
    "read_datasource",
    "read_delta",
    "read_delta_sharing_tables",
    "read_hudi",
    "read_iceberg",
    "read_images",
    "read_json",
    "read_lance",
    "read_numpy",
    "read_mongo",
    "read_parquet",
    "read_parquet_bulk",
    "read_snowflake",
    "read_sql",
    "read_tfrecords",
    "read_unity_catalog",
    "read_videos",
    "read_webdataset",
    "Preprocessor",
    "TFXReadOptions",
]
