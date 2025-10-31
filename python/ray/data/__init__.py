# Short term workaround for https://github.com/ray-project/ray/issues/32435
# Dataset has a hard dependency on pandas, so it doesn't need to be delayed.
import pandas  # noqa
from packaging.version import parse as parse_version

from ray._private.arrow_utils import get_pyarrow_version

from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.datasource.tfrecords_datasource import TFXReadOptions
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
)
from ray.data._internal.logging import configure_logging
from ray.data.context import DataContext, DatasetContext
from ray.data.dataset import Dataset, Schema, ClickHouseTableSettings
from ray.data._internal.datasource.clickhouse_datasink import SinkMode
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
    read_mcap,
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

# Import SQL API through the public ray.data.sql module
# This provides a clean public interface without exposing experimental paths
try:
    # Import the sql module - users can do: from ray.data.sql import ...
    from ray.data import sql  # noqa: F401

    # Also make key functions available at ray.data level for convenience
    from ray.data.sql import (
        clear_tables,
        list_tables,
        register,
        sql as _sql_func,
    )

    # Make sql() callable as both ray.data.sql() and ray.data.sql.sql()
    sql = _sql_func  # noqa: F811
except ImportError:
    # SQL module not available, skip import
    pass

# Module-level cached global functions for callable classes. It needs to be defined here
# since it has to be process-global across cloudpickled funcs.
_map_actor_context = None

configure_logging()

try:
    import pyarrow as pa

    # Import these arrow extension types to ensure that they are registered.
    from ray.air.util.tensor_extensions.arrow import (  # noqa
        ArrowTensorType,
        ArrowVariableShapedTensorType,
    )

    # https://github.com/apache/arrow/pull/38608 deprecated `PyExtensionType`, and
    # disabled it's deserialization by default. To ensure that users can load data
    # written with earlier version of Ray Data, we enable auto-loading of serialized
    # tensor extensions.
    #
    # NOTE: `PyExtensionType` is deleted from Arrow >= 21.0
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is None or pyarrow_version >= parse_version("21.0.0"):
        pass
    else:
        from ray._private.ray_constants import env_bool

        RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE = env_bool(
            "RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE", False
        )

        if (
            pyarrow_version >= parse_version("14.0.1")
            and RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE
        ):
            pa.PyExtensionType.set_auto_load(True)

except ModuleNotFoundError:
    pass


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
    "TaskPoolStrategy",
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
    "read_mcap",
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
    # Simplified SQL API exports
    "sql",
    "register",
    "list_tables",
    "clear_tables",
    "sql_config",
    # SQL exceptions
    "SQLError",
    "SQLParseError",
    "SQLExecutionError",
    "TableNotFoundError",
    "ColumnNotFoundError",
]
