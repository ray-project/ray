# Short term workaround for https://github.com/ray-project/ray/issues/32435
# Dataset has a hard dependency on pandas, so it doesn't need to be delayed.
import pandas  # noqa
from pkg_resources._vendor.packaging.version import parse as parse_version

from ray._private.utils import _get_pyarrow_version
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
)
from ray.data._internal.progress_bar import set_progress_bars
from ray.data.context import DataContext, DatasetContext
from ray.data.dataset import Dataset, Schema
from ray.data.dataset_pipeline import DatasetPipeline
from ray.data.datasource import (
    BlockBasedFileDatasink,
    Datasink,
    Datasource,
    ReadTask,
    RowBasedFileDatasink,
)
from ray.data.iterator import DataIterator, DatasetIterator
from ray.data.preprocessor import Preprocessor
from ray.data.read_api import (  # noqa: F401
    from_arrow,
    from_arrow_refs,
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
    read_bigquery,
    read_binary_files,
    read_csv,
    read_databricks_tables,
    read_datasource,
    read_images,
    read_json,
    read_mongo,
    read_numpy,
    read_parquet,
    read_parquet_bulk,
    read_sql,
    read_text,
    read_tfrecords,
    read_webdataset,
)

# Module-level cached global functions for callable classes. It needs to be defined here
# since it has to be process-global across cloudpickled funcs.
_cached_fn = None
_cached_cls = None


try:
    import pyarrow as pa

    # https://github.com/apache/arrow/pull/38608 deprecated `PyExtensionType`, and
    # disabled it's deserialization by default. To ensure that users can load data
    # written with earlier version of Ray Data, we enable auto-loading of serialized
    # tensor extensions.
    pyarrow_version = _get_pyarrow_version()
    if not isinstance(pyarrow_version, str):
        # PyArrow is mocked in documentation builds. In this case, we don't need to do
        # anything.
        pass
    elif parse_version(pyarrow_version) >= parse_version("14.0.1"):
        pa.PyExtensionType.set_auto_load(True)
except ModuleNotFoundError:
    pass


__all__ = [
    "ActorPoolStrategy",
    "BlockBasedFileDatasink",
    "Dataset",
    "DataContext",
    "DatasetContext",  # Backwards compatibility alias.
    "DataIterator",
    "DatasetIterator",  # Backwards compatibility alias.
    "DatasetPipeline",
    "Datasink",
    "Datasource",
    "ExecutionOptions",
    "ExecutionResources",
    "NodeIdStr",
    "ReadTask",
    "RowBasedFileDatasink",
    "Schema",
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
    "read_text",
    "read_binary_files",
    "read_csv",
    "read_datasource",
    "read_images",
    "read_json",
    "read_numpy",
    "read_mongo",
    "read_parquet",
    "read_parquet_bulk",
    "read_sql",
    "read_tfrecords",
    "read_webdataset",
    "set_progress_bars",
    "Preprocessor",
]
