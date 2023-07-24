# Short term workaround for https://github.com/ray-project/ray/issues/32435
# Dataset has a hard dependency on pandas, so it doesn't need to be delayed.
import pandas  # noqa

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
from ray.data.datasource import Datasource, ReadTask
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
    range_table,
    range_tensor,
    read_binary_files,
    read_csv,
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

__all__ = [
    "ActorPoolStrategy",
    "Dataset",
    "DataContext",
    "DatasetContext",  # Backwards compatibility alias.
    "DataIterator",
    "DatasetIterator",  # Backwards compatibility alias.
    "DatasetPipeline",
    "Datasource",
    "ExecutionOptions",
    "ExecutionResources",
    "NodeIdStr",
    "ReadTask",
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
    "range_table",
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
