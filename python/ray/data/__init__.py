import ray
from ray.data._internal.arrow_serialization import (
    _register_arrow_json_readoptions_serializer,
)
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.progress_bar import set_progress_bars
from ray.data.dataset import Dataset
from ray.data.dataset_pipeline import DatasetPipeline
from ray.data.datasource import Datasource, ReadTask
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
    range,
    range_arrow,
    range_table,
    range_tensor,
    read_binary_files,
    read_csv,
    read_datasource,
    read_json,
    read_numpy,
    read_parquet,
    read_parquet_bulk,
    read_text,
)

# Register custom Arrow JSON ReadOptions serializer after worker has initialized.
if ray.is_initialized():
    _register_arrow_json_readoptions_serializer()
else:
    pass
#    ray._internal.worker._post_init_hooks.append(_register_arrow_json_readoptions_serializer)

__all__ = [
    "ActorPoolStrategy",
    "Dataset",
    "DatasetPipeline",
    "Datasource",
    "ReadTask",
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
    "from_huggingface",
    "range",
    "range_table",
    "range_tensor",
    "read_text",
    "read_binary_files",
    "read_csv",
    "read_datasource",
    "read_json",
    "read_numpy",
    "read_parquet",
    "read_parquet_bulk",
    "set_progress_bars",
    "Preprocessor",
]
