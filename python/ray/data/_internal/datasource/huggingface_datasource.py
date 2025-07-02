import sys
from typing import TYPE_CHECKING, Iterable, List, Optional, Union

from ray.air.util.tensor_extensions.arrow import pyarrow_table_from_pydict
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.dataset import Dataset
from ray.data.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import datasets


TRANSFORMERS_IMPORT_ERROR: Optional[ImportError] = None

try:
    # Due to HF Dataset's dynamic module system, we need to dynamically import the
    # datasets_modules module on every actor when training.
    # We accomplish this by simply running the following bit of code directly
    # in the module you are currently viewing. This ensures that when we
    # unpickle the Dataset, it runs before pickle tries to
    # import datasets_modules and prevents an exception from being thrown.
    # Same logic is present inside HF Transformers Ray
    # integration: https://github.com/huggingface/transformers/blob/\
    # 7d5fde991d598370d961be8cb7add6541e2b59ce/src/transformers/integrations.py#L271
    # Also see https://github.com/ray-project/ray/issues/28084
    from transformers.utils import is_datasets_available

    if "datasets_modules" not in sys.modules and is_datasets_available():
        import importlib
        import os

        import datasets.load

        dynamic_modules_path = os.path.join(
            datasets.load.init_dynamic_modules(), "__init__.py"
        )
        # load dynamic_modules from path
        spec = importlib.util.spec_from_file_location(
            "datasets_modules", dynamic_modules_path
        )
        datasets_modules = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = datasets_modules
        spec.loader.exec_module(datasets_modules)
except ImportError as e:
    TRANSFORMERS_IMPORT_ERROR = e


class HuggingFaceDatasource(Datasource):
    """Hugging Face Dataset datasource, for reading from a
    `Hugging Face Datasets Dataset <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.Dataset/>`_.
    This Datasource implements a streamed read using a
    single read task, most beneficial for a
    `Hugging Face Datasets IterableDataset <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.IterableDataset/>`_
    or datasets which are too large to fit in-memory.
    For an in-memory Hugging Face Dataset (`datasets.Dataset`), use :meth:`~ray.data.from_huggingface`
    directly for faster performance.
    """  # noqa: E501

    def __init__(
        self,
        dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
        batch_size: int = 4096,
    ):
        if TRANSFORMERS_IMPORT_ERROR is not None:
            raise TRANSFORMERS_IMPORT_ERROR

        self._dataset = dataset
        self._batch_size = batch_size

    @classmethod
    def list_parquet_urls_from_dataset(
        cls, dataset: Union["datasets.Dataset", "datasets.IterableDataset"]
    ) -> Dataset:
        """Return list of Hugging Face hosted parquet file URLs if they
        exist for the data (i.e. if the dataset is a public dataset that
        has not been transformed) else return an empty list."""
        import datasets

        # We can use the dataset name, config name, and split name to load
        # public hugging face datasets from the Hugging Face Hub. More info
        # here: https://huggingface.co/docs/datasets-server/parquet
        dataset_name = dataset.info.dataset_name
        config_name = dataset.info.config_name
        split_name = str(dataset.split)

        # If a dataset is not an iterable dataset, we will check if the
        # dataset with the matching dataset name, config name, and split name
        # on the Hugging Face Hub has the same fingerprint as the
        # dataset passed into this function. If it is not matching, transforms
        # or other operations have been performed so we cannot use the parquet
        # files on the Hugging Face Hub, so we return an empty list.
        if not isinstance(dataset, datasets.IterableDataset):
            from datasets import load_dataset

            try:
                ds = load_dataset(dataset_name, config_name, split=split_name)
                if ds._fingerprint != dataset._fingerprint:
                    return []
            except Exception:
                # If an exception is thrown when trying to reload the dataset
                # we should exit gracefully by returning an empty list.
                return []

        import requests

        public_url = (
            f"https://huggingface.co/api/datasets/{dataset_name}"
            f"/parquet/{config_name}/{split_name}"
        )
        resp = requests.get(public_url)
        if resp.status_code == requests.codes["ok"]:
            # dataset corresponds to a public dataset, return list of parquet_files
            return resp.json()
        else:
            return []

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._dataset.dataset_size

    def _read_dataset(self) -> Iterable[Block]:
        # Note: This is a method instead of a higher level function because
        # we need to capture `self`. This will trigger the try-import logic at
        # the top of file to avoid import error of dataset_modules.
        import numpy as np
        import pandas as pd
        import pyarrow

        for batch in self._dataset.with_format("arrow").iter(
            batch_size=self._batch_size
        ):
            # HuggingFace IterableDatasets do not fully support methods like
            # `set_format`, `with_format`, and `formatted_as`, so the dataset
            # can return whatever is the default configured batch type, even if
            # the format is manually overriden before iterating above.
            # Therefore, we limit support to batch formats which have native
            # block types in Ray Data (pyarrow.Table, pd.DataFrame),
            # or can easily be converted to such (dict, np.array).
            # See: https://github.com/huggingface/datasets/issues/3444
            if not isinstance(batch, (pyarrow.Table, pd.DataFrame, dict, np.array)):
                raise ValueError(
                    f"Batch format {type(batch)} isn't supported. Only the "
                    f"following batch formats are supported: "
                    f"dict (corresponds to `None` in `dataset.with_format()`), "
                    f"pyarrow.Table, np.array, pd.DataFrame."
                )
            # Ensure np.arrays are wrapped in a dict
            # (subsequently converted to a pyarrow.Table).
            if isinstance(batch, np.ndarray):
                batch = {"item": batch}
            if isinstance(batch, dict):
                batch = pyarrow_table_from_pydict(batch)
            # Ensure that we return the default block type.
            block = BlockAccessor.for_block(batch).to_default()
            yield block

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        # Note: `parallelism` arg is currently not used by HuggingFaceDatasource.
        # We always generate a single ReadTask to perform the read.
        _check_pyarrow_version()

        # TODO(scottjlee): IterableDataset doesn't provide APIs
        # for getting number of rows, byte size, etc., so the
        # BlockMetadata is currently empty. Properly retrieve
        # or calculate these so that progress bars have meaning.
        meta = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            input_files=None,
            exec_stats=None,
        )
        read_tasks: List[ReadTask] = [
            ReadTask(
                self._read_dataset,
                meta,
            )
        ]
        return read_tasks
