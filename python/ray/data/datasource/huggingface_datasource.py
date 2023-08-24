import sys
from typing import TYPE_CHECKING, Iterable, List, Optional, Union

from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import Datasource, Reader, ReadTask
from ray.util.annotations import DeveloperAPI

logger = DatasetLogger(__name__)

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
    # Same logic is present inside Ray's TransformersTrainer and HF Transformers Ray
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


@DeveloperAPI
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

    def create_reader(
        self,
        dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
    ) -> "_HuggingFaceDatasourceReader":
        if TRANSFORMERS_IMPORT_ERROR is not None:
            raise TRANSFORMERS_IMPORT_ERROR
        return _HuggingFaceDatasourceReader(dataset)


class _HuggingFaceDatasourceReader(Reader):
    def __init__(
        self,
        dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
        batch_size: int = 4096,
    ):
        self._dataset = dataset
        self._batch_size = batch_size

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._dataset.dataset_size

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        # Note: `parallelism` arg is currently not used by HuggingFaceDatasource.
        # We always generate a single ReadTask to perform the read.
        _check_pyarrow_version()
        import pyarrow

        def _read_dataset(dataset: "datasets.IterableDataset") -> Iterable[Block]:
            for batch in dataset.with_format("arrow").iter(batch_size=self._batch_size):
                block = pyarrow.Table.from_pydict(batch)
                yield block

        # TODO(scottjlee): IterableDataset doesn't provide APIs
        # for getting number of rows, byte size, etc., so the
        # BlockMetadata is currently empty. Properly retrieve
        # or calculate these so that progress bars have meaning.
        meta = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=None,
            input_files=None,
            exec_stats=None,
        )
        read_tasks: List[ReadTask] = [
            ReadTask(
                lambda hfds=self._dataset: _read_dataset(hfds),
                meta,
            )
        ]
        return read_tasks
