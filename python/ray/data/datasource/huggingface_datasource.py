from typing import TYPE_CHECKING, Iterable, List, Optional, Union

from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import Datasource, Reader, ReadTask
from ray.util.annotations import DeveloperAPI

logger = DatasetLogger(__name__)

if TYPE_CHECKING:
    import datasets


@DeveloperAPI
class HuggingFaceDatasource(Datasource):
    """Hugging Face Dataset datasource, for reading from a
    `Hugging Face Datasets Dataset <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.Dataset/>`_.
    This Datasource implements a streamed read, most beneficial for a
    `Hugging Face Datasets IterableDataset <https://huggingface.co/docs/datasets/package_reference/main_classes#datasets.IterableDataset/>`_
    or datasets which are too large to fit in-memory.
    For an in-memory Hugging Face Dataset (`datasets.Dataset`), use :meth:`~ray.data.from_huggingface`
    for faster performance.
    """  # noqa: E501

    def create_reader(
        self,
        dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
    ) -> "_HuggingFaceDatasourceReader":
        return _HuggingFaceDatasourceReader(dataset)


class _HuggingFaceDatasourceReader(Reader):
    def __init__(
        self,
        dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
        batch_size: int = 4096 * 64,
    ):
        self._dataset = dataset
        self._batch_size = batch_size

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._dataset.dataset_size

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        # Note that `parallelism` arg is currently not used for HuggingFaceDatasource.
        # We always generate a single ReadTask.
        _check_pyarrow_version()
        import pyarrow

        def _read_shard(dataset: "datasets.IterableDataset") -> Iterable[Block]:
            for batch in dataset.with_format("arrow").iter(batch_size=self._batch_size):
                block = pyarrow.Table.from_pydict(batch)
                yield block

        # TODO(scottjlee): figure out how to properly get metadata estimates,
        # so progress bars have meaning.
        meta = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=None,
            input_files=None,
            exec_stats=None,
        )
        # TODO(scottjlee): figure out how to properly shard HF dataset, so we can
        # use multiple ReadTasks.
        read_tasks: List[ReadTask] = [
            ReadTask(
                lambda shard=self._dataset: _read_shard(shard),
                meta,
            )
        ]
        return read_tasks
