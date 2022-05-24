import logging
from typing import TYPE_CHECKING, Callable, Iterator, List

from ray.data.block import Block, BlockMetadata, T
from ray.data.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import tensorflow as tf

logger = logging.getLogger(__name__)


@PublicAPI
class SimpleTensorFlowDatasource(Datasource[T]):
    """A datasource that lets you use TensorFlow datasets with Ray Data.

    .. warning::
        ``SimpleTensorFlowDataset`` doesn't support parallel reads. You should only use
        this datasource for small datasets like MNIST or CIFAR.

    Example:
        >>> import ray.data
        >>> from ray.data.datasource import SimpleTensorFlowDatasource
        >>> import tensorflow_datasets as tfds  # doctest: +SKIP
        >>>
        >>> def dataset_factory():
        ...     return tfds.load("cifar10", split=["train"], as_supervised=True)[0]
        ...
        >>> dataset = ray.data.read_datasource(  # doctest: +SKIP
        ...     SimpleTensorFlowDatasource(),
        ...     parallelism=1,
        ...     dataset_factory=dataset_factory
        ... )
        >>> features, label = dataset.take(1)[0]  # doctest: +SKIP
        >>> features.shape  # doctest: +SKIP
        TensorShape([32, 32, 3])
        >>> label  # doctest: +SKIP
        <tf.Tensor: shape=(), dtype=int64, numpy=7>
    """

    def prepare_read(
        self,
        parallelism: int,
        dataset_factory: Callable[[], "tf.data.Dataset"],
    ) -> List[ReadTask]:
        """Return a read task that loads a TensorFlow dataset.

        Arguments:
            parallelism: This argument isn't used.
            dataset_factory: A no-argument function that returns the TensorFlow dataset
                to be read.
        """
        if parallelism > 1:
            logger.warn(
                "`SimpleTensorFlowDatasource` doesn't support parallel reads. The "
                "`parallelism` argument will be ignored."
            )

        def read_fn() -> Iterator[Block]:
            # Load the entire dataset into memory.
            block = list(dataset_factory())
            # Store the data in a single block.
            yield block

        metadata = BlockMetadata(
            num_rows=None,
            size_bytes=None,
            schema=None,
            input_files=None,
            exec_stats=None,
        )
        return [ReadTask(read_fn, metadata)]
