from typing import Iterator, Optional, Union, TYPE_CHECKING
import warnings

from ray.data.block import DataBatch
from ray.data.dataset_iterator import DatasetIterator
from ray.train.error import SessionMisuseError

if TYPE_CHECKING:
    import tensorflow as tf
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType
    from ray.data.dataset import Dataset
    from ray.data.dataset_pipeline import DatasetPipeline


class TrainDatasetIterator(DatasetIterator):
    """A DatasetIterator with Ray Train specific logic.

    Args:
        dataset_iterator: The base dataset iterator.
    """

    def __init__(
        self,
        dataset_iterator: DatasetIterator,
    ):
        self._dataset_iterator = dataset_iterator

    def iter_batches(self, *args, **kwargs) -> Iterator["DataBatch"]:
        return self._dataset_iterator.iter_batches(*args, **kwargs)

    def iter_torch_batches(
        self, *, device: Optional[str] = None, **kwargs
    ) -> Iterator["TorchTensorBatchType"]:

        # Automatically move torch tensors to the appropriate device.
        if device is None:
            from ray.train.torch import get_device

            try:
                device = get_device()
            except SessionMisuseError:
                pass

        if isinstance(device, list):
            device = device[0]

        return self._dataset_iterator.iter_torch_batches(device=device, **kwargs)

    def to_tf(self, *args, **kwargs) -> "tf.data.Dataset":
        return self._dataset_iterator.to_tf(*args, **kwargs)

    def stats(self) -> str:
        return self._dataset_iterator.stats()

    @property
    def _base_dataset_or_pipeline(self) -> Union["Dataset", "DatasetPipeline"]:
        return self._dataset_iterator._base_dataset_or_pipeline

    def __getattr__(self, name):
        if name == "_dataset_iterator":
            raise AttributeError

        if hasattr(self._dataset_iterator, name):
            return getattr(self._dataset_iterator, name)

        # Warning for backwards compatibility.
        warnings.warn(
            "session.get_dataset_shard returns a ray.data.DatasetIterator "
            "instead of a Dataset/DatasetPipeline as of Ray v2.3. "
            "Use iter_torch_batches(), to_tf(), or iter_batches() to "
            "iterate over one epoch. See "
            "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
            "for full DatasetIterator docs."
        )

        return getattr(self._base_dataset_or_pipeline, name)
