import abc
from typing import TYPE_CHECKING, Dict, List, Optional, Union, Iterator
from typing_extensions import Literal

from ray.data._internal.block_batching import BatchType
from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import tf
    import torch


@PublicAPI(stability="beta")
class DatasetIterator(abc.ABC):
    """An iterator for repeatedly reading a preprocessed dataset. Each
    iteration call represents one pass (epoch) over the dataset.

    If using Ray AIR, each trainer actor should get its own iterator by calling
    :meth:`session.get_dataset_shard("train")
    <ray.air.session.get_dataset_shard>`.

    Tip: For debugging purposes, use
    :meth:`~ray.air.util.check_ingest.make_local_dataset_iterator` to create a
    local `DatasetIterator` from a :class:`~ray.data.Dataset`, a
    :class:`~ray.data.Preprocessor`, and a :class:`~ray.air.DatasetConfig`.
    """

    @abc.abstractmethod
    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 256,
        batch_format: Literal["default", "numpy", "pandas"] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[BatchType]:
        """Return a local batched iterator over the dataset.

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            batch_format: The format in which to return each batch.
                Specify "default" to use the default block format (promoting
                tables to Pandas and tensors to NumPy), "pandas" to select
                ``pandas.DataFrame``, "pyarrow" to select ``pyarrow.Table``, or "numpy"
                to select ``numpy.ndarray`` for tensor datasets and
                ``Dict[str, numpy.ndarray]`` for tabular datasets. Default is "default".
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If non-None, the data will be randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer will be drained.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterator over record batches.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def iter_torch_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: Optional[str] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[TorchTensorBatchType]:
        """Return a local batched iterator of Torch Tensors over the dataset.

        This iterator will yield single-tensor batches if the underlying dataset
        consists of a single column; otherwise, it will yield a dictionary of
        column-tensors. If looking for more flexibility in the tensor conversion (e.g.
        casting dtypes) or the batch format, try using `.iter_batches` directly.

        Time complexity: O(1)

        Args:
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The Torch dtype(s) for the created tensor(s); if None, the dtype
                will be inferred from the tensor data.
            device: The device on which the tensor should be placed; if None, the Torch
                tensor will be constructed on the CPU.
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If non-None, the data will be randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer will be drained. This
                buffer size must be greater than or equal to ``batch_size``, and
                therefore ``batch_size`` must also be specified when using local
                shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterator over Torch Tensor batches.
        """
        return NotImplementedError

    def to_tf(
        self,
        feature_columns: Union[str, List[str]],
        label_columns: Union[str, List[str]],
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> "tf.data.Dataset":
        """Return a TF Dataset over this dataset.

        .. warning::
            If your dataset contains ragged tensors, this method errors. To prevent
            errors, resize tensors or
            :ref:`disable tensor extension casting <disable_tensor_extension_casting>`.

        Args:
            feature_columns: Columns that correspond to model inputs. If this is a
                string, the input data is a tensor. If this is a list, the input data
                is a ``dict`` that maps column names to their tensor representation.
            label_column: Columns that correspond to model targets. If this is a
                string, the target data is a tensor. If this is a list, the target data
                is a ``dict`` that maps column names to their tensor representation.
            prefetch_blocks: The number of blocks to prefetch ahead of the
                current block during the scan.
            batch_size: Record batch size. Defaults to 1.
            drop_last: Set to True to drop the last incomplete batch,
                if the dataset size is not divisible by the batch size. If
                False and the size of dataset is not divisible by the batch
                size, then the last batch will be smaller. Defaults to False.
            local_shuffle_buffer_size: If non-None, the data will be randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer will be drained. This
                buffer size must be greater than or equal to ``batch_size``, and
                therefore ``batch_size`` must also be specified when using local
                shuffling.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            A ``tf.data.Dataset`` that yields inputs and targets.
        """  # noqa: E501
        return NotImplementedError

    @abc.abstractmethod
    def stats(self) -> str:
        return NotImplementedError
