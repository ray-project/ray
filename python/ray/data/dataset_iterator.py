import abc
from typing import TYPE_CHECKING, Dict, List, Optional, Union, Iterator
from typing_extensions import Literal

from ray.data._internal.block_batching import BatchType
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import tf
    import torch
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType


@PublicAPI(stability="beta")
class DatasetIterator(abc.ABC):
    """An iterator for reading items from a :class:`~Dataset` or
    :class:`~DatasetPipeline`.

    For Datasets, each iteration call represents a complete read of all items in the
    Dataset. For DatasetPipelines, each iteration call represents one pass (epoch)
    over the Dataset. Note that for DatasetPipelines, each pass iterates over
    the original Dataset, instead of a window (if `.window()` was used).

    If using Ray AIR, each trainer actor should get its own iterator by calling
    :meth:`session.get_dataset_shard("train")
    <ray.air.session.get_dataset_shard>`.

    Examples:
        >>> import ray
        >>> ds = ray.data.range(5)
        >>> ds
        Dataset(num_blocks=5, num_rows=5, schema=<class 'int'>)
        >>> ds.iterator()
        DatasetIterator(Dataset(num_blocks=5, num_rows=5, schema=<class 'int'>))
        >>> ds = ds.repeat(); ds
        DatasetPipeline(num_windows=inf, num_stages=2)
        >>> ds.iterator()
        DatasetIterator(DatasetPipeline(num_windows=inf, num_stages=2))

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

        Examples:
            >>> import ray
            >>> for batch in ray.data.range(
            ...     1000000
            ... ).iterator().iter_batches(): # doctest: +SKIP
            ...     print(batch) # doctest: +SKIP

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
    ) -> Iterator["TorchTensorBatchType"]:
        """Return a local batched iterator of Torch Tensors over the dataset.

        This iterator will yield single-tensor batches if the underlying dataset
        consists of a single column; otherwise, it will yield a dictionary of
        column-tensors. If looking for more flexibility in the tensor conversion (e.g.
        casting dtypes) or the batch format, try using `.iter_batches` directly.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range( # doctest: +SKIP
            ...     12,
            ... ).iterator().iter_torch_batches(batch_size=4):
            ...     print(batch.shape) # doctest: +SKIP
            torch.Size([4, 1])
            torch.Size([4, 1])
            torch.Size([4, 1])

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
        raise NotImplementedError

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

        Examples:
            >>> import ray
            >>> it = ray.data.read_csv(
            ...     "s3://anonymous@air-example-data/iris.csv"
            ... ).iterator()
            >>> it
            DatasetIterator(Dataset(num_blocks=1, num_rows=150, schema={sepal length (cm): double, sepal width (cm): double, petal length (cm): double, petal width (cm): double, target: int64}))

            If your model accepts a single tensor as input, specify a single feature column.

            >>> it.to_tf(feature_columns="sepal length (cm)", label_columns="target")  # doctest: +SKIP
            <_OptionsDataset element_spec=(TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your model accepts a dictionary as input, specify a list of feature columns.

            >>> it.to_tf(["sepal length (cm)", "sepal width (cm)"], "target")  # doctest: +SKIP
            <_OptionsDataset element_spec=({'sepal length (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), 'sepal width (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal width (cm)')}, TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your dataset contains multiple features but your model accepts a single
            tensor as input, combine features with
            :class:`~ray.data.preprocessors.Concatenator`.

            >>> from ray.data.preprocessors import Concatenator
            >>> preprocessor = Concatenator(output_column_name="features", exclude="target")
            >>> it = preprocessor.transform(ds).iterator()
            >>> it
            DatasetIterator(Dataset(num_blocks=1, num_rows=150, schema={target: int64, features: TensorDtype(shape=(4,), dtype=float64)}))
            >>> it.to_tf("features", "target")  # doctest: +SKIP
            <_OptionsDataset element_spec=(TensorSpec(shape=(None, 4), dtype=tf.float64, name='features'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

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
        raise NotImplementedError

    @abc.abstractmethod
    def stats(self) -> str:
        """Returns a string containing execution timing information."""
        raise NotImplementedError

    def iter_epochs(self, max_epoch: int = -1) -> None:
        raise DeprecationWarning(
            "If you are using AIR, note that session.get_dataset_shard() "
            "returns a ray.data.DatasetIterator instead of a "
            "DatasetPipeline as of Ray 2.3. "
            "To iterate over one epoch of data, use iter_batches(), "
            "iter_torch_batches(), or to_tf()."
        )

    @abc.abstractmethod
    def _with_backward_compat(self) -> "DatasetIterator":
        """
        Provide backwards compatibility for AIR users.
        """
        raise NotImplementedError
