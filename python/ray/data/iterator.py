import abc
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from ray.data._internal.block_batching.iter_batches import iter_batches
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.optimizers import LogicalPlan
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats, StatsManager
from ray.data.block import BlockAccessor, DataBatch, _apply_batch_format
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import tensorflow as tf
    import torch

    from ray.data.dataset import (
        CollatedData,
        MaterializedDataset,
        Schema,
        TensorFlowTensorBatchType,
        TorchBatchType,
    )


T = TypeVar("T")


class _IterableFromIterator(Iterable[T]):
    def __init__(self, iterator_gen: Callable[[], Iterator[T]]):
        """Constructs an Iterable from an iterator generator.

        Args:
            iterator_gen: A function that returns an iterator each time it
                is called. For example, this can be a generator function.
        """
        self.iterator_gen = iterator_gen

    def __iter__(self):
        return self.iterator_gen()


@PublicAPI
class DataIterator(abc.ABC):
    """An iterator for reading records from a :class:`~Dataset`.

    For Datasets, each iteration call represents a complete read of all items in the
    Dataset.

    If using Ray Train, each trainer actor should get its own iterator by calling
    :meth:`ray.train.get_dataset_shard("train")
    <ray.train.get_dataset_shard>`.

    Examples:
        >>> import ray
        >>> ds = ray.data.range(5)
        >>> ds
        Dataset(num_rows=5, schema={id: int64})
        >>> ds.iterator()
        DataIterator(Dataset(num_rows=5, schema={id: int64}))
    """

    @abc.abstractmethod
    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], bool]:
        """Returns the iterator to use for `iter_batches`.

        Returns:
            A tuple. The first item of the tuple is an iterator over RefBundles.
            The second item of the tuple is a DatasetStats object used for recording
            stats during iteration.
            The third item is a boolean indicating if the blocks can be safely cleared
            after use.
        """
        raise NotImplementedError

    @PublicAPI
    def iter_batches(
        self,
        *,
        prefetch_batches: int = 1,
        batch_size: int = 256,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        _collate_fn: Optional[Callable[[DataBatch], "CollatedData"]] = None,
        _finalize_fn: Optional[Callable[[Any], Any]] = None,
    ) -> Iterable[DataBatch]:
        """Return a batched iterable over the dataset.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range(
            ...     1000000
            ... ).iterator().iter_batches(): # doctest: +SKIP
            ...     print(batch) # doctest: +SKIP

        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            batch_format: Specify ``"default"`` to use the default block format
                (NumPy), ``"pandas"`` to select ``pandas.DataFrame``, "pyarrow" to
                select ``pyarrow.Table``, or ``"numpy"`` to select
                ``Dict[str, numpy.ndarray]``, or None to return the underlying block
                exactly as is with no additional formatting.
            drop_last: Whether to drop the last batch if it's incomplete.
            local_shuffle_buffer_size: If non-None, the data will be randomly shuffled
                using a local in-memory shuffle buffer, and this value will serve as the
                minimum number of rows that must be in the local in-memory shuffle
                buffer in order to yield a batch. When there are no more rows to add to
                the buffer, the remaining rows in the buffer will be drained.
            local_shuffle_seed: The seed to use for the local random shuffle.

        Returns:
            An iterable over record batches.
        """
        batch_format = _apply_batch_format(batch_format)

        def _create_iterator() -> Iterator[DataBatch]:
            time_start = time.perf_counter()
            # Iterate through the dataset from the start each time
            # _iterator_gen is called.
            # This allows multiple iterations of the dataset without
            # needing to explicitly call `iter_batches()` multiple times.
            (
                ref_bundles_iterator,
                stats,
                blocks_owned_by_consumer,
            ) = self._to_ref_bundle_iterator()

            iterator = iter(
                iter_batches(
                    ref_bundles_iterator,
                    stats=stats,
                    clear_block_after_read=blocks_owned_by_consumer,
                    batch_size=batch_size,
                    batch_format=batch_format,
                    drop_last=drop_last,
                    collate_fn=_collate_fn,
                    finalize_fn=_finalize_fn,
                    shuffle_buffer_min_size=local_shuffle_buffer_size,
                    shuffle_seed=local_shuffle_seed,
                    prefetch_batches=prefetch_batches,
                )
            )

            dataset_tag = self._get_dataset_tag()

            if stats:
                stats.iter_initialize_s.add(time.perf_counter() - time_start)

            for batch in iterator:
                yield batch
                StatsManager.update_iteration_metrics(stats, dataset_tag)
            StatsManager.clear_iteration_metrics(dataset_tag)

            if stats:
                stats.iter_total_s.add(time.perf_counter() - time_start)

        return _IterableFromIterator(_create_iterator)

    def _get_dataset_tag(self) -> str:
        return "unknown_dataset"

    @PublicAPI
    def iter_rows(self) -> Iterable[Dict[str, Any]]:
        """Return a local row iterable over the dataset.

        If the dataset is a tabular dataset (Arrow/Pandas blocks), dicts
        are yielded for each row by the iterator. If the dataset is not tabular,
        the raw row is yielded.

        Examples:
            >>> import ray
            >>> dataset = ray.data.range(10)
            >>> next(iter(dataset.iterator().iter_rows()))
            {'id': 0}

        Time complexity: O(1)

        Returns:
            An iterable over rows of the dataset.
        """
        batch_iterable = self.iter_batches(
            batch_size=None, batch_format=None, prefetch_batches=1
        )

        def _wrapped_iterator():
            for batch in batch_iterable:
                batch = BlockAccessor.for_block(BlockAccessor.batch_to_block(batch))
                for row in batch.iter_rows(public_row_format=True):
                    yield row

        return _IterableFromIterator(_wrapped_iterator)

    @abc.abstractmethod
    @PublicAPI
    def stats(self) -> str:
        """Returns a string containing execution timing information."""
        raise NotImplementedError

    @abc.abstractmethod
    def schema(self) -> "Schema":
        """Return the schema of the dataset iterated over."""
        raise NotImplementedError

    @PublicAPI
    def iter_torch_batches(
        self,
        *,
        prefetch_batches: int = 1,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["torch.dtype", Dict[str, "torch.dtype"]]] = None,
        device: str = "auto",
        collate_fn: Optional[Callable[[Dict[str, np.ndarray]], "CollatedData"]] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterable["TorchBatchType"]:
        """Return a batched iterable of Torch Tensors over the dataset.

        This iterable yields a dictionary of column-tensors. If you are looking for
        more flexibility in the tensor conversion (e.g. casting dtypes) or the batch
        format, try using :meth:`~ray.data.DataIterator.iter_batches` directly.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range(
            ...     12,
            ... ).iterator().iter_torch_batches(batch_size=4):
            ...     print(batch)
            {'id': tensor([0, 1, 2, 3])}
            {'id': tensor([4, 5, 6, 7])}
            {'id': tensor([ 8,  9, 10, 11])}

            Use the ``collate_fn`` to customize how the tensor batch is created.

            >>> from typing import Any, Dict
            >>> import torch
            >>> import numpy as np
            >>> import ray
            >>> def collate_fn(batch: Dict[str, np.ndarray]) -> Any:
            ...     return torch.stack(
            ...         [torch.as_tensor(array) for array in batch.values()],
            ...         axis=1
            ...     )
            >>> iterator = ray.data.from_items([
            ...     {"col_1": 1, "col_2": 2},
            ...     {"col_1": 3, "col_2": 4}]).iterator()
            >>> for batch in iterator.iter_torch_batches(collate_fn=collate_fn):
            ...     print(batch)
            tensor([[1, 2],
                    [3, 4]])

        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The Torch dtype(s) for the created tensor(s); if None, the dtype
                will be inferred from the tensor data. You can't use this parameter
                with ``collate_fn``.
            device: The device on which the tensor should be placed. Defaults to
                "auto" which moves the tensors to the appropriate device when the
                Dataset is passed to Ray Train and ``collate_fn`` is not provided.
                Otherwise, defaults to CPU. You can't use this parameter with
                ``collate_fn``.
            collate_fn: A function to convert a Numpy batch to a PyTorch tensor batch.
                When this parameter is specified, the user should manually handle the
                host to device data transfer outside of ``collate_fn``.
                This is useful for further processing the data after it has been
                batched. Potential use cases include collating along a dimension other
                than the first, padding sequences of various lengths, or generally
                handling batches of different length tensors. If not provided, the
                default collate function is used which simply converts the batch of
                numpy arrays to a batch of PyTorch tensors. This API is still
                experimental and is subject to change. You can't use this parameter in
                conjunction with ``dtypes`` or ``device``.
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
            An iterable over Torch Tensor batches.
        """

        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )
        from ray.train.torch import get_device

        if collate_fn is not None and (dtypes is not None or device != "auto"):
            raise ValueError(
                "collate_fn cannot be used with dtypes and device."
                "You should manually move the output Torch tensors to the"
                "desired dtype and device outside of collate_fn."
            )

        if device == "auto":
            # Use the appropriate device for Ray Train, or falls back to CPU if
            # Ray Train is not being used.
            device = get_device()

        if collate_fn is None:
            # The default collate_fn handles formatting and Tensor creation.
            # Here, we set device=None to defer host to device data transfer
            # to the subsequent finalize_fn.
            def collate_fn(batch: Union[np.ndarray, Dict[str, np.ndarray]]):
                return convert_ndarray_batch_to_torch_tensor_batch(
                    batch,
                    dtypes=dtypes,
                    device=None,
                )

            # The default finalize_fn handles the host to device data transfer.
            # This is executed in a 1-thread pool separately from collate_fn
            # to allow independent parallelism of these steps.
            def finalize_fn(batch: Union["torch.Tensor", Dict[str, "torch.Tensor"]]):
                if device is not None:
                    if isinstance(batch, dict):
                        for k, t in batch.items():
                            batch[k] = t.to(device=device)
                    else:
                        batch = batch.to(device=device)
                return batch

        else:
            finalize_fn = None

        return self.iter_batches(
            prefetch_batches=prefetch_batches,
            batch_size=batch_size,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
            _collate_fn=collate_fn,
            _finalize_fn=finalize_fn,
        )

    def iter_tf_batches(
        self,
        *,
        prefetch_batches: int = 1,
        batch_size: Optional[int] = 256,
        dtypes: Optional[Union["tf.dtypes.DType", Dict[str, "tf.dtypes.DType"]]] = None,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterable["TensorFlowTensorBatchType"]:
        """Return a batched iterable of TensorFlow Tensors over the dataset.

        This iterable will yield single-tensor batches of the underlying dataset
        consists of a single column; otherwise, it will yield a dictionary of
        column-tensors.

        .. tip::
            If you don't need the additional flexibility provided by this method,
            consider using :meth:`~ray.data.Dataset.to_tf` instead. It's easier
            to use.

        Examples:
            >>> import ray
            >>> for batch in ray.data.range( # doctest: +SKIP
            ...     12,
            ... ).iter_tf_batches(batch_size=4):
            ...     print(batch.shape) # doctest: +SKIP
            (4, 1)
            (4, 1)
            (4, 1)

        Time complexity: O(1)

        Args:
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1.
            batch_size: The number of rows in each batch, or None to use entire blocks
                as batches (blocks may contain different number of rows).
                The final batch may include fewer than ``batch_size`` rows if
                ``drop_last`` is ``False``. Defaults to 256.
            dtypes: The TensorFlow dtype(s) for the created tensor(s); if None, the
                dtype will be inferred from the tensor data.
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
            An iterator over TensorFlow Tensor batches.
        """
        from ray.air._internal.tensorflow_utils import (
            convert_ndarray_batch_to_tf_tensor_batch,
        )

        batch_iterable = self.iter_batches(
            prefetch_batches=prefetch_batches,
            batch_size=batch_size,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )
        mapped_iterable = map(
            lambda batch: convert_ndarray_batch_to_tf_tensor_batch(
                batch, dtypes=dtypes
            ),
            batch_iterable,
        )

        return mapped_iterable

    def to_torch(
        self,
        *,
        label_column: Optional[str] = None,
        feature_columns: Optional[
            Union[List[str], List[List[str]], Dict[str, List[str]]]
        ] = None,
        label_column_dtype: Optional["torch.dtype"] = None,
        feature_column_dtypes: Optional[
            Union["torch.dtype", List["torch.dtype"], Dict[str, "torch.dtype"]]
        ] = None,
        batch_size: int = 1,
        prefetch_batches: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        unsqueeze_label_tensor: bool = True,
        unsqueeze_feature_tensors: bool = True,
    ) -> "torch.utils.data.IterableDataset":
        """Return a Torch IterableDataset over this dataset.

        This is only supported for datasets convertible to Arrow records.

        It is recommended to use the returned ``IterableDataset`` directly
        instead of passing it into a torch ``DataLoader``.

        Each element in IterableDataset will be a tuple consisting of 2
        elements. The first item contains the feature tensor(s), and the
        second item is the label tensor. Those can take on different
        forms, depending on the specified arguments.

        For the features tensor (N is the ``batch_size`` and n, m, k
        are the number of features per tensor):

        * If ``feature_columns`` is a ``List[str]``, the features will be
          a tensor of shape (N, n), with columns corresponding to
          ``feature_columns``

        * If ``feature_columns`` is a ``List[List[str]]``, the features will be
          a list of tensors of shape [(N, m),...,(N, k)], with columns of each
          tensor corresponding to the elements of ``feature_columns``

        * If ``feature_columns`` is a ``Dict[str, List[str]]``, the features
          will be a dict of key-tensor pairs of shape
          {key1: (N, m),..., keyN: (N, k)}, with columns of each
          tensor corresponding to the value of ``feature_columns`` under the
          key.

        If ``unsqueeze_label_tensor=True`` (default), the label tensor will be
        of shape (N, 1). Otherwise, it will be of shape (N,).
        If ``label_column`` is specified as ``None``, then no column from the
        ``Dataset`` will be treated as the label, and the output label tensor
        will be ``None``.

        Note that you probably want to call ``.split()`` on this dataset if
        there are to be multiple Torch workers consuming the data.

        Time complexity: O(1)

        Args:
            label_column: The name of the column used as the
                label (second element of the output list). Can be None for
                prediction, in which case the second element of returned
                tuple will also be None.
            feature_columns: The names of the columns
                to use as the features. Can be a list of lists or
                a dict of string-list pairs for multi-tensor output.
                If None, then use all columns except the label column as
                the features.
            label_column_dtype: The torch dtype to
                use for the label column. If None, then automatically infer
                the dtype.
            feature_column_dtypes: The dtypes to use for the feature
                tensors. This should match the format of ``feature_columns``,
                or be a single dtype, in which case it will be applied to
                all tensors. If None, then automatically infer the dtype.
            batch_size: How many samples per batch to yield at a time.
                Defaults to 1.
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1.
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
            unsqueeze_label_tensor: If set to True, the label tensor
                will be unsqueezed (reshaped to (N, 1)). Otherwise, it will
                be left as is, that is (N, ). In general, regression loss
                functions expect an unsqueezed tensor, while classification
                loss functions expect a squeezed one. Defaults to True.
            unsqueeze_feature_tensors: If set to True, the features tensors
                will be unsqueezed (reshaped to (N, 1)) before being concatenated into
                the final features tensor. Otherwise, they will be left as is, that is
                (N, ). Defaults to True.

        Returns:
            A torch IterableDataset.
        """
        import torch

        from ray.air._internal.torch_utils import convert_pandas_to_torch_tensor
        from ray.data._internal.torch_iterable_dataset import TorchIterableDataset

        # If an empty collection is passed in, treat it the same as None
        if not feature_columns:
            feature_columns = None

        if feature_column_dtypes and not isinstance(feature_column_dtypes, torch.dtype):
            if isinstance(feature_columns, dict):
                if not isinstance(feature_column_dtypes, dict):
                    raise TypeError(
                        "If `feature_columns` is a dict, "
                        "`feature_column_dtypes` must be None, `torch.dtype`,"
                        f" or dict, got {type(feature_column_dtypes)}."
                    )
                if set(feature_columns) != set(feature_column_dtypes):
                    raise ValueError(
                        "`feature_columns` and `feature_column_dtypes` "
                        "must have the same keys."
                    )
                if any(not subcolumns for subcolumns in feature_columns.values()):
                    raise ValueError("column list may not be empty")
            elif isinstance(feature_columns[0], (list, tuple)):
                if not isinstance(feature_column_dtypes, (list, tuple)):
                    raise TypeError(
                        "If `feature_columns` is a list of lists, "
                        "`feature_column_dtypes` must be None, `torch.dtype`,"
                        f" or a sequence, got {type(feature_column_dtypes)}."
                    )
                if len(feature_columns) != len(feature_column_dtypes):
                    raise ValueError(
                        "`feature_columns` and `feature_column_dtypes` "
                        "must have the same length."
                    )
                if any(not subcolumns for subcolumns in feature_columns):
                    raise ValueError("column list may not be empty")

        def make_generator():
            for batch in self.iter_batches(
                batch_size=batch_size,
                batch_format="pandas",
                prefetch_batches=prefetch_batches,
                drop_last=drop_last,
                local_shuffle_buffer_size=local_shuffle_buffer_size,
                local_shuffle_seed=local_shuffle_seed,
            ):
                if label_column:
                    label_tensor = convert_pandas_to_torch_tensor(
                        batch,
                        [label_column],
                        label_column_dtype,
                        unsqueeze=unsqueeze_label_tensor,
                    )
                    batch.pop(label_column)
                else:
                    label_tensor = None

                if isinstance(feature_columns, dict):
                    features_tensor = {
                        key: convert_pandas_to_torch_tensor(
                            batch,
                            feature_columns[key],
                            (
                                feature_column_dtypes[key]
                                if isinstance(feature_column_dtypes, dict)
                                else feature_column_dtypes
                            ),
                            unsqueeze=unsqueeze_feature_tensors,
                        )
                        for key in feature_columns
                    }
                else:
                    features_tensor = convert_pandas_to_torch_tensor(
                        batch,
                        columns=feature_columns,
                        column_dtypes=feature_column_dtypes,
                        unsqueeze=unsqueeze_feature_tensors,
                    )

                yield (features_tensor, label_tensor)

        return TorchIterableDataset(make_generator)

    @PublicAPI
    def to_tf(
        self,
        feature_columns: Union[str, List[str]],
        label_columns: Union[str, List[str]],
        *,
        additional_columns: Union[Optional[str], Optional[List[str]]] = None,
        prefetch_batches: int = 1,
        batch_size: int = 1,
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
        feature_type_spec: Union["tf.TypeSpec", Dict[str, "tf.TypeSpec"]] = None,
        label_type_spec: Union["tf.TypeSpec", Dict[str, "tf.TypeSpec"]] = None,
        additional_type_spec: Union[
            Optional["tf.TypeSpec"], Optional[Dict[str, "tf.TypeSpec"]]
        ] = None,
    ) -> "tf.data.Dataset":
        """Return a TF Dataset over this dataset.

        .. warning::
            If your dataset contains ragged tensors, this method errors. To prevent
            errors, :ref:`resize your tensors <transforming_tensors>`.

        Examples:
            >>> import ray
            >>> ds = ray.data.read_csv(
            ...     "s3://anonymous@air-example-data/iris.csv"
            ... )
            >>> it = ds.iterator(); it
            DataIterator(Dataset(
               num_rows=?,
               schema={
                  sepal length (cm): double,
                  sepal width (cm): double,
                  petal length (cm): double,
                  petal width (cm): double,
                  target: int64
               }
            ))

            If your model accepts a single tensor as input, specify a single feature column.

            >>> it.to_tf(feature_columns="sepal length (cm)", label_columns="target")
            <_OptionsDataset element_spec=(TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your model accepts a dictionary as input, specify a list of feature columns.

            >>> it.to_tf(["sepal length (cm)", "sepal width (cm)"], "target")
            <_OptionsDataset element_spec=({'sepal length (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), 'sepal width (cm)': TensorSpec(shape=(None,), dtype=tf.float64, name='sepal width (cm)')}, TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your dataset contains multiple features but your model accepts a single
            tensor as input, combine features with
            :class:`~ray.data.preprocessors.Concatenator`.

            >>> from ray.data.preprocessors import Concatenator
            >>> columns_to_concat = ["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"]
            >>> preprocessor = Concatenator(columns=columns_to_concat, output_column_name="features")
            >>> it = preprocessor.transform(ds).iterator()
            >>> it
            DataIterator(Concatenator
            +- Dataset(
                  num_rows=?,
                  schema={
                     sepal length (cm): double,
                     sepal width (cm): double,
                     petal length (cm): double,
                     petal width (cm): double,
                     target: int64
                  }
               ))
            >>> it.to_tf("features", "target")
            <_OptionsDataset element_spec=(TensorSpec(shape=(None, 4), dtype=tf.float64, name='features'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'))>

            If your model accepts different types, shapes, or names of tensors as input, specify the type spec.
            If type specs are not specified, they are automatically inferred from the schema of the iterator.

            >>> import tensorflow as tf
            >>> it.to_tf(
            ...     feature_columns="features",
            ...     label_columns="target",
            ...     feature_type_spec=tf.TensorSpec(shape=(None, 4), dtype=tf.float32, name="features"),
            ...     label_type_spec=tf.TensorSpec(shape=(None,), dtype=tf.float32, name="label")
            ... )
            <_OptionsDataset element_spec=(TensorSpec(shape=(None, 4), dtype=tf.float32, name='features'), TensorSpec(shape=(None,), dtype=tf.float32, name='label'))>

            If your model accepts additional metadata aside from features and label, specify a single additional column or a list of additional columns.
            A common use case is to include sample weights in the data samples and train a ``tf.keras.Model`` with ``tf.keras.Model.fit``.

            >>> import pandas as pd
            >>> ds = ds.add_column("sample weights", lambda df: pd.Series([1] * len(df)))
            >>> it = ds.iterator()
            >>> it.to_tf(feature_columns="sepal length (cm)", label_columns="target", additional_columns="sample weights")
            <_OptionsDataset element_spec=(TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'), TensorSpec(shape=(None,), dtype=tf.int64, name='sample weights'))>

            If your model accepts different types, shapes, or names for the additional metadata, specify the type spec of the additional column.

            >>> it.to_tf(
            ...     feature_columns="sepal length (cm)",
            ...     label_columns="target",
            ...     additional_columns="sample weights",
            ...     additional_type_spec=tf.TensorSpec(shape=(None,), dtype=tf.float32, name="weight")
            ... )
            <_OptionsDataset element_spec=(TensorSpec(shape=(None,), dtype=tf.float64, name='sepal length (cm)'), TensorSpec(shape=(None,), dtype=tf.int64, name='target'), TensorSpec(shape=(None,), dtype=tf.float32, name='weight'))>

        Args:
            feature_columns: Columns that correspond to model inputs. If this is a
                string, the input data is a tensor. If this is a list, the input data
                is a ``dict`` that maps column names to their tensor representation.
            label_columns: Columns that correspond to model targets. If this is a
                string, the target data is a tensor. If this is a list, the target data
                is a ``dict`` that maps column names to their tensor representation.
            additional_columns: Columns that correspond to sample weights or other metadata.
                If this is a string, the weight data is a tensor. If this is a list, the
                weight data is a ``dict`` that maps column names to their tensor representation.
            prefetch_batches: The number of batches to fetch ahead of the current batch
                to fetch. If set to greater than 0, a separate threadpool will be used
                to fetch the objects to the local node, format the batches, and apply
                the collate_fn. Defaults to 1.
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
            feature_type_spec: The `tf.TypeSpec` of `feature_columns`. If there is
                only one column, specify a `tf.TypeSpec`. If there are multiple columns,
                specify a ``dict`` that maps column names to their `tf.TypeSpec`.
                Default is `None` to automatically infer the type of each column.
            label_type_spec: The `tf.TypeSpec` of `label_columns`. If there is
                only one column, specify a `tf.TypeSpec`. If there are multiple columns,
                specify a ``dict`` that maps column names to their `tf.TypeSpec`.
                Default is `None` to automatically infer the type of each column.
            additional_type_spec: The `tf.TypeSpec` of `additional_columns`. If there
                is only one column, specify a `tf.TypeSpec`. If there are multiple
                columns, specify a ``dict`` that maps column names to their `tf.TypeSpec`.
                Default is `None` to automatically infer the type of each column.

        Returns:
            A ``tf.data.Dataset`` that yields inputs and targets.
        """  # noqa: E501

        from ray.air._internal.tensorflow_utils import (
            convert_ndarray_to_tf_tensor,
            get_type_spec,
        )

        try:
            import tensorflow as tf
        except ImportError:
            raise ValueError("tensorflow must be installed!")

        def validate_column(column: str) -> None:
            if column not in valid_columns:
                raise ValueError(
                    f"You specified '{column}' in `feature_columns`, "
                    f"`label_columns`, or `additional_columns`, but there's no "
                    f"column named '{column}' in the dataset. "
                    f"Valid column names are: {valid_columns}."
                )

        def validate_columns(columns: Union[str, List]) -> None:
            if isinstance(columns, list):
                for column in columns:
                    validate_column(column)
            else:
                validate_column(columns)

        def convert_batch_to_tensors(
            batch: Dict[str, np.ndarray],
            *,
            columns: Union[str, List[str]],
            type_spec: Union[tf.TypeSpec, Dict[str, tf.TypeSpec]],
        ) -> Union[tf.Tensor, Dict[str, tf.Tensor]]:
            if isinstance(columns, str):
                return convert_ndarray_to_tf_tensor(batch[columns], type_spec=type_spec)
            return {
                column: convert_ndarray_to_tf_tensor(
                    batch[column], type_spec=type_spec[column]
                )
                for column in columns
            }

        def generator():
            for batch in self.iter_batches(
                prefetch_batches=prefetch_batches,
                batch_size=batch_size,
                drop_last=drop_last,
                local_shuffle_buffer_size=local_shuffle_buffer_size,
                local_shuffle_seed=local_shuffle_seed,
            ):
                assert isinstance(batch, dict)
                features = convert_batch_to_tensors(
                    batch, columns=feature_columns, type_spec=feature_type_spec
                )
                labels = convert_batch_to_tensors(
                    batch, columns=label_columns, type_spec=label_type_spec
                )

                if additional_columns is None:
                    yield features, labels
                else:
                    additional_metadata = convert_batch_to_tensors(
                        batch,
                        columns=additional_columns,
                        type_spec=additional_type_spec,
                    )
                    yield features, labels, additional_metadata

        if feature_type_spec is None or label_type_spec is None:
            schema = self.schema()
            valid_columns = set(schema.names)
            validate_columns(feature_columns)
            validate_columns(label_columns)
            feature_type_spec = get_type_spec(schema, columns=feature_columns)
            label_type_spec = get_type_spec(schema, columns=label_columns)

        if additional_columns is not None and additional_type_spec is None:
            schema = self.schema()
            valid_columns = set(schema.names)
            validate_columns(additional_columns)
            additional_type_spec = get_type_spec(schema, columns=additional_columns)

        if additional_columns is not None:
            dataset = tf.data.Dataset.from_generator(
                generator,
                output_signature=(
                    feature_type_spec,
                    label_type_spec,
                    additional_type_spec,
                ),
            )
        else:
            dataset = tf.data.Dataset.from_generator(
                generator, output_signature=(feature_type_spec, label_type_spec)
            )

        options = tf.data.Options()
        options.experimental_distribute.auto_shard_policy = (
            tf.data.experimental.AutoShardPolicy.OFF
        )
        return dataset.with_options(options)

    @PublicAPI
    def materialize(self) -> "MaterializedDataset":
        """Execute and materialize this data iterator into object store memory.

        .. note::
            This method triggers the execution and materializes all blocks
            of the iterator, returning its contents as a
            :class:`~ray.data.dataset.MaterializedDataset` for further processing.
        """

        from ray.data.dataset import MaterializedDataset

        ref_bundles_iter, stats, _ = self._to_ref_bundle_iterator()

        ref_bundles = list(ref_bundles_iter)
        execution_plan = ExecutionPlan(stats)
        logical_plan = LogicalPlan(
            InputData(input_data=ref_bundles),
            execution_plan._context,
        )
        return MaterializedDataset(
            execution_plan,
            logical_plan,
        )

    def __del__(self):
        # Clear metrics on deletion in case the iterator was not fully consumed.
        StatsManager.clear_iteration_metrics(self._get_dataset_tag())


# Backwards compatibility alias.
DatasetIterator = DataIterator
