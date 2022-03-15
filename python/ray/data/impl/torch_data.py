from typing import List, Dict, Iterator, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.data.dataset_pipeline import DatasetPipeline

from ray.data.impl.block_batching import BatchType
from ray.ml.utils.torch_utils import convert_pandas_to_torch_tensor


import torch
from torchdata.datapipes import functional_datapipe
from torchdata.datapipes.iter import IterDataPipe


class TorchBatchFormatter:
    """Formats Pandas DataFrame batches as Torch tensor batches.

    Args:
        label_column: The name of the column used as the label (second element of the
            output list). Can be None for prediction, in which case the second element
            of returned tuple will also be None.
        feature_columns: The names of the columns to use as the features. Can be a list
            of lists or a dict of string-list pairs for multi-tensor output. If None,
            then use all columns except the label columns as the features.
        label_column_dtype: The torch dtype to use for the label column. If None, then
            automatically infer the dtype.
        feature_column_dtypes: The dtypes to use for the feature tensors. This should
            match the format of ``feature_columns``, or be a single dtype, in which case
            it will be applied to all tensors. If None, then automatically infer the
            dtype.
        unsqueeze_label_tensor: If set to True, the label tensor will be unsqueezed
            (reshaped to (N, 1)). Otherwise, it will be left as is, that is (N, ). In
            general, regression loss functions expect an unsqueezed tensor, while
            classification loss functions expect a squeezed one.
    """

    def __init__(
        self,
        label_column: Optional[str],
        feature_columns: Optional[
            Union[List[str], List[List[str]], Dict[str, List[str]]]
        ],
        label_column_dtype: Optional["torch.dtype"],
        feature_column_dtypes: Optional[
            Union["torch.dtype", List["torch.dtype"], Dict[str, "torch.dtype"]]
        ],
        unsqueeze_label_tensor: bool,
    ):
        _validate_feature_column_spec(feature_columns, feature_column_dtypes)
        self.label_column = label_column
        # If an empty collection is passed in, treat it the same as None
        if not feature_columns:
            feature_columns = None
        self.feature_columns = feature_columns
        self.label_column_dtype = label_column_dtype
        self.feature_column_dtypes = feature_column_dtypes
        self.unsqueeze_label_tensor = unsqueeze_label_tensor

    def format(self, batch: BatchType) -> BatchType:
        if self.label_column:
            label_vals = batch.pop(self.label_column).values
            label_tensor = torch.as_tensor(label_vals, dtype=self.label_column_dtype)
            if self.unsqueeze_label_tensor:
                label_tensor = label_tensor.view(-1, 1)
        else:
            label_tensor = None

        if isinstance(self.feature_columns, dict):
            features_tensor = {
                key: convert_pandas_to_torch_tensor(
                    batch,
                    self.feature_columns[key],
                    self.feature_column_dtypes[key]
                    if isinstance(self.feature_column_dtypes, dict)
                    else self.feature_column_dtypes,
                )
                for key in self.feature_columns
            }
        else:
            features_tensor = convert_pandas_to_torch_tensor(
                batch,
                columns=self.feature_columns,
                column_dtypes=self.feature_column_dtypes,
            )
        return features_tensor, label_tensor


def _validate_feature_column_spec(
    feature_columns: Optional[Union[List[str], List[List[str]], Dict[str, List[str]]]],
    feature_column_dtypes: Optional[
        Union["torch.dtype", List["torch.dtype"], Dict[str, "torch.dtype"]]
    ],
):
    """Validate feature column spcification.

    This raises an error if the feature column specification is determined to be
    invalid.

    Args:
        feature_columns (Union[None, List[str], List[List[str]], \
Dict[str, List[str]]]): The names of the columns
            to use as the features. Can be a list of lists or
            a dict of string-list pairs for multi-tensor output.
            If None, then use all columns except the label columns as
            the features.
        label_column_dtype (Optional[torch.dtype]): The torch dtype to
            use for the label column. If None, then automatically infer
            the dtype.
        feature_column_dtypes (Union[None, torch.dtype, List[torch.dtype],\
Dict[str, torch.dtype]]): The dtypes to use for the feature
            tensors. This should match the format of ``feature_columns``,
            or be a single dtype, in which case it will be applied to
            all tensors. If None, then automatically infer the dtype.
    """
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


@functional_datapipe("iter_batches")
class DatasetBatcherIterDataPipe(IterDataPipe):
    """Yields Pandas DataFrame batches from the dataset datapipe.

    Args:
        source_datapipe: Datapipe yielding datasets.
        batch_size: How many samples per batch to yield at a time.
        prefetch_blocks: The number of blocks to prefetch ahead of the current block
            during the scan.
        drop_last: Set to True to drop the last incomplete batch, if the dataset size is
            not divisible by the batch size. If False and the size of dataset is not
            divisible by the batch size, then the last batch will be smaller.
    """

    def __init__(
        self,
        source_datapipe: "IterDataPipe[Dataset]",
        batch_size: int,
        prefetch_blocks: int,
        drop_last: bool,
    ):
        self.source_datapipe = source_datapipe
        self.batch_size = batch_size
        self.prefetch_blocks = prefetch_blocks
        self.drop_last = drop_last

    def __iter__(self) -> Iterator[BatchType]:
        for ds in self.source_datapipe:
            yield from ds.iter_batches(
                batch_size=self.batch_size,
                batch_format="pandas",
                prefetch_blocks=self.prefetch_blocks,
                drop_last=self.drop_last,
            )


@functional_datapipe("iter_datasets")
class EpochDatasetListerIterDataPipe(IterDataPipe):
    """Yields datasets from the dataset pipeline datapipe.

    Args:
        source_datapipe: Datapipe yielding dataset pipelines.
    """

    def __init__(self, source_datapipe: "IterDataPipe[DatasetPipeline]"):
        self.source_datapipe = source_datapipe

    def __iter__(self) -> Iterator[BatchType]:
        for pipe in self.source_datapipe:
            yield from pipe.iter_datasets()


@functional_datapipe("format_batches")
class BatchFormatterIterDataPipe(IterDataPipe):
    """Yields formatted Torch tensor batches from a Pandas DataFrame batch datapipe.

    Args:
        datapipe: Datapipe yielding unformatted Pandas DataFrame batches.
        formatter: Batch formatter that transforms Pandas DataFrame batches into Torch
            tensor batches.
    """

    def __init__(
        self,
        datapipe: IterDataPipe[BatchType],
        formatter: TorchBatchFormatter,
    ):
        self.datapipe = datapipe
        self.formatter = formatter

    def __iter__(self) -> Iterator[BatchType]:
        for batch in self.datapipe:
            yield self.formatter.format(batch)
