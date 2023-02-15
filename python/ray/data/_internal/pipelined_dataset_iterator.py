from typing import TYPE_CHECKING, Dict, List, Optional, Union, Iterator

from ray.data import Dataset
from ray.data.block import DataBatch
from ray.data.dataset_iterator import DatasetIterator

if TYPE_CHECKING:
    import tensorflow as tf
    import torch
    from ray.data import DatasetPipeline
    from ray.data._internal.torch_iterable_dataset import TorchTensorBatchType
    from ray.train._internal.dataset_iterator import TrainDatasetIterator


class PipelinedDatasetIterator(DatasetIterator):
    def __init__(
        self,
        base_dataset_pipeline: "DatasetPipeline",
    ):
        self._base_dataset_pipeline = base_dataset_pipeline
        self._epoch_iterator = None

    def __repr__(self) -> str:
        return f"DatasetIterator({self._base_dataset_pipeline})"

    def _get_next_dataset(self) -> "DatasetPipeline":
        if self._epoch_iterator is None:
            self._epoch_iterator = self._base_dataset_pipeline.iter_epochs()

        # The epoch iterator will always have 1 dataset.
        ds = next(self._epoch_iterator)
        return ds

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: Optional[int] = 256,
        batch_format: str = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[DataBatch]:
        ds = self._get_next_dataset()
        return ds.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        )

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
        from ray.air._internal.torch_utils import (
            convert_ndarray_batch_to_torch_tensor_batch,
        )

        ds = self._get_next_dataset()
        for batch in ds.iter_batches(
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format="numpy",
            drop_last=drop_last,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            local_shuffle_seed=local_shuffle_seed,
        ):
            yield convert_ndarray_batch_to_torch_tensor_batch(
                batch,
                dtypes=dtypes,
                device=device,
            )

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

        feature_type_spec = get_type_spec(schema, columns=feature_columns)
        label_type_spec = get_type_spec(schema, columns=label_columns)
        output_signature = (feature_type_spec, label_type_spec)

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

        def generator(pipeline):
            for batch in pipeline.iter_batches(
                prefetch_blocks=prefetch_blocks,
                batch_size=batch_size,
                drop_last=drop_last,
                local_shuffle_buffer_size=local_shuffle_buffer_size,
                local_shuffle_seed=local_shuffle_seed,
                batch_format="numpy",
            ):
                assert isinstance(batch, dict)
                features = convert_batch_to_tensors(
                    batch, columns=feature_columns, type_spec=feature_type_spec
                )
                labels = convert_batch_to_tensors(
                    batch, columns=label_columns, type_spec=label_type_spec
                )
                yield features, labels
    
    class Generator:
        def __init__(self):
            self.current_epoch = self._get_next_dataset()
        
        def __iter__(self):
            return generator()

        #ds = self._get_next_dataset()
        #return ds.to_tf(feature_columns, label_columns)
        # return Dataset.to_tf(
        #     self,
        #     feature_columns,
        #     label_columns,
        #     prefetch_blocks=prefetch_blocks,
        #     batch_size=batch_size,
        #     drop_last=drop_last,
        #     local_shuffle_buffer_size=local_shuffle_buffer_size,
        #     local_shuffle_seed=local_shuffle_seed,
        # )

    def stats(self) -> str:
        return self._base_dataset_pipeline.stats()

    def _to_train_iterator(self) -> "TrainDatasetIterator":
        from ray.train._internal.dataset_iterator import TrainDatasetIterator

        return TrainDatasetIterator(self)
