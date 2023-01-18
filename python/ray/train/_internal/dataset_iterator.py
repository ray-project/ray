import warnings


class TrainDatasetIterator(BulkDatasetIterator):
    """A DatasetIterator with training specific logic.

    Args:
        dataset_iterator: The base dataset iterator.
    """
    def __init__(
        self,
        dataset_iterator: BulkDatasetIterator,
    ):
        self._dataset_iterator = dataset_iterator

    def iter_torch_batches(self, *, device: Optional[str] = None, **kwargs) -> Iterator["TorchTensorBatchType"]:

        return self._dataset_iterator.iter_torch_batches(device=device, **kwargs)

    def __getattr__(self, name):
        if name == "_dataset_iterator":
            raise AttributeError

        if hasattr(self._dataset_iterator, name):
            return getattr(self._dataset_iterator, name)

        warnings.warn(
            "session.get_dataset_shard returns a ray.data.DatasetIterator "
            "instead of a Dataset as of Ray v2.3. "
            "Use iter_torch_batches(), to_tf(), or iter_batches() to "
            "iterate over one epoch. See "
            "https://docs.ray.io/en/latest/data/api/dataset_iterator.html "
            "for full DatasetIterator docs."
        )

        return getattr(self._dataset_iterator._base_dataset, name)
