import ray
from ray.data._internal.bulk_dataset_iterator import BulkDatasetIterator
from ray.data._internal.pipelined_dataset_iterator import PipelinedDatasetIterator
from ray.train._internal.dataset_iterator import TrainDatasetIterator


def test_backwards_compatibility():
    # `DatasetIterator` doesn't expose a `count` method, but you should still be able to
    # access it for backwards compatibility.
    dataset = ray.data.range(1)
    iterator = TrainDatasetIterator(BulkDatasetIterator(dataset))
    assert iterator.count() == 1

    pipeline = dataset.repeat(1)
    iterator = TrainDatasetIterator(PipelinedDatasetIterator(pipeline))
    assert iterator.count() == 1


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
