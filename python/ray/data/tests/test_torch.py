import numpy as np
import pandas as pd
import pytest
import torch

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


def test_iter_torch_batches(ray_start_10_cpus_shared):
    import torch

    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_torch_batches(batch_size=3):
            iterations.append(
                torch.stack(
                    (batch["one"], batch["two"], batch["label"]),
                    dim=1,
                ).numpy()
            )
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


def test_iter_torch_batches_tensor_ds(ray_start_10_cpus_shared):
    arr1 = np.arange(12).reshape((3, 2, 2))
    arr2 = np.arange(12, 24).reshape((3, 2, 2))
    arr = np.concatenate((arr1, arr2))
    ds = ray.data.from_numpy([arr1, arr2])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_torch_batches(batch_size=2):
            iterations.append(batch["data"].numpy())
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(arr, combined_iterations)


# This test catches an error in stream_split_iterator dealing with empty blocks,
# which is difficult to reproduce outside of TorchTrainer.
def test_torch_trainer_crash(ray_start_10_cpus_shared):
    from ray import train
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    ray.data.DataContext.get_current().execution_options.verbose_progress = True

    train_ds = ray.data.range_tensor(100)
    train_ds = train_ds.materialize()

    def train_loop_per_worker():
        it = train.get_dataset_shard("train")
        for i in range(2):
            count = 0
            for batch in it.iter_batches():
                count += len(batch["data"])
            assert count == 50

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": train_ds},
    )
    my_trainer.fit()


@pytest.mark.parametrize("local_read", [True, False])
def test_from_torch_map_style_dataset(ray_start_10_cpus_shared, local_read):
    class StubDataset(torch.utils.data.Dataset):
        def __len__(self):
            return 1

        def __getitem__(self, index):
            return index

    torch_dataset = StubDataset()

    ray_dataset = ray.data.from_torch(torch_dataset, local_read=local_read)

    actual_data = ray_dataset.take_all()
    assert actual_data == [{"item": 0}]


def test_from_torch_iterable_style_dataset(ray_start_10_cpus_shared):
    class StubIterableDataset(torch.utils.data.IterableDataset):
        def __len__(self):
            return 1

        def __iter__(self):
            return iter([0])

    iter_torch_dataset = StubIterableDataset()

    ray_dataset = ray.data.from_torch(iter_torch_dataset)

    actual_data = ray_dataset.take_all()
    assert actual_data == [{"item": 0}]


@pytest.mark.parametrize("local_read", [True, False])
def test_from_torch_boundary_conditions(ray_start_10_cpus_shared, local_read):
    """
    Tests that from_torch respects __len__ for map-style datasets
    """
    from torch.utils.data import Dataset

    class BoundaryTestMapDataset(Dataset):
        """A map-style dataset where __len__ is less than the underlying data size."""

        def __init__(self, data, length):
            super().__init__()
            self._data = data
            self._length = length
            assert self._length <= len(
                self._data
            ), "Length must be <= data size to properly test boundary conditions"

        def __len__(self):
            return self._length

        def __getitem__(self, index):
            if not (0 <= index < self._length):
                # Note: don't use IndexError because we want to fail clearly if
                # Ray Data tries to access beyond __len__ - 1
                raise RuntimeError(
                    f"Index {index} out of bounds for dataset with length {self._length}"
                )
            return self._data[index]

    source_data = list(range(10))
    dataset_len = 8  # Intentionally less than len(source_data)

    # --- Test MapDataset ---
    map_ds = BoundaryTestMapDataset(source_data, dataset_len)
    # Expected data only includes elements up to dataset_len - 1
    expected_items = source_data[:dataset_len]

    ray_ds_map = ray.data.from_torch(map_ds, local_read=local_read)
    actual_items_map = extract_values("item", list(ray_ds_map.take_all()))

    # This assertion verifies that ray_ds_map didn't try to access index 8 or 9,
    # which would have raised an IndexError in BoundaryTestMapDataset.__getitem__
    assert actual_items_map == expected_items
    assert len(actual_items_map) == dataset_len


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
