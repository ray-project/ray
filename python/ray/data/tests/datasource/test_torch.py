import pytest
import torch

import ray
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


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
