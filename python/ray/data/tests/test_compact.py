import pytest
import numpy as np

import ray


def make_dataset(sizes_mib):
    # Creates a Ray Dataset where each block is ~size MiB as a numpy array
    # One block per size entry
    blocks = [np.zeros((size * 256 * 1024,), dtype=np.int32) for size in sizes_mib]
    # Make sure each block is constructed as a Ray Dataset from an explicit block
    ds = ray.data.from_numpy_refs([ray.put(b) for b in blocks])
    return ds


def test_compact_merges_blocks():
    ds = make_dataset([5] * 8)  # 8x5MiB
    total_bytes = sum(len(b.tobytes()) for b in ds.take())
    out = ds.compact("20MiB")
    assert isinstance(out, type(ds))
    # Should ideally reduce number of blocks. Since 8*5MiB=40MiB, "20MiB" target → 2 blocks
    # However, check weaker invariant: fewer blocks, still has all records
    assert out.num_blocks() == 2
    assert out.count() == ds.count()


def test_compact_noop_if_already_at_target():
    ds = make_dataset([16] * 2)
    out = ds.compact("16MiB")
    assert out is ds


def test_compact_empty_dataset():
    ds = ray.data.from_items([])
    out = ds.compact("4MiB")
    assert out.count() == 0
    assert out is ds


@pytest.mark.parametrize("val", ["1MiB", 1024 * 1024, "2MiB"])
def test_compact_accepts_int_and_str(val):
    ds = make_dataset([1, 1])
    out = ds.compact(val)
    assert isinstance(out, type(ds))
    assert out.count() == ds.count()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
