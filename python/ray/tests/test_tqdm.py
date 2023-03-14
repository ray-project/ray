import os
import sys
import time

import pytest

import ray
from ray.experimental import tqdm_ray
from ray._private.test_utils import wait_for_condition


def test_distributed_tqdm_remote():
    @ray.remote
    def foo():
        print("foo started")
        try:
            foo = tqdm_ray.tqdm(desc="foo", total=100, position=0)
            foo.update(42)
        except Exception as e:
            print(e)
        print("foo wait")
        time.sleep(999)  # Keep the bar open.

    foo.remote()

    mgr = tqdm_ray.instance()
    wait_for_condition(lambda: len(mgr.bar_groups) == 1)
    bar_group = list(mgr.bar_groups.values())[0]
    assert len(bar_group.bars_by_uuid) == 1
    bar = list(bar_group.bars_by_uuid.values())[0]
    assert bar.bar.n == 42, bar.bar.n
    assert "foo" in bar.bar.desc, bar.bar.desc


def test_distributed_tqdm_local():
    mgr = tqdm_ray.instance()
    mgr.bar_groups.clear()

    bar = tqdm_ray.tqdm(desc="bar", total=100, position=0)
    bar.update(42)
    wait_for_condition(lambda: len(mgr.bar_groups) == 1)
    assert len(mgr.bar_groups) == 1
    bar_group = list(mgr.bar_groups.values())[0]
    assert len(bar_group.bars_by_uuid) == 1
    bar = list(bar_group.bars_by_uuid.values())[0]
    assert bar.bar.n == 42, bar.bar.n
    assert "bar" in bar.bar.desc, bar.bar.desc


if __name__ == "__main__":
    # Test suite is timing out. Disable on windows for now.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
