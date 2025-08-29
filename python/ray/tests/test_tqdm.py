import sys
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.experimental import tqdm_ray


def test_distributed_tqdm_remote():
    @ray.remote
    class Actor:
        def __init__(self):
            try:
                self.bar = tqdm_ray.tqdm(
                    desc="foo", total=100, position=0, flush_interval_s=0
                )
                self.bar.update(42)
            except Exception as e:
                print(e)

        def print_something(self):
            print("hello there")

        def update(self):
            self.bar.update(1)

    a = Actor.remote()
    mgr = tqdm_ray.instance()
    wait_for_condition(lambda: len(mgr.bar_groups) == 1)
    bar_group = list(mgr.bar_groups.values())[0]
    assert len(bar_group.bars_by_uuid) == 1
    bar = list(bar_group.bars_by_uuid.values())[0]
    assert bar.bar.n == 42, bar.bar.n
    assert "foo" in bar.bar.desc, bar.bar.desc
    assert not mgr.in_hidden_state

    # Test stdout save/restore clearing.
    assert mgr.num_hides == 0
    ray.get(a.print_something.remote())
    wait_for_condition(lambda: mgr.num_hides == 1)
    wait_for_condition(lambda: not mgr.in_hidden_state)


def test_distributed_tqdm_local():
    mgr = tqdm_ray.instance()
    mgr.bar_groups.clear()

    bar = tqdm_ray.tqdm(desc="bar", total=100, position=0, flush_interval_s=0)
    bar.update(42)
    wait_for_condition(lambda: len(mgr.bar_groups) == 1)
    assert len(mgr.bar_groups) == 1
    bar_group = list(mgr.bar_groups.values())[0]
    assert len(bar_group.bars_by_uuid) == 1
    bar = list(bar_group.bars_by_uuid.values())[0]
    assert bar.bar.n == 42, bar.bar.n
    assert "bar" in bar.bar.desc, bar.bar.desc


def test_distributed_tqdm_iterator():
    mgr = tqdm_ray.instance()
    mgr.bar_groups.clear()

    assert sum(tqdm_ray.tqdm(range(100), desc="baz", flush_interval_s=0)) == sum(
        range(100)
    )
    wait_for_condition(lambda: len(mgr.bar_groups) == 1)
    assert len(mgr.bar_groups) == 1
    bar_group = list(mgr.bar_groups.values())[0]
    assert len(bar_group.bars_by_uuid) == 1
    bar = list(bar_group.bars_by_uuid.values())[0]
    assert bar.bar.n == 100, bar.bar.n
    assert "baz" in bar.bar.desc, bar.bar.desc


def test_flush_interval():
    mgr = tqdm_ray.instance()
    mgr.bar_groups.clear()

    FLUSH_INTERVAL_S = 0.1

    def check_value(expected_value):
        bar_group = list(mgr.bar_groups.values())[0]
        assert len(bar_group.bars_by_uuid) == 1
        bar = list(bar_group.bars_by_uuid.values())[0]
        assert bar.bar.n == expected_value

    bar = tqdm_ray.tqdm(
        desc="bar",
        total=100,
        position=0,
        flush_interval_s=FLUSH_INTERVAL_S,
    )
    # The first update should trigger flush.
    bar.update(1)
    check_value(1)
    # Quickly calling update multiple times
    # should not trigger flush.
    for _ in range(10):
        bar.update(1)
    check_value(1)
    # Wait for flush interval and call update again.
    # This should trigger flush.
    time.sleep(FLUSH_INTERVAL_S)
    bar.update(1)
    check_value(12)
    bar.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
