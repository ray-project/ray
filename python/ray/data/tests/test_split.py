import math
import random
import time
from unittest.mock import patch

import numpy as np
import pytest

import ray
from ray.data._internal.block_list import BlockList
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.block import BlockAccessor
from ray.data.dataset import Dataset
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


@pytest.mark.parametrize("pipelined", [False, True])
def test_equal_split(shutdown_only, pipelined):
    ray.init(num_cpus=2)

    def range2x(n):
        if pipelined:
            return ray.data.range(n).repeat(2)
        else:
            return ray.data.range(2 * n)

    def counts(shards):
        @ray.remote(num_cpus=0)
        def count(s):
            return s.count()

        return ray.get([count.remote(s) for s in shards])

    r1 = counts(range2x(10).split(3, equal=True))
    assert all(c == 6 for c in r1), r1

    r2 = counts(range2x(10).split(3, equal=False))
    assert all(c >= 6 for c in r2), r2
    assert not all(c == 6 for c in r2), r2


@pytest.mark.parametrize(
    "block_sizes,num_splits",
    [
        ([3, 6, 3], 3),  # Test baseline.
        ([3, 3, 3], 3),  # Already balanced.
        ([3, 6, 4], 3),  # Row truncation.
        ([3, 6, 2, 3], 3),  # Row truncation, smaller number of blocks.
        ([5, 6, 2, 5], 5),  # Row truncation, larger number of blocks.
        ([1, 1, 1, 1, 6], 5),  # All smaller but one.
        ([4, 4, 4, 4, 1], 5),  # All larger but one.
        ([2], 2),  # Single block.
        ([2, 5], 1),  # Single split.
    ],
)
def test_equal_split_balanced(ray_start_regular_shared, block_sizes, num_splits):
    _test_equal_split_balanced(block_sizes, num_splits)


def _test_equal_split_balanced(block_sizes, num_splits):
    blocks = []
    metadata = []
    total_rows = 0
    for block_size in block_sizes:
        block = list(range(total_rows, total_rows + block_size))
        blocks.append(ray.put(block))
        metadata.append(BlockAccessor.for_block(block).get_metadata(None, None))
        total_rows += block_size
    block_list = BlockList(blocks, metadata)
    ds = Dataset(
        ExecutionPlan(block_list, DatasetStats.TODO()),
        0,
        False,
    )

    splits = ds.split(num_splits, equal=True)
    split_counts = [split.count() for split in splits]
    assert len(split_counts) == num_splits
    expected_block_size = total_rows // num_splits
    # Check that all splits are the expected size.
    assert all([count == expected_block_size for count in split_counts])
    expected_total_rows = sum(split_counts)
    # Check that the expected number of rows were dropped.
    assert total_rows - expected_total_rows == total_rows % num_splits
    # Check that all rows are unique (content check).
    split_rows = [row for split in splits for row in split.take(total_rows)]
    assert len(set(split_rows)) == len(split_rows)


def test_equal_split_balanced_grid(ray_start_regular_shared):

    # Tests balanced equal splitting over a grid of configurations.
    # Grid: num_blocks x num_splits x num_rows_block_1 x ... x num_rows_block_n
    seed = int(time.time())
    print(f"Seeding RNG for test_equal_split_balanced_grid with: {seed}")
    random.seed(seed)
    max_num_splits = 20
    num_splits_samples = 5
    max_num_blocks = 50
    max_num_rows_per_block = 100
    num_blocks_samples = 5
    block_sizes_samples = 5
    for num_splits in np.random.randint(2, max_num_splits + 1, size=num_splits_samples):
        for num_blocks in np.random.randint(
            1, max_num_blocks + 1, size=num_blocks_samples
        ):
            block_sizes_list = [
                np.random.randint(1, max_num_rows_per_block + 1, size=num_blocks)
                for _ in range(block_sizes_samples)
            ]
            for block_sizes in block_sizes_list:
                if sum(block_sizes) < num_splits:
                    min_ = math.ceil(num_splits / num_blocks)
                    block_sizes = np.random.randint(
                        min_, max_num_rows_per_block + 1, size=num_blocks
                    )
                _test_equal_split_balanced(block_sizes, num_splits)


@pytest.mark.parametrize("pipelined", [False, True])
def test_split_small(ray_start_regular_shared, pipelined):
    x = [Counter.remote() for _ in range(4)]
    data = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    fail = []

    @ray.remote(num_cpus=0)
    def take(s):
        return s.take()

    for m in [1, 3]:
        for n in [1, 3]:
            for locality_hints in [None, x[:n]]:
                for equal in [True, False]:
                    print("Testing", m, n, equal, locality_hints)
                    ds = ray.data.from_items(data, parallelism=m)
                    ds = maybe_pipeline(ds, pipelined)
                    splits = ds.split(n, equal=equal, locality_hints=locality_hints)
                    assert len(splits) == n
                    outs = ray.get([take.remote(s) for s in splits])
                    out = []
                    for r in outs:
                        out.extend(r)
                    if equal:
                        lens = set([len(s) for s in outs])  # noqa
                        limit = len(data) - (len(data) % n)
                        allowed = [limit]
                        # Allow for some pipelining artifacts.
                        if pipelined:
                            allowed.append(limit + 2)
                            allowed.append(limit + 1)
                            allowed.append(limit - 1)
                            allowed.append(limit - 2)
                        print(len(out), len(set(out)), allowed)
                        if (
                            len(out) not in allowed
                            or len(set(out)) != len(out)
                            # TODO(ekl) we should be able to enable this check, but
                            # there are some edge condition bugs in split.
                            # or len(lens) != 1
                        ):
                            print("FAIL", m, n, equal, locality_hints)
                            fail.append((m, n, equal, locality_hints))
                    else:
                        if sorted(out) != data:
                            print("FAIL", m, n, equal, locality_hints)
                            fail.append((m, n, equal, locality_hints))

    assert not fail, fail


def test_split_at_indices(ray_start_regular_shared):
    ds = ray.data.range(10, parallelism=3)

    with pytest.raises(ValueError):
        ds.split_at_indices([])

    with pytest.raises(ValueError):
        ds.split_at_indices([-1])

    with pytest.raises(ValueError):
        ds.split_at_indices([3, 1])

    splits = ds.split_at_indices([5])
    r = [s.take() for s in splits]
    assert r == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_at_indices([2, 5])
    r = [s.take() for s in splits]
    assert r == [[0, 1], [2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_at_indices([2, 5, 5, 100])
    r = [s.take() for s in splits]
    assert r == [[0, 1], [2, 3, 4], [], [5, 6, 7, 8, 9], []]

    splits = ds.split_at_indices([100])
    r = [s.take() for s in splits]
    assert r == [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], []]

    splits = ds.split_at_indices([0])
    r = [s.take() for s in splits]
    assert r == [[], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]


def test_split_proportionately(ray_start_regular_shared):
    ds = ray.data.range(10, parallelism=3)

    with pytest.raises(ValueError):
        ds.split_proportionately([])

    with pytest.raises(ValueError):
        ds.split_proportionately([-1])

    with pytest.raises(ValueError):
        ds.split_proportionately([0])

    with pytest.raises(ValueError):
        ds.split_proportionately([1])

    with pytest.raises(ValueError):
        ds.split_proportionately([0.5, 0.5])

    splits = ds.split_proportionately([0.5])
    r = [s.take() for s in splits]
    assert r == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_proportionately([0.2, 0.3])
    r = [s.take() for s in splits]
    assert r == [[0, 1], [2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_proportionately([0.2, 0.3, 0.3])
    r = [s.take() for s in splits]
    assert r == [[0, 1], [2, 3, 4], [5, 6, 7], [8, 9]]

    splits = ds.split_proportionately([0.98, 0.01])
    r = [s.take() for s in splits]
    assert r == [[0, 1, 2, 3, 4, 5, 6, 7], [8], [9]]

    with pytest.raises(ValueError):
        ds.split_proportionately([0.90] + ([0.001] * 90))


def test_split(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    datasets = ds.split(5)
    assert [2] * 5 == [
        dataset._plan.execute().initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(3)
    assert [4, 3, 3] == [
        dataset._plan.execute().initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(1)
    assert [10] == [
        dataset._plan.execute().initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(10)
    assert [1] * 10 == [
        dataset._plan.execute().initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() for dataset in datasets])

    datasets = ds.split(11)
    assert [1] * 10 + [0] == [
        dataset._plan.execute().initial_num_blocks() for dataset in datasets
    ]
    assert 190 == sum([dataset.sum() or 0 for dataset in datasets])


def test_split_hints(ray_start_regular_shared):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

    def assert_split_assignment(block_node_ids, actor_node_ids, expected_split_result):
        """Helper function to setup split hints test.

        Args:
            block_node_ids: a list of blocks with their locations. For
                example ["node1", "node2"] represents two blocks with
                "node1", "node2" as their location respectively.
            actor_node_ids: a list of actors with their locations. For
                example ["node1", "node2"] represents two actors with
                "node1", "node2" as their location respectively.
            expected_split_result: a list of allocation result, each entry
                in the list stores the block_index in the split dataset.
                For example, [[0, 1], [2]] represents the split result has
                two datasets, datasets[0] contains block 0 and 1; and
                datasets[1] contains block 2.
        """
        num_blocks = len(block_node_ids)
        ds = ray.data.range(num_blocks, parallelism=num_blocks)
        blocks = ds.get_internal_block_refs()
        assert len(block_node_ids) == len(blocks)
        actors = [Actor.remote() for i in range(len(actor_node_ids))]
        with patch("ray.experimental.get_object_locations") as location_mock:
            with patch("ray._private.state.actors") as state_mock:
                block_locations = {}
                for i, node_id in enumerate(block_node_ids):
                    if node_id:
                        block_locations[blocks[i]] = {"node_ids": [node_id]}
                location_mock.return_value = block_locations

                actor_state = {}
                for i, node_id in enumerate(actor_node_ids):
                    actor_state[actors[i]._actor_id.hex()] = {
                        "Address": {"NodeID": node_id}
                    }

                state_mock.return_value = actor_state

                datasets = ds.split(len(actors), locality_hints=actors)
                assert len(datasets) == len(actors)
                for i in range(len(actors)):
                    assert {blocks[j] for j in expected_split_result[i]} == set(
                        datasets[i].get_internal_block_refs()
                    )

    assert_split_assignment(
        ["node2", "node1", "node1"], ["node1", "node2"], [[1, 2], [0]]
    )
    assert_split_assignment(
        ["node1", "node1", "node1"], ["node1", "node2"], [[2, 1], [0]]
    )
    assert_split_assignment(["node2", "node2", None], ["node1", "node2"], [[0, 2], [1]])
    assert_split_assignment(["node2", "node2", None], [None, None], [[2, 1], [0]])
    assert_split_assignment(
        ["n1", "n2", "n3", "n1", "n2"], ["n1", "n2"], [[0, 2, 3], [1, 4]]
    )

    assert_split_assignment(["n1", "n2"], ["n1", "n2", "n3"], [[0], [1], []])

    # perfect split:
    #
    # split 300 blocks
    #   with node_ids interleaving between "n0", "n1", "n2"
    #
    # to 3 actors
    #   with has node_id "n1", "n2", "n0"
    #
    # expect that block 1, 4, 7... are assigned to actor with node_id n1
    #             block 2, 5, 8... are assigned to actor with node_id n2
    #             block 0, 3, 6... are assigned to actor with node_id n0
    assert_split_assignment(
        ["n0", "n1", "n2"] * 100,
        ["n1", "n2", "n0"],
        [range(1, 300, 3), range(2, 300, 3), range(0, 300, 3)],
    )

    # even split regardless of locality:
    #
    # split 301 blocks
    #   with block 0 to block 50 on "n0",
    #        block 51 to block 300 on "n1"
    #
    # to 3 actors
    #   with node_ids "n1", "n2", "n0"
    #
    # expect that block 200 to block 300 are assigned to actor with node_id n1
    #             block 100 to block 199 are assigned to actor with node_id n2
    #             block 0 to block 99 are assigned to actor with node_id n0
    assert_split_assignment(
        ["n0"] * 50 + ["n1"] * 251,
        ["n1", "n2", "n0"],
        [range(200, 301), range(100, 200), list(range(0, 50)) + list(range(50, 100))],
    )
