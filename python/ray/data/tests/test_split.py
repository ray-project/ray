import itertools
import math
import random
import threading
import time
from typing import Any, List, Tuple
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data._internal.equalize import _equalize
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators import InputData
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.split import (
    _drop_empty_block_split,
    _generate_global_split_results,
    _generate_per_block_split_indices,
    _generate_valid_indices,
    _split_at_indices,
    _split_single_block,
)
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.data.dataset import Dataset
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa
from ray.types import ObjectRef


@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value


def test_equal_split(shutdown_only):
    ray.init(num_cpus=2)

    def range2x(n):
        return ray.data.range(2 * n)

    def counts(shards):
        @ray.remote(num_cpus=0)
        def count(s):
            return s.count()

        return ray.get([count.remote(s) for s in shards])

    r1 = counts(range2x(10).split(3, equal=True))
    assert all(c == 6 for c in r1), r1

    # The following test is failing and may be a regression.
    # Splits appear to be based on existing block boundaries ([10, 5, 5], [8, 8, 4]).

    # r2 = counts(range2x(10).split(3, equal=False))
    # assert all(c >= 6 for c in r2), r2
    # assert not all(c == 6 for c in r2), r2


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
def test_equal_split_balanced(ray_start_regular_shared_2_cpus, block_sizes, num_splits):
    _test_equal_split_balanced(block_sizes, num_splits)


def _test_equal_split_balanced(block_sizes, num_splits):
    ctx = DataContext.get_current()

    blocks = []
    metadata = []
    ref_bundles = []
    total_rows = 0
    for block_size in block_sizes:
        block = pd.DataFrame({"id": list(range(total_rows, total_rows + block_size))})
        blocks.append(ray.put(block))
        metadata.append(BlockAccessor.for_block(block).get_metadata())
        schema = BlockAccessor.for_block(block).schema()
        blk = (blocks[-1], metadata[-1])
        ref_bundles.append(RefBundle((blk,), owns_blocks=True, schema=schema))
        total_rows += block_size

    logical_plan = LogicalPlan(InputData(input_data=ref_bundles), ctx)
    stats = DatasetStats(metadata={"TODO": []}, parent=None)
    ds = Dataset(
        ExecutionPlan(stats, ctx),
        logical_plan,
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
    assert len(set(extract_values("id", split_rows))) == len(split_rows)


def test_equal_split_balanced_grid(ray_start_regular_shared_2_cpus):
    # Tests balanced equal splitting over a grid of configurations.
    # Grid: num_blocks x num_splits x num_rows_block_1 x ... x num_rows_block_n
    seed = int(time.time())
    print(f"Seeding RNG for test_equal_split_balanced_grid with: {seed}")
    random.seed(seed)
    max_num_splits = 15
    num_splits_samples = 3
    max_num_blocks = 50
    max_num_rows_per_block = 100
    num_blocks_samples = 3
    block_sizes_samples = 3
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


def test_split_small(ray_start_regular_shared_2_cpus):
    x = [Counter.remote() for _ in range(4)]
    data = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    fail = []

    @ray.remote(num_cpus=0)
    def take(s):
        return extract_values("item", s.take())

    for m in [1, 3]:
        for n in [1, 3]:
            for locality_hints in [None, x[:n]]:
                for equal in [True, False]:
                    print("Testing", m, n, equal, locality_hints)
                    ds = ray.data.from_items(data, override_num_blocks=m)
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


def test_split_at_indices_simple(ray_start_regular_shared_2_cpus, restore_data_context):
    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    ds = ray.data.range(10, override_num_blocks=3)

    with pytest.raises(ValueError):
        ds.split_at_indices([])

    with pytest.raises(ValueError):
        ds.split_at_indices([-1])

    with pytest.raises(ValueError):
        ds.split_at_indices([3, 1])

    splits = ds.split_at_indices([5])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_at_indices([2, 5])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1], [2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_at_indices([2, 5, 5, 100])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1], [2, 3, 4], [], [5, 6, 7, 8, 9], []]

    splits = ds.split_at_indices([100])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], []]

    splits = ds.split_at_indices([0])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]


@pytest.mark.parametrize("num_blocks", list(range(1, 20)) + [25, 40])
@pytest.mark.parametrize(
    "indices",
    [
        # Two-splits.
        [5],
        [10],
        [15],
        # Three-splits.
        [5, 12],
        [1, 18],
        [9, 10],
        # Misc.
        [3, 10, 17],
        [2, 4, 11, 12, 19],
        list(range(20)),
        list(range(0, 20, 2)),
        # Empty splits.
        [10, 10],
        [5, 10, 10, 15],
        # Out-of-bounds.
        [25],
        [7, 11, 23, 33],
    ],
)
def test_split_at_indices_coverage(
    ray_start_regular_shared_2_cpus, num_blocks, indices, restore_data_context
):
    # Test that split_at_indices() creates the expected splits on a set of partition and
    # indices configurations.

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    ds = ray.data.range(20, override_num_blocks=num_blocks)
    splits = ds.split_at_indices(indices)
    r = [extract_values("id", s.sort("id").take_all()) for s in splits]
    # Use np.array_split() semantics as our correctness ground-truth.
    assert r == [arr.tolist() for arr in np.array_split(list(range(20)), indices)]


@pytest.mark.parametrize("num_blocks", [1, 3, 5, 10])
@pytest.mark.parametrize(
    "indices",
    [
        [2],  # Single split
        [1, 3],  # Two splits
        [0, 2, 4],  # Three splits
        [1, 2, 3, 4],  # Four splits
        [1, 2, 3, 4, 7],  # Five splits
        [1, 2, 3, 4, 6, 9],  # Six splits
    ]
    + [
        list(x) for x in itertools.combinations_with_replacement([1, 3, 4], 2)
    ]  # Selected two-split cases
    + [
        list(x) for x in itertools.combinations_with_replacement([0, 2, 4], 3)
    ],  # Selected three-split cases
)
def test_split_at_indices_coverage_complete(
    ray_start_regular_shared_2_cpus, num_blocks, indices, restore_data_context
):
    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Test that split_at_indices() creates the expected splits on a set of partition and
    # indices configurations.
    ds = ray.data.range(10, override_num_blocks=num_blocks)
    splits = ds.split_at_indices(indices)
    r = [extract_values("id", s.take_all()) for s in splits]
    # Use np.array_split() semantics as our correctness ground-truth.
    assert r == [arr.tolist() for arr in np.array_split(list(range(10)), indices)]


def test_split_proportionately(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(10, override_num_blocks=3)

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
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_proportionately([0.2, 0.3])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1], [2, 3, 4], [5, 6, 7, 8, 9]]

    splits = ds.split_proportionately([0.2, 0.3, 0.3])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1], [2, 3, 4], [5, 6, 7], [8, 9]]

    splits = ds.split_proportionately([0.98, 0.01])
    r = [extract_values("id", s.take()) for s in splits]
    assert r == [[0, 1, 2, 3, 4, 5, 6, 7], [8], [9]]

    with pytest.raises(ValueError):
        ds.split_proportionately([0.90] + ([0.001] * 90))


def test_split(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    datasets = ds.split(5)
    assert [2] * 5 == [len(dataset._plan.execute().blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum("id") for dataset in datasets])

    datasets = ds.split(3)
    assert [4, 3, 3] == [len(dataset._plan.execute().blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum("id") for dataset in datasets])

    datasets = ds.split(1)
    assert [10] == [len(dataset._plan.execute().blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum("id") for dataset in datasets])

    datasets = ds.split(10)
    assert [1] * 10 == [len(dataset._plan.execute().blocks) for dataset in datasets]
    assert 190 == sum([dataset.sum("id") for dataset in datasets])

    datasets = ds.split(11)
    assert [1] * 10 + [0] == [
        len(dataset._plan.execute().blocks) for dataset in datasets
    ]
    assert 190 == sum([dataset.sum("id") or 0 for dataset in datasets])


def test_split_hints(ray_start_regular_shared_2_cpus):
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
        ds = ray.data.range(num_blocks, override_num_blocks=num_blocks).materialize()
        bundles = ds.iter_internal_ref_bundles()
        blocks = _ref_bundles_iterator_to_block_refs_list(bundles)
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
                        _ref_bundles_iterator_to_block_refs_list(
                            datasets[i].iter_internal_ref_bundles()
                        )
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


def test_generate_valid_indices():
    assert [1, 2, 3] == _generate_valid_indices([10], [1, 2, 3])
    assert [1, 2, 2] == _generate_valid_indices([1, 1], [1, 2, 3])


def test_generate_per_block_split_indices():
    assert [[1], [1, 2], [], []] == _generate_per_block_split_indices(
        [3, 3, 3, 1], [1, 4, 5]
    )
    assert [[3], [], [], [1, 1]] == _generate_per_block_split_indices(
        [3, 3, 3, 1], [3, 10, 10]
    )
    assert [[], [], [], []] == _generate_per_block_split_indices([3, 3, 3, 1], [])


def _create_meta(num_rows):
    return BlockMetadata(
        num_rows=num_rows,
        size_bytes=None,
        input_files=None,
        exec_stats=None,
    )


def _create_block_and_metadata(data: Any) -> Tuple[ObjectRef[Block], BlockMetadata]:
    block = pd.DataFrame({"id": data})
    metadata = BlockAccessor.for_block(block).get_metadata()
    return (ray.put(block), metadata)


def _create_bundle(blocks: List[List[Any]]) -> RefBundle:
    schema = BlockAccessor.for_block(pd.DataFrame({"id": []})).schema()
    return RefBundle(
        [_create_block_and_metadata(block) for block in blocks],
        owns_blocks=True,
        schema=schema,
    )


def _create_blocks_with_metadata(blocks):
    bundle = _create_bundle(blocks)
    return list(bundle.blocks)


def test_split_single_block(ray_start_regular_shared_2_cpus):
    block = pd.DataFrame({"id": [1, 2, 3]})
    metadata = _create_meta(3)

    results = ray.get(
        ray.remote(_split_single_block)
        .options(num_returns=2)
        .remote(234, block, metadata, [])
    )
    block_id, meta = results[0]
    blocks = results[1:]
    assert 234 == block_id
    assert len(blocks) == 1
    assert list(blocks[0]["id"]) == [1, 2, 3]
    assert meta[0].num_rows == 3

    results = ray.get(
        ray.remote(_split_single_block)
        .options(num_returns=3)
        .remote(234, block, metadata, [1])
    )
    block_id, meta = results[0]
    blocks = results[1:]
    assert 234 == block_id
    assert len(blocks) == 2
    assert list(blocks[0]["id"]) == [1]
    assert meta[0].num_rows == 1
    assert list(blocks[1]["id"]) == [2, 3]
    assert meta[1].num_rows == 2

    results = ray.get(
        ray.remote(_split_single_block)
        .options(num_returns=6)
        .remote(234, block, metadata, [0, 1, 1, 3])
    )
    block_id, meta = results[0]
    blocks = results[1:]
    assert 234 == block_id
    assert len(blocks) == 5
    assert list(blocks[0]["id"]) == []
    assert list(blocks[1]["id"]) == [1]
    assert list(blocks[2]["id"]) == []
    assert list(blocks[3]["id"]) == [2, 3]
    assert list(blocks[4]["id"]) == []

    block = pd.DataFrame({"id": []})
    metadata = _create_meta(0)

    results = ray.get(
        ray.remote(_split_single_block)
        .options(num_returns=3)
        .remote(234, block, metadata, [0])
    )
    block_id, meta = results[0]
    blocks = results[1:]
    assert 234 == block_id
    assert len(blocks) == 2
    assert list(blocks[0]["id"]) == []
    assert list(blocks[1]["id"]) == []


def test_drop_empty_block_split():
    assert [1, 2] == _drop_empty_block_split([0, 1, 2, 3], 3)
    assert [1, 2] == _drop_empty_block_split([1, 1, 2, 2], 3)
    assert [] == _drop_empty_block_split([0], 0)


def verify_splits(splits, blocks_by_split):
    assert len(splits) == len(blocks_by_split)
    for blocks, (block_refs, metas) in zip(blocks_by_split, splits):
        assert len(blocks) == len(block_refs)
        assert len(blocks) == len(metas)
        for block, block_ref, meta in zip(blocks, block_refs, metas):
            assert list(ray.get(block_ref)["id"]) == block
            assert meta.num_rows == len(block)


def test_generate_global_split_results(ray_start_regular_shared_2_cpus):
    inputs = [
        _create_block_and_metadata([1]),
        _create_block_and_metadata([2, 3]),
        _create_block_and_metadata([4]),
    ]

    splits = list(zip(*_generate_global_split_results(iter(inputs), [1, 2, 1])))
    verify_splits(splits, [[[1]], [[2, 3]], [[4]]])

    splits = list(zip(*_generate_global_split_results(iter(inputs), [3, 1])))
    verify_splits(splits, [[[1], [2, 3]], [[4]]])

    splits = list(zip(*_generate_global_split_results(iter(inputs), [3, 0, 1])))
    verify_splits(splits, [[[1], [2, 3]], [], [[4]]])

    inputs = []
    splits = list(zip(*_generate_global_split_results(iter(inputs), [0, 0])))
    verify_splits(splits, [[], []])


def test_private_split_at_indices(ray_start_regular_shared_2_cpus):
    inputs = _create_blocks_with_metadata([])
    splits = list(zip(*_split_at_indices(inputs, [0])))
    verify_splits(splits, [[], []])

    splits = list(zip(*_split_at_indices(inputs, [])))
    verify_splits(splits, [[]])

    inputs = _create_blocks_with_metadata([[1], [2, 3], [4]])

    splits = list(zip(*_split_at_indices(inputs, [1])))
    verify_splits(splits, [[[1]], [[2, 3], [4]]])

    inputs = _create_blocks_with_metadata([[1], [2, 3], [4]])
    splits = list(zip(*_split_at_indices(inputs, [2])))
    verify_splits(splits, [[[1], [2]], [[3], [4]]])

    inputs = _create_blocks_with_metadata([[1], [2, 3], [4]])
    splits = list(zip(*_split_at_indices(inputs, [1])))
    verify_splits(splits, [[[1]], [[2, 3], [4]]])

    inputs = _create_blocks_with_metadata([[1], [2, 3], [4]])
    splits = list(zip(*_split_at_indices(inputs, [2, 2])))
    verify_splits(splits, [[[1], [2]], [], [[3], [4]]])

    inputs = _create_blocks_with_metadata([[1], [2, 3], [4]])
    splits = list(zip(*_split_at_indices(inputs, [])))
    verify_splits(splits, [[[1], [2, 3], [4]]])

    inputs = _create_blocks_with_metadata([[1], [2, 3], [4]])
    splits = list(zip(*_split_at_indices(inputs, [0, 4])))
    verify_splits(splits, [[], [[1], [2, 3], [4]], []])


def equalize_helper(input_block_lists: List[List[List[Any]]]):
    result = _equalize(
        [_create_bundle(block_list) for block_list in input_block_lists],
        owned_by_consumer=True,
    )
    result_block_lists = []
    for bundle in result:
        block_list = []
        for block_ref in bundle.block_refs:
            block = ray.get(block_ref)
            block_accessor = BlockAccessor.for_block(block)
            block_list.append(list(block_accessor.to_default()["id"]))
        result_block_lists.append(block_list)
    return result_block_lists


def verify_equalize_result(input_block_lists, expected_block_lists):
    result_block_lists = equalize_helper(input_block_lists)
    assert result_block_lists == expected_block_lists


def test_equalize(ray_start_regular_shared_2_cpus):
    verify_equalize_result([], [])
    verify_equalize_result([[]], [[]])
    verify_equalize_result([[[1]], []], [[], []])
    verify_equalize_result([[[1], [2, 3]], [[4]]], [[[1], [2]], [[4], [3]]])
    verify_equalize_result([[[1], [2, 3]], []], [[[1]], [[2]]])
    verify_equalize_result(
        [[[1], [2, 3], [4, 5]], [[6]], []], [[[1], [2]], [[6], [3]], [[4, 5]]]
    )
    verify_equalize_result(
        [[[1, 2, 3], [4, 5]], [[6]], []], [[[4, 5]], [[6], [1]], [[2, 3]]]
    )


def test_equalize_randomized(ray_start_regular_shared_2_cpus):
    # verify the entries in the splits are in the range of 0 .. num_rows,
    # unique, and the total number matches num_rows if exact_num == True.
    def assert_unique_and_inrange(splits, num_rows, exact_num=False):
        unique_set = set()
        for split in splits:
            for block in split:
                for entry in block:
                    assert entry not in unique_set
                    assert entry >= 0 and entry < num_rows
                    unique_set.add(entry)
        if exact_num:
            assert len(unique_set) == num_rows

    # verify that splits are equalized.
    def assert_equal_split(splits, num_rows, num_split):
        split_size = num_rows // num_split
        for split in splits:
            assert len((list(itertools.chain.from_iterable(split)))) == split_size

    # create randomized splits contains entries from 0 ... num_rows.
    def random_split(num_rows, num_split):
        split_point = [int(random.random() * num_rows) for _ in range(num_split - 1)]
        split_index_helper = [0] + sorted(split_point) + [num_rows]
        splits = []
        for i in range(1, len(split_index_helper)):
            split_start = split_index_helper[i - 1]
            split_end = split_index_helper[i]
            num_entries = split_end - split_start
            split = []
            num_block_split = int(random.random() * num_entries)
            block_split_point = [
                split_start + int(random.random() * num_entries)
                for _ in range(num_block_split)
            ]
            block_index_helper = [split_start] + sorted(block_split_point) + [split_end]
            for j in range(1, len(block_index_helper)):
                split.append(
                    list(range(block_index_helper[j - 1], block_index_helper[j]))
                )
            splits.append(split)
        assert_unique_and_inrange(splits, num_rows, exact_num=True)
        return splits

    for i in range(100):
        num_rows = int(random.random() * 100)
        num_split = int(random.random() * 10) + 1
        input_splits = random_split(num_rows, num_split)
        print(input_splits)
        equalized_splits = equalize_helper(input_splits)
        assert_unique_and_inrange(equalized_splits, num_rows)
        assert_equal_split(equalized_splits, num_rows, num_split)


def test_train_test_split(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(8)

    # float
    train, test = ds.train_test_split(test_size=0.25)
    assert extract_values("id", train.take()) == [0, 1, 2, 3, 4, 5]
    assert extract_values("id", test.take()) == [6, 7]

    # int
    train, test = ds.train_test_split(test_size=2)
    assert extract_values("id", train.take()) == [0, 1, 2, 3, 4, 5]
    assert extract_values("id", test.take()) == [6, 7]

    # shuffle
    train, test = ds.train_test_split(test_size=0.25, shuffle=True, seed=1)
    assert extract_values("id", train.take()) == [7, 4, 6, 0, 5, 2]
    assert extract_values("id", test.take()) == [1, 3]

    # error handling
    with pytest.raises(TypeError):
        ds.train_test_split(test_size=[1])

    with pytest.raises(ValueError):
        ds.train_test_split(test_size=-1)

    with pytest.raises(ValueError):
        ds.train_test_split(test_size=0)

    with pytest.raises(ValueError):
        ds.train_test_split(test_size=1.1)

    with pytest.raises(ValueError):
        ds.train_test_split(test_size=9)


def test_train_test_split_stratified(ray_start_regular_shared_2_cpus):
    # Test basic stratification with simple dataset
    data = [
        {"id": 0, "label": "A"},
        {"id": 1, "label": "A"},
        {"id": 2, "label": "B"},
        {"id": 3, "label": "B"},
        {"id": 4, "label": "C"},
        {"id": 5, "label": "C"},
    ]
    ds = ray.data.from_items(data)

    # Test stratified split
    train, test = ds.train_test_split(test_size=0.5, stratify="label")

    # Check that we have the right number of samples
    assert train.count() == 3
    assert test.count() == 3

    # Check that class proportions are preserved
    train_labels = [row["label"] for row in train.take()]
    test_labels = [row["label"] for row in test.take()]

    train_label_counts = {label: train_labels.count(label) for label in ["A", "B", "C"]}
    test_label_counts = {label: test_labels.count(label) for label in ["A", "B", "C"]}

    # Each class should have exactly 1 sample in each split
    assert train_label_counts == {"A": 1, "B": 1, "C": 1}
    assert test_label_counts == {"A": 1, "B": 1, "C": 1}


def test_train_test_split_shuffle_stratify_error(ray_start_regular_shared_2_cpus):
    # Test that shuffle=True and stratify cannot be used together
    data = [
        {"id": 0, "label": "A"},
        {"id": 1, "label": "A"},
        {"id": 2, "label": "B"},
        {"id": 3, "label": "B"},
    ]
    ds = ray.data.from_items(data)

    # Test that combining shuffle=True and stratify raises ValueError
    with pytest.raises(
        ValueError, match="Cannot specify both 'shuffle=True' and 'stratify'"
    ):
        ds.train_test_split(test_size=0.5, shuffle=True, stratify="label")


def test_train_test_split_stratified_imbalanced(ray_start_regular_shared_2_cpus):
    # Test stratified split with imbalanced class distribution
    data = [
        {"id": 0, "label": "A"},
        {"id": 1, "label": "A"},
        {"id": 2, "label": "A"},
        {"id": 3, "label": "A"},
        {"id": 4, "label": "A"},
        {"id": 5, "label": "A"},  # 6 samples of class A
        {"id": 6, "label": "B"},
        {"id": 7, "label": "B"},  # 2 samples of class B
        {"id": 8, "label": "C"},  # 1 sample of class C
    ]
    ds = ray.data.from_items(data)

    # Test with 0.3 test size
    train, test = ds.train_test_split(test_size=0.3, stratify="label")

    train_labels = [row["label"] for row in train.take()]
    test_labels = [row["label"] for row in test.take()]

    train_label_counts = {label: train_labels.count(label) for label in ["A", "B", "C"]}
    test_label_counts = {label: test_labels.count(label) for label in ["A", "B", "C"]}

    # Check proportions are maintained as closely as possible
    # Class A: 6 samples -> test_count = int(6 * 0.3) = 1 -> train: 5, test: 1
    # Class B: 2 samples -> test_count = int(2 * 0.3) = 0 -> train: 2, test: 0
    # Class C: 1 sample -> test_count = int(1 * 0.3) = 0 -> train: 1, test: 0
    assert train_label_counts["A"] == 5
    assert test_label_counts["A"] == 1
    assert train_label_counts["B"] == 2
    assert test_label_counts["B"] == 0
    assert train_label_counts["C"] == 1
    assert test_label_counts["C"] == 0


def test_split_is_not_disruptive(ray_start_cluster):
    ray.shutdown()
    ds = ray.data.range(100, override_num_blocks=10).map_batches(lambda x: x)

    def verify_integrity(splits):
        for dss in splits:
            for batch in dss.iter_batches():
                pass
        for batch in ds.iter_batches():
            pass

    # No block splitting invovled: split 10 even blocks into 2 groups.
    verify_integrity(ds.split(2, equal=True))
    # Block splitting invovled: split 10 even blocks into 3 groups.
    verify_integrity(ds.split(3, equal=True))

    # Same as above but having tranforms post converting to lazy.
    verify_integrity(ds.map_batches(lambda x: x).split(2, equal=True))
    verify_integrity(ds.map_batches(lambda x: x).split(3, equal=True))

    # Same as above but having in-place tranforms post converting to lazy.
    verify_integrity(ds.randomize_block_order().split(2, equal=True))
    verify_integrity(ds.randomize_block_order().split(3, equal=True))


def test_streaming_train_test_split_hash(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(10000000, override_num_blocks=10)

    ds_train, ds_test = ds.streaming_train_test_split(
        test_size=0.2, split_type="hash", hash_column="id"
    )

    np.testing.assert_almost_equal(float(ds_train.count()) / 10000000.0, 0.8, decimal=3)
    np.testing.assert_almost_equal(float(ds_test.count()) / 10000000.0, 0.2, decimal=3)

    # Check if train and test are disjoint
    assert (
        ds_train.join(ds_test, join_type="inner", on=("id",), num_partitions=1).count()
        == 0
    )


@pytest.mark.parametrize("seed", [None, 42])
def test_streaming_train_test_split_random(ray_start_regular_shared_2_cpus, seed):
    ds = ray.data.range(10000000, override_num_blocks=10)

    ds_train, ds_test = ds.streaming_train_test_split(
        test_size=0.2, split_type="random", seed=seed
    )

    np.testing.assert_almost_equal(float(ds_train.count()) / 10000000.0, 0.8, decimal=3)
    np.testing.assert_almost_equal(float(ds_test.count()) / 10000000.0, 0.2, decimal=3)

    # Check if train and test are disjoint
    assert (
        ds_train.join(ds_test, join_type="inner", on=("id",), num_partitions=1).count()
        == 0
    )


@pytest.mark.parametrize(
    "test_size,split_type,hash_column,seed,error_msg",
    [
        (0.2, "hash", None, None, "hash_column is required for hash split"),
        (0.2, "hash", "id", 42, "seed is not supported for hash split"),
        (0, "hash", "id", None, "test_size must be between 0 and 1"),
        (1, "hash", "id", None, "test_size must be between 0 and 1"),
        (0.2, "random", "id", None, "hash_column is not supported for random split"),
        (0, "random", None, None, "test_size must be between 0 and 1"),
        (1, "random", None, None, "test_size must be between 0 and 1"),
        (0.2, "unknown", "id", None, "Invalid split type: unknown"),
    ],
)
def test_streaming_train_test_split_wrong_params(
    ray_start_regular_shared_2_cpus, test_size, split_type, hash_column, seed, error_msg
):
    ds = ray.data.range(10)

    with pytest.raises(ValueError, match=error_msg):
        ds.streaming_train_test_split(
            test_size=test_size,
            split_type=split_type,
            hash_column=hash_column,
            seed=seed,
        )


@pytest.mark.parametrize("prefetch_batches", [0, 2])
def test_streaming_split_reports_and_clears_prefetched_bytes(
    ray_start_regular_shared_2_cpus, prefetch_batches
):
    """Test streaming_split reports and clears prefetched bytes.

    Verifies that:
    1. Prefetched bytes are tracked during iteration
    2. When StopIteration is raised in SplitCoordinator.get(), the
       prefetched bytes are cleared to avoid stale backpressure data
    """
    ds = ray.data.range(20, override_num_blocks=4)
    splits = ds.streaming_split(2, equal=True)

    # Get the coordinator actor to check prefetched bytes
    coord = splits[0]._coord_actor

    results = []

    def consume(split, idx):
        count = 0
        for batch in split.iter_batches(
            batch_size=5, prefetch_batches=prefetch_batches
        ):
            count += len(batch["id"])
        results.append((idx, count))

    threads = [
        threading.Thread(target=consume, args=(splits[0], 0)),
        threading.Thread(target=consume, args=(splits[1], 1)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Verify both splits consumed all data (epoch completed via StopIteration)
    total_rows = sum(r[1] for r in results)
    assert total_rows == 20, f"Expected 20 total rows, got {total_rows}"

    # Verify client prefetched bytes are cleared after epoch end
    # (cleared when StopIteration is raised in SplitCoordinator.get())
    client_bytes = ray.get(coord.get_client_prefetched_bytes.remote())
    for split_idx, bytes_val in client_bytes.items():
        assert (
            bytes_val == 0
        ), f"Split {split_idx} has stale prefetched bytes: {bytes_val}"

    # Run a second epoch to verify cleared bytes don't cause issues
    results.clear()

    threads = [
        threading.Thread(target=consume, args=(splits[0], 0)),
        threading.Thread(target=consume, args=(splits[1], 1)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Second epoch should also consume all data
    total_rows = sum(r[1] for r in results)
    assert total_rows == 20, f"Second epoch: expected 20 rows, got {total_rows}"

    # Verify prefetched bytes cleared again after second epoch
    client_bytes = ray.get(coord.get_client_prefetched_bytes.remote())
    for split_idx, bytes_val in client_bytes.items():
        assert (
            bytes_val == 0
        ), f"Split {split_idx} stale bytes after 2nd epoch: {bytes_val}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
