"""Tests for get_bundle_indices_sorted_by_node."""

import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.test_utils import placement_group_assert_no_leak
from ray.llm._internal.serve.utils.pg_utils import get_bundle_indices_sorted_by_node

# Realistic 56-char hex node IDs
NODE_A = "ab7c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c"  # driver node
NODE_B = "1234567890abcdef1234567890abcdef1234567890abcdef12345678"
NODE_C = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba09"


def _assert_grouped_by_node(result, bundles_to_node_id):
    seen_nodes = set()
    prev_node = None
    for idx in result:
        node = bundles_to_node_id[idx]
        if node != prev_node:
            assert (
                node not in seen_nodes
            ), f"Node {node} appeared non-contiguously in {result}"
            seen_nodes.add(node)
            prev_node = node


@pytest.mark.parametrize(
    "bundles_to_node_id,expected_sorted_bundle_indices",
    [
        pytest.param(
            {0: NODE_C, 1: NODE_A, 2: NODE_B, 3: NODE_C, 4: NODE_A, 5: NODE_B},
            [1, 4, 2, 5, 0, 3],
        ),
        pytest.param(
            {0: NODE_B, 1: NODE_B, 2: NODE_A, 3: NODE_A},
            [2, 3, 0, 1],
        ),
        pytest.param(
            # Driver has no bundles
            {0: NODE_C, 1: NODE_B, 2: NODE_C, 3: NODE_B},
            [1, 3, 0, 2],
        ),
        pytest.param(
            {0: NODE_A, 1: NODE_A, 2: NODE_A},
            [0, 1, 2],
        ),
        pytest.param(
            {0: NODE_A},
            [0],
        ),
        pytest.param(
            {},
            [],
        ),
    ],
)
def test_sort_bundle_indices_by_node(
    bundles_to_node_id, expected_sorted_bundle_indices
):
    mock_pg = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.get_node_id.return_value = NODE_A

    with (
        patch(
            "ray.llm._internal.serve.utils.pg_utils.placement_group_table",
            return_value={"bundles_to_node_id": bundles_to_node_id},
        ),
        patch("ray.llm._internal.serve.utils.pg_utils.ray") as mock_ray,
    ):
        mock_ray.get_runtime_context.return_value = mock_ctx
        result = get_bundle_indices_sorted_by_node(mock_pg)

    assert result == expected_sorted_bundle_indices


# Integration test: verifies the full path through a real cluster, though
# bundle ordering across nodes is non-deterministic and may already be sorted.
@pytest.mark.parametrize("strategy", ["SPREAD", "PACK"])
def test_sort_bundles(ray_start_cluster, strategy):
    cluster = ray_start_cluster
    for _ in range(2):
        cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.wait_for_nodes()

    driver_node_id = ray.get_runtime_context().get_node_id()

    pg = ray.util.placement_group(
        name=f"test_sort_bundles_{strategy}",
        strategy=strategy,
        bundles=[{"CPU": 1}] * 4,
    )
    ray.get(pg.ready())

    result = get_bundle_indices_sorted_by_node(pg)

    table = ray.util.placement_group_table(pg)
    bundles_to_node_id = table["bundles_to_node_id"]

    assert sorted(result) == list(range(4))
    _assert_grouped_by_node(result, bundles_to_node_id)

    driver_bundles = [i for i in result if bundles_to_node_id[i] == driver_node_id]
    if driver_bundles:
        assert result[: len(driver_bundles)] == driver_bundles

    placement_group_assert_no_leak([pg])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
