"""Comprehensive tests for the generic ranker type system."""

from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.ranker import (
    DefaultRanker,
    Ranker,
    ResidentInputRanker,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import Topology


def test_default_ranker():
    """Test that the ranker interface works correctly."""
    ranker = DefaultRanker()

    # Mock objects
    op1 = MagicMock()
    op1.throttling_disabled.return_value = False
    op2 = MagicMock()
    op2.throttling_disabled.return_value = True
    topology = {}
    resource_manager = MagicMock()
    resource_manager.get_op_usage.return_value = MagicMock()
    resource_manager.get_op_usage.return_value.object_store_memory = 1024

    # Test rank_operator for first op
    rank1 = ranker.rank_operator(op1, topology, resource_manager)
    assert rank1 == (1, 1024)  # throttling_disabled=False -> 1, memory=1024

    # Test rank_operator for second op
    rank2 = ranker.rank_operator(op2, topology, resource_manager)
    assert rank2 == (0, 1024)  # throttling_disabled=True -> 0, memory=1024

    # Test rank_operators with both ops
    ops = [op1, op2]
    ranks = ranker.rank_operators(ops, topology, resource_manager)
    assert ranks == [(1, 1024), (0, 1024)]


def _op_state_with_inputs(*resident_per_queue: bool) -> MagicMock:
    state = MagicMock()
    state.input_queues = [
        MagicMock(has_resident_next=MagicMock(return_value=resident))
        for resident in resident_per_queue
    ]
    return state


def test_resident_input_ranker():
    ranker = ResidentInputRanker()

    throttled_resident = MagicMock()
    throttled_resident.throttling_disabled.return_value = False
    throttled_lost = MagicMock()
    throttled_lost.throttling_disabled.return_value = False
    unthrottled_lost = MagicMock()
    unthrottled_lost.throttling_disabled.return_value = True
    topology = {
        throttled_resident: _op_state_with_inputs(False, True),
        throttled_lost: _op_state_with_inputs(False),
        unthrottled_lost: _op_state_with_inputs(),
    }
    resource_manager = MagicMock()
    resource_manager.get_op_usage.return_value.object_store_memory = 1024.0

    ranks = ranker.rank_operators(list(topology), topology, resource_manager)
    assert ranks == [(1, 0, 1024.0), (1, 1, 1024.0), (0, 1, 1024.0)]
    # Ordering: unthrottleable first, then ops with a resident input bundle.
    assert sorted(range(3), key=ranks.__getitem__) == [2, 0, 1]


class IntRanker(Ranker[int]):
    """Ranker that returns integer rankings."""

    def rank_operator(
        self,
        op: PhysicalOperator,
        topology: "Topology",
        resource_manager: ResourceManager,
    ) -> int:
        """Return integer ranking."""
        return resource_manager.get_op_usage(op).object_store_memory


def test_generic_types():
    """Test that specific generic types work correctly."""
    # Test integer ranker
    int_ranker = IntRanker()
    op1 = MagicMock()
    op2 = MagicMock()
    topology = {}
    resource_manager = MagicMock()
    resource_manager.get_op_usage.return_value = MagicMock()
    resource_manager.get_op_usage.return_value.object_store_memory = 1024

    # Test rank_operator for first op
    rank1 = int_ranker.rank_operator(op1, topology, resource_manager)
    assert rank1 == 1024

    # Test rank_operator for second op
    rank2 = int_ranker.rank_operator(op2, topology, resource_manager)
    assert rank2 == 1024

    # Test rank_operators with both ops
    ops = [op1, op2]
    ranks = int_ranker.rank_operators(ops, topology, resource_manager)
    assert ranks == [1024, 1024]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
