import pytest
import sys

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def test_node_affinity_scheduling_strategy_invalid_attributes():
    with pytest.raises(
        ValueError,
        match="Invalid NodeAffinitySchedulingStrategy attribute. "
        "_spill_on_unavailable cannot be set when soft is False. "
        "Please set soft to True to use _spill_on_unavailable.",
    ):
        NodeAffinitySchedulingStrategy(
            node_id="123", soft=False, _spill_on_unavailable=True
        )
    with pytest.raises(
        ValueError,
        match="Invalid NodeAffinitySchedulingStrategy attribute. "
        "_fail_on_unavailable cannot be set when soft is True. "
        "Please set soft to False to use _fail_on_unavailable.",
    ):
        NodeAffinitySchedulingStrategy(
            node_id="123", soft=True, _fail_on_unavailable=True
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
