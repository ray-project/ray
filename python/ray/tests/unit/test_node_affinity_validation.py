import re
import sys

import pytest

from ray._raylet import NodeID
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def test_node_affinity_scheduling_strategy_invalid_attributes():
    valid_hex = NodeID.from_random().hex()
    with pytest.raises(
        ValueError,
        match="Invalid NodeAffinitySchedulingStrategy attribute. "
        "_spill_on_unavailable cannot be set when soft is False. "
        "Please set soft to True to use _spill_on_unavailable.",
    ):
        NodeAffinitySchedulingStrategy(
            node_id=valid_hex, soft=False, _spill_on_unavailable=True
        )
    with pytest.raises(
        ValueError,
        match="Invalid NodeAffinitySchedulingStrategy attribute. "
        "_fail_on_unavailable cannot be set when soft is True. "
        "Please set soft to False to use _fail_on_unavailable.",
    ):
        NodeAffinitySchedulingStrategy(
            node_id=valid_hex, soft=True, _fail_on_unavailable=True
        )

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Invalid node_id 'invalid_node_id'. Node ID must be a valid "
            "hex string. To get a list of all nodes and their IDs in your cluster, "
            "use ray.nodes(). See https://docs.ray.io/en/latest/ray-core/miscellaneous.html"
            "#node-information for more details."
        ),
    ):
        NodeAffinitySchedulingStrategy(
            node_id="invalid_node_id", soft=True, _fail_on_unavailable=True
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
