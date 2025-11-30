from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.operators.actor_pool_map_operator import _ActorState
from ray.data._internal.execution.operators.actor_removal_strategy import (
    DefaultActorRemovalStrategy,
    NodeAwareActorRemovalStrategy,
)


def test_default_actor_removal_strategy():
    """Test the default actor removal strategy."""
    strategy = DefaultActorRemovalStrategy()

    # mock actors
    actor1 = MagicMock()
    actor2 = MagicMock()
    actor3 = MagicMock()

    running_actors = {
        actor1: _ActorState(
            num_tasks_in_flight=2, actor_location="node1", is_restarting=False
        ),
        actor2: _ActorState(
            num_tasks_in_flight=0, actor_location="node1", is_restarting=False
        ),
        actor3: _ActorState(
            num_tasks_in_flight=0, actor_location="node2", is_restarting=False
        ),
    }

    # Should return the first idle actor (actor2 or actor3)
    selected = strategy.select_actor_to_remove(running_actors)
    assert selected in [actor2, actor3]

    # Should return None when there are no idle actors
    running_actors_busy = {
        actor1: _ActorState(
            num_tasks_in_flight=2, actor_location="node1",  is_restarting=False
        ),
        actor2: _ActorState(
            num_tasks_in_flight=1, actor_location="node1", is_restarting=False
        ),
    }
    selected = strategy.select_actor_to_remove(running_actors_busy)
    assert selected is None


def test_node_aware_actor_removal_strategy():
    """Test the node-aware actor removal strategy."""
    strategy = NodeAwareActorRemovalStrategy()

    # mock actors distributed across different nodes
    actor1 = MagicMock()
    actor2 = MagicMock()
    actor3 = MagicMock()
    actor4 = MagicMock()
    actor5 = MagicMock()

    running_actors = {
        # node1: 1 idle actor
        actor1: _ActorState(
            num_tasks_in_flight=1, actor_location="node1", is_restarting=False
        ),
        actor2: _ActorState(
            num_tasks_in_flight=0, actor_location="node1", is_restarting=False
        ),
        # node2: 3 idle actors
        actor3: _ActorState(
            num_tasks_in_flight=0, actor_location="node2", is_restarting=False
        ),
        actor4: _ActorState(
            num_tasks_in_flight=0, actor_location="node2", is_restarting=False
        ),
        actor5: _ActorState(
            num_tasks_in_flight=0, actor_location="node2", is_restarting=False
        ),
    }

    # Should select an actor from node2 (because node2 has the most idle actors)
    selected = strategy.select_actor_to_remove(running_actors)
    assert selected in [actor3, actor4, actor5]

    # Test the logic for counting idle actors per node
    idle_counts = strategy._count_idle_actors_per_node(running_actors)
    assert idle_counts["node1"] == 1
    assert idle_counts["node2"] == 3


def test_node_aware_strategy_no_idle_actors():
    """Test the behavior when there are no idle actors."""
    strategy = NodeAwareActorRemovalStrategy()

    actor1 = MagicMock()
    actor2 = MagicMock()

    running_actors = {
        actor1: _ActorState(
            num_tasks_in_flight=2, actor_location="node1", is_restarting=False
        ),
        actor2: _ActorState(
            num_tasks_in_flight=1, actor_location="node2", is_restarting=False
        ),
    }

    selected = strategy.select_actor_to_remove(running_actors)
    assert selected is None


def test_node_aware_strategy_single_node():
    """Test the behavior on a single node."""
    strategy = NodeAwareActorRemovalStrategy()

    actor1 = MagicMock()
    actor2 = MagicMock()

    running_actors = {
        actor1: _ActorState(
            num_tasks_in_flight=0, actor_location="node1", is_restarting=False
        ),
        actor2: _ActorState(
            num_tasks_in_flight=0, actor_location="node1", is_restarting=False
        ),
    }

    # Should select an actor from the single node
    selected = strategy.select_actor_to_remove(running_actors)
    assert selected in [actor1, actor2]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
