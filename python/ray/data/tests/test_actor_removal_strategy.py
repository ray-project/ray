import pytest
from unittest.mock import MagicMock
import ray
from ray.data._internal.execution.operators.actor_removal_strategy import (
    DefaultActorRemovalStrategy,
    NodeAwareActorRemovalStrategy
)
from ray.data._internal.execution.operators.actor_pool_map_operator import \
    _ActorState


def test_default_actor_removal_strategy():
    """测试默认的 actor 移除策略"""
    strategy = DefaultActorRemovalStrategy()

    # 创建 mock actors
    actor1 = MagicMock()
    actor2 = MagicMock()
    actor3 = MagicMock()

    running_actors = {
        actor1: _ActorState(num_tasks_in_flight=2, actor_location="node1",
                            is_restarting=False),
        actor2: _ActorState(num_tasks_in_flight=0, actor_location="node1",
                            is_restarting=False),
        actor3: _ActorState(num_tasks_in_flight=0, actor_location="node2",
                            is_restarting=False),
    }

    # 应该返回第一个空闲的 actor (actor2 或 actor3)
    selected = strategy.select_actor_to_remove(running_actors)
    assert selected in [actor2, actor3]

    # 没有空闲 actor 时返回 None
    running_actors_busy = {
        actor1: _ActorState(num_tasks_in_flight=2, actor_location="node1",
                            is_restarting=False),
        actor2: _ActorState(num_tasks_in_flight=1, actor_location="node1",
                            is_restarting=False),
    }
    selected = strategy.select_actor_to_remove(running_actors_busy)
    assert selected is None


def test_node_aware_actor_removal_strategy():
    """测试节点感知的 actor 移除策略"""
    strategy = NodeAwareActorRemovalStrategy()

    # 创建 mock actors 分布在不同节点
    actor1 = MagicMock()
    actor2 = MagicMock()
    actor3 = MagicMock()
    actor4 = MagicMock()
    actor5 = MagicMock()

    running_actors = {
        # node1: 1 个空闲 actor
        actor1: _ActorState(num_tasks_in_flight=1, actor_location="node1",
                            is_restarting=False),
        actor2: _ActorState(num_tasks_in_flight=0, actor_location="node1",
                            is_restarting=False),
        # node2: 3 个空闲 actors
        actor3: _ActorState(num_tasks_in_flight=0, actor_location="node2",
                            is_restarting=False),
        actor4: _ActorState(num_tasks_in_flight=0, actor_location="node2",
                            is_restarting=False),
        actor5: _ActorState(num_tasks_in_flight=0, actor_location="node2",
                            is_restarting=False),
    }

    # 应该从 node2 选择一个 actor (因为 node2 有最多的空闲 actors)
    selected = strategy.select_actor_to_remove(running_actors)
    assert selected in [actor3, actor4, actor5]

    # 验证统计逻辑
    idle_counts = strategy._count_idle_actors_per_node(running_actors)
    assert idle_counts["node1"] == 1
    assert idle_counts["node2"] == 3


def test_node_aware_strategy_no_idle_actors():
    """测试没有空闲 actor 时的行为"""
    strategy = NodeAwareActorRemovalStrategy()

    actor1 = MagicMock()
    actor2 = MagicMock()

    running_actors = {
        actor1: _ActorState(num_tasks_in_flight=2, actor_location="node1",
                            is_restarting=False),
        actor2: _ActorState(num_tasks_in_flight=1, actor_location="node2",
                            is_restarting=False),
    }

    selected = strategy.select_actor_to_remove(running_actors)
    assert selected is None


def test_node_aware_strategy_single_node():
    """测试单节点情况"""
    strategy = NodeAwareActorRemovalStrategy()

    actor1 = MagicMock()
    actor2 = MagicMock()

    running_actors = {
        actor1: _ActorState(num_tasks_in_flight=0, actor_location="node1",
                            is_restarting=False),
        actor2: _ActorState(num_tasks_in_flight=0, actor_location="node1",
                            is_restarting=False),
    }

    # 应该从唯一的节点选择一个 actor
    selected = strategy.select_actor_to_remove(running_actors)
    assert selected in [actor1, actor2]
