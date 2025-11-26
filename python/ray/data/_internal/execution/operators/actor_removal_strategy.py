from typing import TYPE_CHECKING, Optional, Dict, List
import ray

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from .actor_pool_map_operator import _ActorState


class ActorRemovalStrategy:
    """Strategy for selecting which actor to remove during scale down"""

    def select_actor_to_remove(
        self,
        running_actors: Dict["ActorHandle", "_ActorState"]
    ) -> Optional["ActorHandle"]:
        """Select an idle actor to remove. Returns None if no idle actors."""
        raise NotImplementedError


class DefaultActorRemovalStrategy(ActorRemovalStrategy):
    """Default strategy: remove first idle actor found"""

    def select_actor_to_remove(
        self,
        running_actors: Dict["ActorHandle", "_ActorState"]
    ) -> Optional["ActorHandle"]:
        for actor, state in running_actors.items():
            if state.num_tasks_in_flight == 0:
                # At least one idle actor, so kill first one found.
                # NOTE: This is a fire-and-forget op
                return actor
        # No idle actors, so indicate to the caller that no actors were killed.
        return None


class NodeAwareActorRemovalStrategy(ActorRemovalStrategy):
    """优先从空闲 actor 最多的节点上移除 actor 的策略"""

    def select_actor_to_remove(
        self,
        running_actors: Dict["ActorHandle", "_ActorState"]
    ) -> Optional["ActorHandle"]:
        """选择一个空闲 actor 进行移除,优先选择空闲 actor 最多的节点"""
        # 统计每个节点上的空闲 actor 数量
        idle_counts_per_node = self._count_idle_actors_per_node(running_actors)

        if not idle_counts_per_node:
            # 没有空闲 actor
            return None

            # 找到空闲 actor 最多的节点
        target_node = max(idle_counts_per_node.keys(),
                          key=lambda n: idle_counts_per_node[n])

        # 从该节点上选择一个空闲 actor
        for actor, state in running_actors.items():
            if state.num_tasks_in_flight == 0 and state.actor_location == target_node:
                return actor

        return None

    def _count_idle_actors_per_node(
        self,
        running_actors: Dict["ActorHandle", "_ActorState"]
    ) -> Dict[str, int]:
        """统计每个节点上的空闲 actor 数量

        使用 actor_location 字段而不是 node_id
        """
        idle_counts = {}
        for actor, state in running_actors.items():
            if state.num_tasks_in_flight == 0:
                node_id = state.actor_location  # 使用现有的 actor_location 字段
                idle_counts[node_id] = idle_counts.get(node_id, 0) + 1
        return idle_counts
