from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from .actor_pool_map_operator import _ActorState
    from ray.actor import ActorHandle


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
    """Select an idle actor to remove, preferring nodes with the most idle actors."""

    def select_actor_to_remove(
        self,
        running_actors: Dict["ActorHandle", "_ActorState"]
    ) -> Optional["ActorHandle"]:
        """Select an idle actor to remove, preferring nodes with the most idle actors."""

        # Count idle actors per node
        idle_counts_per_node = self._count_idle_actors_per_node(running_actors)

        if not idle_counts_per_node:
            return None

        # Find the node with the most idle actors
        target_node = max(idle_counts_per_node.keys(),
                          key=lambda n: idle_counts_per_node[n])

        # Select an idle actor from the target node
        for actor, state in running_actors.items():
            if state.num_tasks_in_flight == 0 and state.actor_location == target_node:
                return actor

        return None

    def _count_idle_actors_per_node(
        self,
        running_actors: Dict["ActorHandle", "_ActorState"]
    ) -> Dict[str, int]:
        """Count idle actors per node"""
        idle_counts = {}
        for actor, state in running_actors.items():
            if state.num_tasks_in_flight == 0:
                node_id = state.actor_location
                idle_counts[node_id] = idle_counts.get(node_id, 0) + 1
        return idle_counts
