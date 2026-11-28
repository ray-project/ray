"""One operator's scheduling constraints, parsed once for the sizer."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, AbstractSet, Dict, Optional

from ray.data._internal.execution.interfaces import ExecutionResources, NodeIdStr
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    PLACEMENT_RESOURCE_DIMS,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.actor_only_resource_reporter import (  # noqa: E501
        DatasetAllocationView,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlacementConstraint:
    """A node pin (``ray.io/node-id`` / ``NodeAffinity``) is just a
    ``label_selector`` on the node-id label -- there is no separate pin
    concept. ``matching_nodes`` returns the constraint-match set; capacity is
    layered on by the caller."""

    # The op's ``label_selector`` (a node pin appears here as
    # ``{"ray.io/node-id": <node>}``). None if the op selects no labels.
    label_selector: Optional[Dict[str, str]] = None
    # Placement group + optional bundle index. PG ops are sized against their
    # bundles (see ``PlacementGroupBundles``) but placed by Ray core, so they
    # impose no label filter.
    placement_group: Optional["PlacementGroup"] = None
    placement_group_bundle_index: Optional[int] = None
    # True when a ``ray_remote_args_fn`` mints a fresh PG per actor. We cannot
    # inspect that PG here (calling the fn would create one)
    mints_pg_per_actor: bool = False

    @property
    def has_placement_group(self) -> bool:
        return self.placement_group is not None

    @property
    def is_pg_managed(self) -> bool:
        """True when the actors are placed through a placement group (a static
        PG, or one minted per actor), so the sizer never picks their node."""
        return self.has_placement_group or self.mints_pg_per_actor

    @property
    def has_label_selector(self) -> bool:
        return self.label_selector is not None

    @staticmethod
    def _matches_selector(
        node_labels: Dict[str, str], label_selector: Dict[str, str]
    ) -> bool:
        """Ray core's matcher, so the grammar is identical by construction."""
        import ray

        # pyrefly: ignore[missing-attribute]  # Cython module attribute.
        return ray._raylet.node_labels_match_selector(node_labels, label_selector)

    def matching_nodes(
        self,
        view: "DatasetAllocationView",
        per_actor: ExecutionResources,
    ) -> AbstractSet[NodeIdStr]:
        """Allocated nodes the op is allowed on.

        No selector -> every allocated node. Otherwise the nodes whose labels
        satisfy the selector. On an unparseable selector, degrade to "schedulable
        everywhere" with a WARN -- never an empty set.

        Nodes whose slice is smaller than one actor (a GPU actor on a CPU-only
        node) are also excluded: no amount of freed capacity makes them
        placeable.
        """
        all_nodes = frozenset(view.reserved_by_node)
        if self.label_selector is None:
            return self._filter_by_resource(all_nodes, view, per_actor)
        try:
            matched = frozenset(
                node_id
                for node_id in all_nodes
                if self._matches_selector(
                    view.labels_by_node.get(node_id, {}), self.label_selector
                )
            )
        except Exception:
            if log_once(
                f"sizer_bad_label_selector_{sorted(self.label_selector.items())}"
            ):
                logger.warning(
                    "could not evaluate label_selector %s against %d node(s); "
                    "treating the operator as unconstrained.",
                    self.label_selector,
                    len(view.labels_by_node),
                    exc_info=True,
                )
            return self._filter_by_resource(all_nodes, view, per_actor)
        return self._filter_by_resource(matched, view, per_actor)

    @staticmethod
    def _filter_by_resource(
        nodes: AbstractSet[NodeIdStr],
        view: "DatasetAllocationView",
        per_actor: ExecutionResources,
    ) -> AbstractSet[NodeIdStr]:
        """Drop nodes whose slice is too small to ever hold one actor.
        Structural, distinct from the has-free-capacity check callers layer
        on top: a node big enough but currently full stays schedulable."""
        return frozenset(
            node_id
            for node_id in nodes
            if all(
                getattr(view.reserved_by_node[node_id], dim) >= getattr(per_actor, dim)
                for dim in PLACEMENT_RESOURCE_DIMS
            )
        )


def is_constrained(op: "ExperimentalActorPoolMapOperator") -> bool:
    """True if ``op`` can run on only some nodes: it has a label selector, or
    it needs a GPU."""
    return (
        op.placement_constraint().has_label_selector
        or op.actor_pool.per_actor_resource_usage().gpu > 0
    )
