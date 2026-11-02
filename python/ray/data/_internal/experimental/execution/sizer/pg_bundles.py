"""Placement-group bundles as capacity slots, so PG ops size like node-placed
ops: each bundle is a "node" an actor must fit inside."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Iterable, Tuple

from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.experimental.execution.sizer.actor_placement_strategy import (
    pack_actors,
)

if TYPE_CHECKING:
    from ray.data._internal.experimental.execution.actor_only_resource_reporter import (  # noqa: E501
        DatasetAllocationView,
    )
    from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
        ExperimentalActorPoolMapOperator,
    )

logger = logging.getLogger(__name__)

# (placement group id hex, bundle index).
BundleKey = Tuple[str, int]


@dataclass(frozen=True)
class PlacementGroupBundles:
    """Bundle shapes of every placement group the topology uses, and which of
    them each static-PG op may land in.

    Shapes are fixed at PG creation, so this is read once when the sizer is
    built. A PG the view has no bundles for (not created yet, or the PG table
    unreadable) is left out: its ops' PG capacity is unknown, and the sizer
    caps them at their pool's max size instead (see
    ``OperatorSizer._read_pg_capacity``).
    """

    shapes: Dict[BundleKey, ExecutionResources] = field(default_factory=dict)
    # All of the op's PG bundles, or just ``placement_group_bundle_index``.
    bundles_by_op: Dict[
        "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
    ] = field(default_factory=dict)

    @classmethod
    def measure(
        cls,
        ops: Iterable["ExperimentalActorPoolMapOperator"],
        view: "DatasetAllocationView",
    ) -> "PlacementGroupBundles":
        shapes: Dict[BundleKey, ExecutionResources] = {}
        bundles_by_op: Dict[
            "ExperimentalActorPoolMapOperator", Tuple[BundleKey, ...]
        ] = {}
        for op in ops:
            constraint = op.placement_constraint()
            if constraint.placement_group is None:
                continue
            pg_id = constraint.placement_group.id.hex()
            bundles = view.pg_bundles.get(pg_id)
            if not bundles:
                continue
            index = constraint.placement_group_bundle_index
            if index is not None and index >= len(bundles):
                # Ray would reject this index; size against every bundle
                # rather than none.
                logger.warning(
                    "%s: placement_group_bundle_index=%d but placement group %s "
                    "has %d bundle(s); sizing against all of them.",
                    op.name,
                    index,
                    pg_id,
                    len(bundles),
                )
                index = None
            if index is not None and index >= 0:
                indices: Iterable[int] = (index,)
            else:
                # None, or Ray's -1: any bundle.
                indices = range(len(bundles))
            keys = tuple((pg_id, i) for i in indices)
            for key in keys:
                shapes[key] = bundles[key[1]]
            bundles_by_op[op] = keys
        return cls(shapes=shapes, bundles_by_op=bundles_by_op)

    def free_by_bundle(self) -> Dict[BundleKey, ExecutionResources]:
        """Each bundle's shape minus what the PG ops already hold in it.

        Ray core picks the bundle and never reports it back, so held actors
        (``total_size``: a draining actor keeps its bundle until dead) are
        assumed packed first-fit, most constrained op first. If core packed
        differently the result is optimistic; the excess pendings wait and
        are released by the stuck-pending expiry.
        """
        free = {key: shape.copy() for key, shape in self.shapes.items()}
        for op in sorted(self.bundles_by_op, key=lambda o: len(self.bundles_by_op[o])):
            pool = op.actor_pool
            pack_actors(
                free,
                self.bundles_by_op[op],
                pool.per_actor_resource_usage(),
                pool.total_size(),
            )
        return free
