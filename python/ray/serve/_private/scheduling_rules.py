"""Rules that decide which nodes a Serve replica may be placed on."""

from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Set

from ray.serve._private.common import DeploymentID


@dataclass
class SchedulingContext:
    """Cluster facts a rule reads to judge one replica."""

    deployment_id: DeploymentID
    node_labels: Dict[str, Dict[str, str]]


class SchedulingConstraint:
    """A rule about which nodes a replica may be placed on."""

    def describe(self) -> str:
        """A short name for logs."""
        return type(self).__name__

    def allowed_nodes(
        self, node_ids: Iterable[str], ctx: SchedulingContext
    ) -> Set[str]:
        """The nodes among `node_ids` the replica may be placed on."""
        return set(node_ids)


class ExcludeNodesConstraint(SchedulingConstraint):
    """Keeps replicas off a changing set of nodes."""

    def __init__(self, nodes: Callable[[], Set[str]]):
        self._nodes = nodes

    def allowed_nodes(
        self, node_ids: Iterable[str], ctx: SchedulingContext
    ) -> Set[str]:
        return set(node_ids) - self._nodes()
