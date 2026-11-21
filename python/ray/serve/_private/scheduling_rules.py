"""Rules that decide which nodes a Serve replica may be placed on."""

from collections import Counter
from dataclasses import dataclass, field
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from ray.serve._private.common import DeploymentID

RAY_NODE_ID_LABEL = "ray.io/node-id"


def label_value(
    node_id: str, label_key: str, node_labels: Mapping[str, Mapping[str, str]]
) -> Optional[str]:
    """The node's `label_key` value, or None. A node's `ray.io/node-id` is its id."""
    value = node_labels.get(node_id, {}).get(label_key)
    if value is None and label_key == RAY_NODE_ID_LABEL:
        return node_id
    return value


@dataclass
class SchedulingContext:
    """Cluster facts a rule reads to judge one replica."""

    deployment_id: DeploymentID
    node_labels: Dict[str, Dict[str, str]]
    replicas_per_node: Mapping[str, int] = field(default_factory=dict)
    num_replicas: int = 0
    candidate_nodes: Optional[Set[str]] = None
    _num_domains: Dict[str, int] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )

    def num_domains(self, label_key: str) -> int:
        """How many values of `label_key` the candidate nodes span."""
        if label_key not in self._num_domains:
            nodes = (
                self.candidate_nodes
                if self.candidate_nodes is not None
                else self.node_labels
            )
            self._num_domains[label_key] = len(
                {
                    domain
                    for node_id in nodes
                    if (domain := label_value(node_id, label_key, self.node_labels))
                    is not None
                }
            )
        return self._num_domains[label_key]


class SchedulingConstraint:
    """A rule about which nodes a replica may be placed on.

    Required rules are never dropped.
    """

    required: bool = False

    def describe(self) -> str:
        """A short name for logs."""
        return type(self).__name__

    def allowed_nodes(
        self, node_ids: Iterable[str], ctx: SchedulingContext
    ) -> Set[str]:
        """The nodes among `node_ids` the replica may be placed on."""
        return set(node_ids)

    def selector(self, ctx: SchedulingContext) -> Dict[str, str]:
        """The rule as a Ray Core label selector, empty if it allows every node."""
        return {}

    def may_stop(self, node_id: str, ctx: SchedulingContext) -> bool:
        """Whether one replica on the node may stop.

        `ctx.num_replicas` is the replica count after the downscale.
        """
        return True


class ExcludeNodesConstraint(SchedulingConstraint):
    """Keeps replicas off a changing set of nodes."""

    required = True

    def __init__(self, nodes: Callable[[], Set[str]]):
        self._nodes = nodes

    def allowed_nodes(
        self, node_ids: Iterable[str], ctx: SchedulingContext
    ) -> Set[str]:
        return set(node_ids) - self._nodes()

    def selector(self, ctx: SchedulingContext) -> Dict[str, str]:
        nodes = self._nodes()
        if not nodes:
            return {}
        return {RAY_NODE_ID_LABEL: f"!in({','.join(sorted(nodes))})"}


class MinTopologyDomainsConstraint(SchedulingConstraint):
    """Spreads replicas over at least `min_domains` values of `label_key`.

    The floor is capped at the replica count and, unless required, the
    domains the replica can use.
    """

    def __init__(
        self,
        label_key: str,
        min_domains: int,
        required: bool = False,
        wait_timeout_s: Optional[float] = None,
    ):
        self._label_key = label_key
        self._min_domains = min_domains
        self.required = required
        self.wait_timeout_s = wait_timeout_s

    @property
    def label_key(self) -> str:
        return self._label_key

    @property
    def min_domains(self) -> int:
        return self._min_domains

    def describe(self) -> str:
        kind = "required " if self.required else ""
        return f"{kind}topology_spread[{self._label_key}>={self._min_domains}]"

    def covered_domains(self, ctx: SchedulingContext) -> Set[str]:
        """The domains the deployment's replicas are on."""
        domains = set()
        for node_id, count in ctx.replicas_per_node.items():
            domain = label_value(node_id, self._label_key, ctx.node_labels)
            if count > 0 and domain is not None:
                domains.add(domain)
        return domains

    def effective_floor(self, ctx: SchedulingContext) -> int:
        """The floor capped at the replica count and, unless required, the domains."""
        if self.required:
            return min(self._min_domains, ctx.num_replicas)
        return min(
            self._min_domains, ctx.num_replicas, ctx.num_domains(self._label_key)
        )

    def is_met(self, ctx: SchedulingContext) -> bool:
        return self.missing_domains(ctx) == 0

    def missing_domains(self, ctx: SchedulingContext) -> int:
        """How many more domains the floor needs."""
        return max(0, self.effective_floor(ctx) - len(self.covered_domains(ctx)))

    def may_stop(self, node_id: str, ctx: SchedulingContext) -> bool:
        """Refuses a stop that would leave fewer domains than the floor."""
        domain = label_value(node_id, self._label_key, ctx.node_labels)
        if domain is None:
            return True
        replicas_per_domain: Counter[str] = Counter()
        for other_node, count in ctx.replicas_per_node.items():
            other_domain = label_value(other_node, self._label_key, ctx.node_labels)
            if count > 0 and other_domain is not None:
                replicas_per_domain[other_domain] += count
        if replicas_per_domain[domain] > 1:
            return True
        return len(replicas_per_domain) - 1 >= self.effective_floor(ctx)

    def selector(self, ctx: SchedulingContext) -> Dict[str, str]:
        """Avoids covered domains, and unlabeled nodes, while unmet."""
        if self.is_met(ctx):
            return {}
        expressions = []
        if self._label_key != RAY_NODE_ID_LABEL:
            expressions.append("exists()")
        covered = self.covered_domains(ctx)
        if covered:
            expressions.append(f"!in({','.join(sorted(covered))})")
        if not expressions:
            return {}
        return {self._label_key: ",".join(expressions)}

    def allowed_nodes(
        self, node_ids: Iterable[str], ctx: SchedulingContext
    ) -> Set[str]:
        if self.is_met(ctx):
            return set(node_ids)
        covered = self.covered_domains(ctx)
        return {
            node_id
            for node_id in node_ids
            if (domain := label_value(node_id, self._label_key, ctx.node_labels))
            is not None
            and domain not in covered
        }


def merge_selectors(selectors: Iterable[Mapping[str, str]]) -> Dict[str, str]:
    """Joins label selectors into one that requires all of them."""
    merged: Dict[str, List[str]] = {}
    for selector in selectors:
        for key, value in selector.items():
            expressions = merged.setdefault(key, [])
            if value not in expressions:
                expressions.append(value)
    return {key: ",".join(expressions) for key, expressions in merged.items()}


def rule_tiers(
    rules: Sequence[SchedulingConstraint],
) -> List[Tuple[SchedulingConstraint, ...]]:
    """Rule sets to try, strictest first. Each drops the last optional rule."""
    required = tuple(rule for rule in rules if rule.required)
    optional = [rule for rule in rules if not rule.required]
    return [
        required + tuple(optional[:num_optional])
        for num_optional in range(len(optional), -1, -1)
    ]
