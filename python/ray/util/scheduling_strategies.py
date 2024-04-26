from typing import Dict, Union, Optional, TYPE_CHECKING
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.util.placement_group import PlacementGroup

# "DEFAULT": The default hybrid scheduling strategy
# based on config scheduler_spread_threshold.
# This disables any potential placement group capture.

# "SPREAD": Spread scheduling on a best effort basis.


@PublicAPI
class PlacementGroupSchedulingStrategy:
    """Placement group based scheduling strategy.

    Attributes:
        placement_group: the placement group this actor belongs to,
            or None if it doesn't belong to any group.
        placement_group_bundle_index: the index of the bundle
            if the actor belongs to a placement group, which may be -1 to
            specify any available bundle.
        placement_group_capture_child_tasks: Whether or not children tasks
            of this actor should implicitly use the same placement group
            as its parent. It is False by default.
    """

    def __init__(
        self,
        placement_group: "PlacementGroup",
        placement_group_bundle_index: int = -1,
        placement_group_capture_child_tasks: Optional[bool] = None,
    ):
        self.placement_group = placement_group
        self.placement_group_bundle_index = placement_group_bundle_index
        self.placement_group_capture_child_tasks = placement_group_capture_child_tasks


@PublicAPI
class NodeAffinitySchedulingStrategy:
    """Static scheduling strategy used to run a task or actor on a particular node.

    Attributes:
        node_id: the hex id of the node where the task or actor should run.
        soft: whether the scheduler should run the task or actor somewhere else
            if the target node doesn't exist (e.g. the node dies) or is infeasible
            during scheduling.
            If the node exists and is feasible, the task or actor
            will only be scheduled there.
            This means if the node doesn't have the available resources,
            the task or actor will wait indefinitely until resources become available.
            If the node doesn't exist or is infeasible, the task or actor
            will fail if soft is False
            or be scheduled somewhere else if soft is True.
    """

    def __init__(
        self,
        node_id: str,
        soft: bool,
        _spill_on_unavailable: bool = False,
        _fail_on_unavailable: bool = False,
    ):
        # This will be removed once we standardize on node id being hex string.
        if not isinstance(node_id, str):
            node_id = node_id.hex()

        self.node_id = node_id
        self.soft = soft
        self._spill_on_unavailable = _spill_on_unavailable
        self._fail_on_unavailable = _fail_on_unavailable


def _validate_label_match_operator_values(values, operator):
    if not values:
        raise ValueError(
            f"The variadic parameter of the {operator} operator"
            f' must be a non-empty tuple: e.g. {operator}("value1", "value2").'
        )

    index = 0
    for value in values:
        if not isinstance(value, str):
            raise ValueError(
                f"Type of value in position {index} for the {operator} operator "
                f'must be str (e.g. {operator}("value1", "value2")) '
                f"but got {str(value)} of type {type(value)}."
            )
        index = index + 1


@PublicAPI(stability="alpha")
class In:
    def __init__(self, *values):
        _validate_label_match_operator_values(values, "In")
        self.values = list(values)


@PublicAPI(stability="alpha")
class NotIn:
    def __init__(self, *values):
        _validate_label_match_operator_values(values, "NotIn")
        self.values = list(values)


@PublicAPI(stability="alpha")
class Exists:
    def __init__(self):
        pass


@PublicAPI(stability="alpha")
class DoesNotExist:
    def __init__(self):
        pass


class _LabelMatchExpression:
    """An expression used to select node by node's labels
    Attributes:
        key: the key of label
        operator: In、NotIn、Exists、DoesNotExist
    """

    def __init__(self, key: str, operator: Union[In, NotIn, Exists, DoesNotExist]):
        self.key = key
        self.operator = operator


LabelMatchExpressionsT = Dict[str, Union[In, NotIn, Exists, DoesNotExist]]


@PublicAPI(stability="alpha")
class NodeLabelSchedulingStrategy:
    """Label based node affinity scheduling strategy

    scheduling_strategy=NodeLabelSchedulingStrategy({
          "region": In("us"),
          "gpu_type": Exists()
    })
    """

    def __init__(
        self, hard: LabelMatchExpressionsT, *, soft: LabelMatchExpressionsT = None
    ):
        self.hard = _convert_map_to_expressions(hard, "hard")
        self.soft = _convert_map_to_expressions(soft, "soft")
        self._check_usage()

    def _check_usage(self):
        if not (self.hard or self.soft):
            raise ValueError(
                "The `hard` and `soft` parameter "
                "of NodeLabelSchedulingStrategy cannot both be empty."
            )


def _convert_map_to_expressions(map_expressions: LabelMatchExpressionsT, param: str):
    expressions = []
    if map_expressions is None:
        return expressions

    if not isinstance(map_expressions, Dict):
        raise ValueError(
            f'The {param} parameter must be a map (e.g. {{"key1": In("value1")}}) '
            f"but got type {type(map_expressions)}."
        )

    for key, value in map_expressions.items():
        if not isinstance(key, str):
            raise ValueError(
                f"The map key of the {param} parameter must "
                f'be of type str (e.g. {{"key1": In("value1")}}) '
                f"but got {str(key)} of type {type(key)}."
            )

        if not isinstance(value, (In, NotIn, Exists, DoesNotExist)):
            raise ValueError(
                f"The map value for key {key} of the {param} parameter "
                f"must be one of the `In`, `NotIn`, `Exists` or `DoesNotExist` "
                f'operator (e.g. {{"key1": In("value1")}}) '
                f"but got {str(value)} of type {type(value)}."
            )

        expressions.append(_LabelMatchExpression(key, value))
    return expressions


SchedulingStrategyT = Union[
    None,
    str,  # Literal["DEFAULT", "SPREAD"]
    PlacementGroupSchedulingStrategy,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
]
