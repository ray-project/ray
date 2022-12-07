from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_operator import MapOperator


class MapOperatorActorsImpl:
    def __init__(self, op: "MapOperator"):
        pass
