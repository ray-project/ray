from typing import List

from ray.data._internal.logical.interfaces import LogicalOperator


class Count(LogicalOperator):
    """An operator that counts the number of rows in input bundles.

    This operator is a no-op. It exists so that we can apply counting-related logical
    optimizations.
    """

    def __init__(
        self,
        input_dependencies: List["LogicalOperator"],
    ):
        super().__init__("Count", input_dependencies)
