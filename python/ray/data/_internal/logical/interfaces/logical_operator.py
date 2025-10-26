from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional

from .operator import Operator
from ray.data.block import BlockMetadata
from ray.data.expressions import col

if TYPE_CHECKING:
    from ray.data._internal.logical.operators.map_operator import Project
    from ray.data.block import Schema


class LogicalOperator(Operator):
    """Abstract class for logical operators.

    A logical operator describes transformation, and later is converted into
    physical operator.
    """

    def __init__(
        self,
        name: str,
        input_dependencies: List["LogicalOperator"],
        num_outputs: Optional[int] = None,
    ):
        super().__init__(
            name,
            input_dependencies,
        )
        for x in input_dependencies:
            assert isinstance(x, LogicalOperator), x

        self._num_outputs: Optional[int] = num_outputs

    def estimated_num_outputs(self) -> Optional[int]:
        """Returns the estimated number of blocks that
        would be outputted by this logical operator.

        This method does not execute the plan, so it does not take into consideration
        block splitting. This method only considers high-level block constraints like
        `Dataset.repartition(num_blocks=X)`. A more accurate estimation can be given by
        `PhysicalOperator.num_outputs_total()` during execution.
        """
        if self._num_outputs is not None:
            return self._num_outputs
        elif len(self._input_dependencies) == 1:
            return self._input_dependencies[0].estimated_num_outputs()
        return None

    # Override the following 3 methods to correct type hints.

    @property
    def input_dependencies(self) -> List["LogicalOperator"]:
        return super().input_dependencies  # type: ignore

    @property
    def output_dependencies(self) -> List["LogicalOperator"]:
        return super().output_dependencies  # type: ignore

    def post_order_iter(self) -> Iterator["LogicalOperator"]:
        return super().post_order_iter()  # type: ignore

    def _apply_transform(
        self, transform: Callable[["LogicalOperator"], "LogicalOperator"]
    ) -> "LogicalOperator":
        return super()._apply_transform(transform)  # type: ignore

    def _get_args(self) -> Dict[str, Any]:
        """This Dict must be serializable"""
        return vars(self)

    def infer_schema(self) -> Optional["Schema"]:
        """Returns the inferred schema of the output blocks."""
        return None

    def infer_metadata(self) -> "BlockMetadata":
        """A ``BlockMetadata`` that represents the aggregate metadata of the outputs.

        This method is used by methods like :meth:`~ray.data.Dataset.schema` to
        efficiently return metadata.
        """
        return BlockMetadata(None, None, None, None)

    def is_lineage_serializable(self) -> bool:
        """Returns whether the lineage of this operator can be serialized.

        An operator is lineage serializable if you can serialize it on one machine and
        deserialize it on another without losing information. Operators that store
        object references (e.g., ``InputData``) aren't lineage serializable because the
        objects aren't available on the deserialized machine.
        """
        return True


class LogicalOperatorSupportsProjectionPushdown(LogicalOperator):
    """Mixin for reading operators supporting projection pushdown"""

    def supports_projection_pushdown(self) -> bool:
        return False

    def get_current_projection(self) -> Optional[List[str]]:
        return None

    def apply_projection(
        self,
        columns: Optional[List[str]],
        column_rename_map: Optional[Dict[str, str]],
    ) -> LogicalOperator:
        return self


class SupportsPushThrough(LogicalOperator):
    """Mixin for operators supporting projection pushthrough"""

    def supports_projection_pushthrough(self) -> bool:
        return True

    # TODO(justin): this will be replaced by LogicalOperatorContainsPartitionKeys
    def get_current_keys(self) -> Optional[List[str]]:
        return None

    def _rename_projection(
        self,
        old_keys: List[str],
        column_rename_map: Dict[str, str],
    ) -> List[str]:

        new_keys = []
        for old_key in old_keys:
            if old_key in column_rename_map:
                new_key = column_rename_map[old_key]
                new_keys.append(new_key)
            else:
                new_keys.append(old_key)
        return new_keys

    def _create_upstream_project(
        self,
        columns: List[str],
        column_rename_map: Optional[Dict[str, str]],
        input_op: LogicalOperator,
    ) -> "Project":
        from ray.data._internal.logical.operators.map_operator import Project

        # NOTE: This can happen when we union the same dataset. The same
        # dataset is only shallowed copied, so we safeguard removing
        # output dependencies more than once.
        if self in input_op.output_dependencies:
            input_op.output_dependencies.remove(self)

        new_exprs = []
        for old_col in columns:
            if old_col in column_rename_map:
                new_col = column_rename_map[old_col]
                new_exprs.append(col(old_col).alias(new_col))
            else:
                new_exprs.append(col(old_col))

        return Project(
            input_op=input_op,
            exprs=new_exprs,
            compute=None,
            ray_remote_args=None,
        )

    def apply_projection(
        self,
        columns: List[str],
        column_rename_map: Dict[str, str],
    ) -> LogicalOperator:
        raise NotImplementedError
