import enum
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Sequence, Union as TypingUnion

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorUnifiesInputSchemas,
    PredicatePassThroughBehavior,
)
from ray.data._internal.utils.transform_pyarrow import _create_empty_table
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow as pa

    from ray.data.block import Schema

__all__ = [
    "Mix",
    "MixStoppingCondition",
    "NAry",
    "Union",
    "Zip",
]


@PublicAPI(stability="alpha")
class MixStoppingCondition(enum.Enum):
    """Controls when a mix pipeline terminates.

    STOP_ON_SHORTEST: Pipeline ends when the shortest dataset is exhausted.
        Other datasets are truncated.
    STOP_ON_LONGEST_DROP: Pipeline ends when the longest dataset is exhausted.
        Shorter datasets drop out once exhausted; later batches are drawn
        entirely from longer datasets.
    """

    STOP_ON_SHORTEST = "stop_on_shortest"
    STOP_ON_LONGEST_DROP = "stop_on_longest_drop"


def estimate_num_mix_outputs(
    per_input_counts: List[Optional[int]],
    weights: List[float],
    stopping_condition: MixStoppingCondition,
) -> Optional[int]:
    """Estimate total output count for a mix operation.

    Used by both the logical and physical Mix operators to estimate
    num_outputs_total / num_output_rows_total.
    """
    if any(c is None for c in per_input_counts):
        return None
    if stopping_condition == MixStoppingCondition.STOP_ON_LONGEST_DROP:
        return sum(per_input_counts)
    elif stopping_condition == MixStoppingCondition.STOP_ON_SHORTEST:
        # Limited by whichever input runs out first relative to its weight.
        total_weight = sum(weights)
        return min(
            int(count / (w / total_weight))
            for count, w in zip(per_input_counts, weights)
        )
    else:
        raise ValueError(f"Unknown stopping condition: {stopping_condition}")


@dataclass(frozen=True, repr=False, eq=False, init=False)
class NAry(LogicalOperator):
    """Base class for n-ary operators, which take multiple input operators."""

    def __init__(
        self,
        input_dependencies: List[LogicalOperator],
    ):
        """Initialize the n-ary operator.

        Args:
            input_dependencies: The input operators.
        """
        object.__setattr__(self, "_input_dependencies", list(input_dependencies))

    def _with_new_input_dependencies(
        self, input_dependencies: List[LogicalOperator]
    ) -> LogicalOperator:
        return self.__class__(input_dependencies)


@dataclass(frozen=True, repr=False, eq=False, init=False)
class Zip(NAry):
    """Logical operator for zip."""

    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)

    def __init__(
        self,
        input_dependencies: List[LogicalOperator],
    ):
        for input_op in input_dependencies:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_input_dependencies", list(input_dependencies))

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs = max(total_num_outputs, num_outputs)
        return total_num_outputs

    def infer_schema(self) -> Optional["Schema"]:
        # Reuse the runtime ``BlockAccessor.zip`` so plan-time and
        # execution-time schemas agree by construction (same column
        # suffixing rules, etc.).
        import pyarrow as pa

        from ray.data.block import BlockAccessor

        input_schemas = [op.infer_schema() for op in self.input_dependencies]
        if not input_schemas or not all(
            isinstance(s, pa.Schema) for s in input_schemas
        ):
            return None
        try:
            combined = _create_empty_table(input_schemas[0])
            for s in input_schemas[1:]:
                combined = BlockAccessor.for_block(combined).zip(_create_empty_table(s))
        except (pa.ArrowTypeError, pa.ArrowInvalid):
            return None
        return combined.schema


@dataclass(frozen=True, repr=False, eq=False, init=False)
class Mix(NAry, LogicalOperatorUnifiesInputSchemas):
    """Logical operator for weighted dataset mixing."""

    _name: str = field(init=False, repr=False)
    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)
    weights: List[float] = field(init=False, repr=False)
    stopping_condition: MixStoppingCondition = field(init=False, repr=False)

    def __init__(
        self,
        input_dependencies: List[LogicalOperator],
        *,
        weights: List[float],
        stopping_condition: MixStoppingCondition = MixStoppingCondition.STOP_ON_SHORTEST,
    ):
        if len(input_dependencies) != len(weights):
            raise ValueError(
                f"Number of input operators ({len(input_dependencies)}) must match "
                f"number of weights ({len(weights)})."
            )
        if any(weight <= 0 for weight in weights):
            raise ValueError(f"Weights must be positive. Got weights: {weights}")

        for input_op in input_dependencies:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_name", self.__class__.__name__)
        object.__setattr__(self, "_input_dependencies", list(input_dependencies))
        object.__setattr__(self, "weights", weights)
        object.__setattr__(self, "stopping_condition", stopping_condition)

    def estimated_num_outputs(self) -> Optional[int]:
        if self.stopping_condition == MixStoppingCondition.STOP_ON_SHORTEST:
            return None

        return estimate_num_mix_outputs(
            [op.estimated_num_outputs() for op in self.input_dependencies],
            self.weights,
            self.stopping_condition,
        )

    def _with_new_input_dependencies(
        self, input_dependencies: List[LogicalOperator]
    ) -> LogicalOperator:
        return self.__class__(
            input_dependencies,
            weights=self.weights,
            stopping_condition=self.stopping_condition,
        )


@dataclass(frozen=True, repr=False, eq=False, init=False)
class Union(
    NAry,
    LogicalOperatorSupportsPredicatePassThrough,
    LogicalOperatorUnifiesInputSchemas,
):
    """Logical operator for union."""

    _input_dependencies: List[LogicalOperator] = field(init=False, repr=False)

    def __init__(
        self,
        input_dependencies: List[LogicalOperator],
    ):
        for input_op in input_dependencies:
            assert isinstance(input_op, LogicalOperator), input_op
        object.__setattr__(self, "_input_dependencies", list(input_dependencies))

    def estimated_num_outputs(self):
        total_num_outputs = 0
        for input in self.input_dependencies:
            num_outputs = input.estimated_num_outputs()
            if num_outputs is None:
                return None
            total_num_outputs += num_outputs
        return total_num_outputs

    def predicate_passthrough_behavior(self) -> PredicatePassThroughBehavior:
        # Union allows pushing filter into each branch
        return PredicatePassThroughBehavior.PUSH_INTO_BRANCHES


def _unify_union_schemas(
    schemas: Sequence[Optional[TypingUnion[type, "Schema"]]],
) -> "pa.Schema":
    """Compute the target schema for a name-aligned union."""
    import pyarrow as pa

    from ray.data._internal.arrow_block import (
        _BATCH_SIZE_PRESERVING_STUB_COL_NAME,
        _is_user_visible_column,
    )

    fields = {}
    field_counts = Counter()
    metadata = None
    for input_index, schema in enumerate(schemas, start=1):
        if not isinstance(schema, pa.Schema):
            raise ValueError(
                f"Union input {input_index} must have a known Arrow schema when "
                "allow_missing_columns=True."
            )
        if len(set(schema.names)) != len(schema):
            raise ValueError(f"Union input {input_index} has duplicate column names.")
        if input_index == 1:
            metadata = schema.metadata
        for column_field in schema:
            name = column_field.name
            if not _is_user_visible_column(name):
                continue
            if name not in fields:
                fields[name] = column_field
            elif fields[name].type != column_field.type:
                raise ValueError(
                    f"Union input {input_index} has type {column_field.type} for column "
                    f"{name!r}, which conflicts with type {fields[name].type} "
                    "in an earlier input."
                )
            elif column_field.nullable:
                fields[name] = fields[name].with_nullable(True)
            field_counts[name] += 1

    target_fields = [
        column_field.with_nullable(
            column_field.nullable or field_counts[name] < len(schemas)
        )
        for name, column_field in fields.items()
    ]
    if not target_fields:
        # A physical placeholder keeps row counts when zero-column blocks are combined.
        target_fields = [pa.field(_BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.null())]
    return pa.schema(target_fields, metadata=metadata)
