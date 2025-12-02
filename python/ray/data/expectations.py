import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
)

from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data.expressions import Expr


class ExpectationType(str, Enum):
    """Type of expectation."""

    DATA_QUALITY = "data_quality"
    EXECUTION_TIME = "execution_time"


@DeveloperAPI
@dataclass
class Expectation:
    """Base class for all expectations.

    Expectations can be attached to dataset operations or functions to
    express data quality requirements or execution time constraints.

    Attributes:
        name: Human-readable name for this expectation.
        description: Detailed description of what this expectation checks.
        expectation_type: Type of expectation (data quality or execution time).
        error_on_failure: If True, raise an exception when expectation fails.
            If False, log a warning.
    """

    name: str
    description: str
    expectation_type: ExpectationType
    error_on_failure: bool = True

    def __post_init__(self):
        if not self.name or (isinstance(self.name, str) and not self.name.strip()):
            raise ValueError("Expectation name cannot be empty or whitespace-only")
        if not self.description or (
            isinstance(self.description, str) and not self.description.strip()
        ):
            raise ValueError(
                "Expectation description cannot be empty or whitespace-only"
            )

    def validate(self, *args, **kwargs) -> bool:
        """Validate this expectation. Subclasses must implement this."""
        raise NotImplementedError("Subclasses must implement validate()")


@DeveloperAPI
@dataclass
class DataQualityExpectation(Expectation):
    """Data quality expectation for validating data correctness.

    Use this to express constraints on data values, schema, completeness,
    or other data quality metrics.

    Attributes:
        name: Human-readable name for this expectation.
        description: Detailed description of what this expectation checks.
        validator_fn: Function that takes a batch (dict or pandas DataFrame)
            and returns True if validation passes, False otherwise.
            Can also raise exceptions for more detailed error reporting.
        error_on_failure: If True, raise an exception when expectation fails.
            If False, log a warning.
    """

    validator_fn: Callable[[Any], bool]
    expectation_type: ExpectationType = field(
        default=ExpectationType.DATA_QUALITY, init=False
    )

    def validate(self, batch: Any) -> bool:
        """Validate a batch of data against this expectation.

        Args:
            batch: A batch of data in any supported format (dict, pandas DataFrame,
                PyArrow Table, etc.). Can be empty.

        Returns:
            True if validation passes, False otherwise.

        Raises:
            ValueError: If validator function returns non-boolean value.
        """
        from ray.data.block import BlockAccessor

        if batch is None:
            return False

        block_accessor = BlockAccessor.for_block(batch)
        if block_accessor.num_rows() == 0:
            return True

        result = self.validator_fn(batch)
        if not isinstance(result, bool):
            raise ValueError(
                f"Validator function must return bool, got {type(result).__name__}. "
                "Validator functions should return True if validation passes, "
                "False otherwise."
            )
        return result


@DeveloperAPI
@dataclass
class ExecutionTimeExpectation(Expectation):
    """Execution time expectation for expressing timing requirements.

    Use this to express execution time constraints like "Job must finish by Y time".

    Attributes:
        name: Human-readable name for this expectation.
        description: Detailed description of what this execution time constraint requires.
        max_execution_time_seconds: Maximum allowed execution time in seconds.
            If None, no time constraint is enforced.
        max_execution_time: Maximum allowed execution time as datetime.timedelta.
            Alternative to max_execution_time_seconds.
        target_completion_time: Target completion time as datetime.datetime.
            Used for deadline-based optimization.
        error_on_failure: If True, raise an exception when execution time constraint is violated.
            If False, log a warning.
    """

    max_execution_time_seconds: Optional[float] = None
    max_execution_time: Optional[datetime.timedelta] = None
    target_completion_time: Optional[datetime.datetime] = None
    expectation_type: ExpectationType = field(
        default=ExpectationType.EXECUTION_TIME, init=False
    )

    def __post_init__(self):
        super().__post_init__()
        if (
            self.max_execution_time_seconds is not None
            and self.max_execution_time is not None
        ):
            raise ValueError(
                "Cannot specify both max_execution_time_seconds and max_execution_time"
            )
        if self.max_execution_time_seconds is None and self.max_execution_time is None:
            if self.target_completion_time is None:
                raise ValueError(
                    "Must specify at least one time constraint: max_execution_time_seconds, "
                    "max_execution_time, or target_completion_time"
                )

    def get_max_execution_time_seconds(self) -> Optional[float]:
        """Get maximum execution time in seconds."""
        if self.max_execution_time_seconds is not None:
            return self.max_execution_time_seconds
        if self.max_execution_time is not None:
            return self.max_execution_time.total_seconds()
        if self.target_completion_time is not None:
            now = datetime.datetime.now()
            if self.target_completion_time <= now:
                # Target time is in the past - return a very small positive value
                # to indicate timeout immediately, but still positive for validation
                return 0.001
            return (self.target_completion_time - now).total_seconds()
        return None

    def validate(self, execution_time_seconds: float) -> bool:
        """Validate that execution time meets requirements."""
        max_time = self.get_max_execution_time_seconds()
        if max_time is None:
            return True
        return execution_time_seconds <= max_time


@DeveloperAPI
@dataclass
class ExpectationResult:
    """Result of validating an expectation.

    Attributes:
        expectation: The expectation that was validated.
        passed: Whether the expectation passed.
        message: Human-readable message describing the result.
        execution_time_seconds: Execution time in seconds (for execution time expectations).
        failure_count: Number of batches/rows that failed validation (for data quality).
        total_count: Total number of batches/rows validated (for data quality).
    """

    expectation: Expectation
    passed: bool
    message: str
    execution_time_seconds: Optional[float] = None
    failure_count: int = 0
    total_count: int = 0

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        return (
            f"ExpectationResult(expectation={self.expectation.name}, "
            f"status={status}, message={self.message})"
        )


@PublicAPI(stability="alpha")
def expect(
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    validator_fn: Optional[Callable[[Any], bool]] = None,
    expr: Optional["Expr"] = None,
    max_execution_time_seconds: Optional[float] = None,
    max_execution_time: Optional[datetime.timedelta] = None,
    target_completion_time: Optional[datetime.datetime] = None,
    error_on_failure: bool = True,
    expectation_type: Optional[ExpectationType] = None,
) -> Expectation:
    """Create an expectation object for data quality or execution time requirements.

    Examples:
        >>> from ray.data.expressions import col
        >>> from ray.data.expectations import expect
        >>>
        >>> # Expression-based data quality
        >>> exp = expect(expr=col("value") > 0)
        >>> ds = ray.data.from_items([{"value": 1}, {"value": -1}])
        >>> passed_ds, failed_ds, result = ds.expect(exp)
        >>>
        >>> # Validator function
        >>> exp = expect(validator_fn=lambda batch: batch["value"].min() > 0)
        >>>
        >>> # Execution time requirement
        >>> exp = expect(max_execution_time_seconds=60.0)

    Args:
        name: Name for the expectation.
        description: Description of what this expectation checks.
        validator_fn: Function for data quality validation (takes batch, returns bool).
            Mutually exclusive with `expr`.
        expr: Expression for data quality validation (e.g., col("value") > 0).
            Mutually exclusive with `validator_fn`.
        max_execution_time_seconds: Maximum execution time in seconds (for execution time expectations).
        max_execution_time: Maximum execution time as datetime.timedelta (for execution time expectations).
        target_completion_time: Target completion time as datetime.datetime (for execution time expectations).
        error_on_failure: If True, raise exception on failure; if False, log warning.
        expectation_type: Type of expectation (auto-detected if not specified).

    Returns:
        An Expectation object that can be used with Dataset.expect().
    """
    # Validate input types
    if validator_fn is not None and not callable(validator_fn):
        raise TypeError(
            f"validator_fn must be callable, got {type(validator_fn).__name__}"
        )
    if max_execution_time is not None and not isinstance(
        max_execution_time, datetime.timedelta
    ):
        raise TypeError(
            f"max_execution_time must be datetime.timedelta, got {type(max_execution_time).__name__}"
        )
    if target_completion_time is not None and not isinstance(
        target_completion_time, datetime.datetime
    ):
        raise TypeError(
            f"target_completion_time must be datetime.datetime, got {type(target_completion_time).__name__}"
        )
    if name is not None and not isinstance(name, str):
        raise TypeError(f"name must be str, got {type(name).__name__}")
    if description is not None and not isinstance(description, str):
        raise TypeError(f"description must be str, got {type(description).__name__}")

    # Handle expression-based data quality expectations
    _expr = expr

    # Validate expr is an Expr object if provided
    if _expr is not None:
        from ray.data.expressions import Expr as _Expr

        if not isinstance(_expr, _Expr):
            raise TypeError(
                f"expr must be a Ray Data Expr object, got {type(_expr).__name__}. "
                f"Use col('column_name') > 0 or similar expression."
            )

    # Check if expr is provided as keyword argument
    if _expr is not None:
        if validator_fn is not None:
            raise ValueError(
                "Cannot specify both `validator_fn` and `expr` for data quality expectations. "
                "Use either `validator_fn` for custom validation logic or `expr` for "
                "expression-based validation."
            )
        if (
            max_execution_time_seconds is not None
            or max_execution_time is not None
            or target_completion_time is not None
        ):
            raise ValueError(
                "Cannot specify both `expr` (data quality) and time constraints (execution time). "
                "Use `expr` for data quality validation or time constraints for execution time requirements."
            )
        validator_fn = _create_validator_from_expression(_expr)
        expectation_type = ExpectationType.DATA_QUALITY

    # Determine expectation type if not specified
    if expectation_type is None:
        if validator_fn is not None or _expr is not None:
            expectation_type = ExpectationType.DATA_QUALITY
        elif (
            max_execution_time_seconds is not None
            or max_execution_time is not None
            or target_completion_time is not None
        ):
            expectation_type = ExpectationType.EXECUTION_TIME
        else:
            raise ValueError(
                "Must specify either validator_fn or expr (for data quality) "
                "or time constraints (for execution time). "
                "Examples: expect(expr=col('x') > 0) or expect(max_execution_time_seconds=60)"
            )

    # Create expectation object
    if expectation_type == ExpectationType.DATA_QUALITY:
        if validator_fn is None:
            raise ValueError(
                "Either validator_fn or expr is required for data quality expectations. "
                "This should not happen - please report this error."
            )
        if name is None:
            name = "Data Quality Check"
        if description is None:
            description = (
                f"Data quality validation: {_expr}"
                if _expr is not None
                else "Data quality validation"
            )
        exp = DataQualityExpectation(
            name=name,
            description=description,
            validator_fn=validator_fn,
            error_on_failure=error_on_failure,
        )
        # Store expression for efficient filter()-based validation
        if _expr is not None:
            exp._expr = _expr
    else:
        # Validate execution time parameters
        if max_execution_time_seconds is not None and max_execution_time_seconds <= 0:
            raise ValueError(
                f"max_execution_time_seconds must be positive, "
                f"got {max_execution_time_seconds}"
            )
        if max_execution_time is not None and max_execution_time.total_seconds() <= 0:
            raise ValueError(
                f"max_execution_time must be positive, got {max_execution_time}"
            )

        if name is None:
            name = "Execution Time Requirement"
        if description is None:
            description = "Execution time constraint"
        exp = ExecutionTimeExpectation(
            name=name,
            description=description,
            max_execution_time_seconds=max_execution_time_seconds,
            max_execution_time=max_execution_time,
            target_completion_time=target_completion_time,
            error_on_failure=error_on_failure,
        )

    # Return expectation object
    return exp


def _extract_boolean_result(result: Any) -> bool:
    """Extract boolean result from expression evaluation."""
    import pyarrow as pa

    if isinstance(result, bool):
        return result
    elif isinstance(result, (pa.Array, pa.ChunkedArray)):
        if len(result) == 0:
            return True
        values = result.to_pylist()
        non_null_values = [v for v in values if v is not None]
        return bool(non_null_values) and all(bool(v) for v in non_null_values)
    elif hasattr(result, "to_pylist"):
        values = result.to_pylist()
        non_null_values = [v for v in values if v is not None]
        return bool(non_null_values) and all(v for v in non_null_values)
    else:
        return bool(result)


def _create_validator_from_expression(expr: "Expr") -> Callable[[Any], bool]:
    """Create a validator function from a Ray Data expression.

    Uses Ray Data's existing expression evaluation infrastructure (eval_expr),
    ensuring consistent behavior with filter() and other expression-based operations.
    """
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_expr,
    )
    from ray.data.block import BlockAccessor

    def validator_fn(batch: Any) -> bool:
        """Validate that all rows in batch satisfy the expression."""
        block_accessor = BlockAccessor.for_block(batch)
        if block_accessor.num_rows() == 0:
            return True
        block = block_accessor.to_arrow()
        result = eval_expr(expr, block)
        return _extract_boolean_result(result)

    return validator_fn
