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
else:
    # Import for runtime type checking
    try:
        from ray.data.expressions import Expr as _ExprType  # noqa: F401
    except ImportError:
        _ExprType = None  # type: ignore


def _is_expr(obj: Any) -> bool:
    """Check if an object is a Ray Data expression.

    Uses isinstance if Expr is available, otherwise falls back to duck typing.
    This ensures we can detect expressions even if the import fails.
    """
    # Import here to avoid circular dependencies and handle lazy import
    try:
        from ray.data.expressions import Expr as ExprType

        return isinstance(obj, ExprType)
    except (ImportError, TypeError):
        pass

    # Duck typing fallback for Expr detection
    # Expectation is defined later in the file, but we can check by class name
    obj_type = type(obj).__name__
    if obj_type == "Expectation" or obj_type.endswith("Expectation"):
        return False
    if callable(obj):
        return False
    return hasattr(obj, "to_pyarrow") and hasattr(obj, "data_type")


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
        if not self.name:
            raise ValueError("Expectation name cannot be empty")
        if not self.description:
            raise ValueError("Expectation description cannot be empty")

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
            Exception: If error_on_failure is True and validation fails.
        """
        try:
            # Handle empty batches gracefully using BlockAccessor
            # This reuses Ray Data's standard batch format handling
            from ray.data.block import BlockAccessor

            if batch is None:
                return False

            # Use BlockAccessor for consistent empty batch detection across formats
            try:
                block_accessor = BlockAccessor.for_block(batch)
                if block_accessor.num_rows() == 0:
                    # Empty batches pass validation (no data to validate)
                    return True
            except Exception:
                # Fallback for unsupported formats
                pass

            result = self.validator_fn(batch)
            if not isinstance(result, bool):
                raise ValueError(
                    f"Validator function must return bool, got {type(result).__name__}. "
                    "Validator functions should return True if validation passes, "
                    "False otherwise."
                )
            return result
        except Exception:
            if self.error_on_failure:
                raise
            return False


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
                    "Must specify either max_execution_time_seconds, "
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
                return 0.0
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
        max_execution_time: Maximum execution time as timedelta (for execution time expectations).
        target_completion_time: Target completion time as datetime (for execution time expectations).
        error_on_failure: If True, raise exception on failure; if False, log warning.
        expectation_type: Type of expectation (auto-detected if not specified).

    Returns:
        An Expectation object that can be used with Dataset.expect().
    """
    # Handle expression-based data quality expectations
    _expr = expr

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


def _convert_batch_to_arrow_block(batch: Any) -> Any:
    """Convert a batch to PyArrow Table format for expression evaluation.

    Uses Ray Data's BlockAccessor pattern to handle all batch formats consistently.
    This is the same pattern used throughout Ray Data for batch format handling.

    Supports:
        - PyArrow Tables (https://arrow.apache.org/docs/python/)
        - Pandas DataFrames
        - Dict[str, np.ndarray] format
        - Any format supported by BlockAccessor.for_block()

    Args:
        batch: Batch in any supported format.

    Returns:
        PyArrow Table suitable for expression evaluation.
    """
    import pyarrow as pa

    from ray.data.block import BlockAccessor

    # Use BlockAccessor - this is the standard Ray Data way to handle batches
    try:
        accessor = BlockAccessor.for_block(batch)
        return accessor.to_arrow()
    except (TypeError, AttributeError, ValueError):
        # Fallback for edge cases
        if isinstance(batch, pa.Table):
            return batch
        elif hasattr(batch, "to_arrow"):
            return batch.to_arrow()
        elif isinstance(batch, dict):
            return pa.table(batch)
        else:
            # Try pandas conversion if available
            # Pandas: https://pandas.pydata.org/docs/
            try:
                import pandas as pd

                if isinstance(batch, pd.DataFrame):
                    return pa.Table.from_pandas(batch)
            except Exception:
                pass
            # Final fallback: assume it's already a PyArrow Table or compatible
            return batch


def _extract_boolean_result(result: Any) -> bool:
    """Extract boolean result from expression evaluation.

    Handles PyArrow Arrays, scalars, and other array-like objects.
    Returns True if all non-null values are True, False otherwise.
    Empty batches/arrays return True (nothing to validate).
    """
    import pyarrow as pa

    if isinstance(result, bool):
        return result
    elif isinstance(result, (pa.Array, pa.ChunkedArray)):
        # PyArrow Array/ChunkedArray - check if all values are True
        if len(result) == 0:
            return True  # Empty batch passes validation

        values = result.to_pylist()
        if not values:
            return True  # Empty list passes validation

        # Filter out None values (nulls) and check if all remaining are True
        non_null_values = [v for v in values if v is not None]
        if not non_null_values:
            # All values are null - treat as passing (no data to validate)
            return True

        # All non-null values must be True
        return all(v is True for v in non_null_values)
    elif isinstance(result, (list, tuple)):
        if not result:
            return True
        non_null_values = [v for v in result if v is not None]
        if not non_null_values:
            return True
        return all(v is True for v in non_null_values)
    elif hasattr(result, "to_pylist"):
        values = result.to_pylist()
        if not values:
            return True
        non_null_values = [v for v in values if v is not None]
        if not non_null_values:
            return True
        return all(v is True for v in non_null_values)
    else:
        # Try to convert to bool (for scalar results)
        try:
            return bool(result)
        except (TypeError, ValueError):
            # If conversion fails, assume False
            return False


def _create_validator_from_expression(expr: "Expr") -> Callable[[Any], bool]:
    """Create a validator function from a Ray Data expression.

    Uses Ray Data's existing expression evaluation infrastructure (eval_expr),
    ensuring consistent behavior with filter() and other expression-based operations.
    """

    def validator_fn(batch: Any) -> bool:
        """Validate that all rows in batch satisfy the expression."""
        try:
            # Import here to avoid circular dependencies
            from ray.data._internal.planner.plan_expression.expression_evaluator import (
                eval_expr,
            )
            from ray.data.block import BlockAccessor

            # Use BlockAccessor for consistent empty batch detection
            # This reuses Ray Data's standard batch format handling
            try:
                block_accessor = BlockAccessor.for_block(batch)
                if block_accessor.num_rows() == 0:
                    # Empty batches pass validation (no data to validate)
                    return True
                # Convert to Arrow format using BlockAccessor
                block = block_accessor.to_arrow()
            except Exception:
                # Fallback to manual conversion if BlockAccessor fails
                block = _convert_batch_to_arrow_block(batch)
                try:
                    import pyarrow as pa

                    if isinstance(block, pa.Table) and len(block) == 0:
                        return True
                except Exception:
                    pass

            # Evaluate expression using the same path as filter(expr=...) and with_column()
            result = eval_expr(expr, block)

            # Extract boolean result from various return types
            return _extract_boolean_result(result)

        except Exception as e:
            # If evaluation fails, consider it a validation failure
            # Include the expression in error message for debugging
            error_msg = f"Failed to evaluate expression {expr} on batch"
            if hasattr(e, "__cause__") and e.__cause__:
                error_msg += f": {e.__cause__}"
            elif str(e):
                error_msg += f": {e}"
            raise ValueError(error_msg) from e

    return validator_fn
