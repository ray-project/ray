import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Union,
    TYPE_CHECKING,
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
    SLA = "sla"


class OptimizationStrategy(str, Enum):
    """Optimization strategy hint based on SLA requirements."""

    COST = "cost"
    PERFORMANCE = "performance"
    BALANCED = "balanced"


@DeveloperAPI
@dataclass
class Expectation:
    """Base class for all expectations.

    Expectations can be attached to dataset operations or functions to
    express data quality requirements or SLA constraints.

    Attributes:
        name: Human-readable name for this expectation.
        description: Detailed description of what this expectation checks.
        expectation_type: Type of expectation (data quality or SLA).
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
                    f"Validator function must return bool, got {type(result)}"
                )
            return result
        except Exception:
            if self.error_on_failure:
                raise
            return False


@DeveloperAPI
@dataclass
class SLAExpectation(Expectation):
    """SLA expectation for expressing performance and timing requirements.

    Use this to express business SLAs like "Job must finish by Y time"
    to guide optimization strategies (cost-based vs performance-based).

    Attributes:
        name: Human-readable name for this expectation.
        description: Detailed description of what this SLA requires.
        max_execution_time_seconds: Maximum allowed execution time in seconds.
            If None, no time constraint is enforced.
        max_execution_time: Maximum allowed execution time as datetime.timedelta.
            Alternative to max_execution_time_seconds.
        optimization_strategy: Hint for optimization strategy.
            COST: Optimize for cost (use fewer resources, slower execution).
            PERFORMANCE: Optimize for performance (use more resources, faster execution).
            BALANCED: Balance between cost and performance.
        target_completion_time: Target completion time as datetime.datetime.
            Used for deadline-based optimization.
        error_on_failure: If True, raise an exception when SLA is violated.
            If False, log a warning.
    """

    max_execution_time_seconds: Optional[float] = None
    max_execution_time: Optional[datetime.timedelta] = None
    optimization_strategy: OptimizationStrategy = OptimizationStrategy.BALANCED
    target_completion_time: Optional[datetime.datetime] = None
    expectation_type: ExpectationType = field(default=ExpectationType.SLA, init=False)

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
        """Validate that execution time meets SLA requirements."""
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
        execution_time_seconds: Execution time in seconds (for SLA expectations).
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
    expectation: Optional[Union[Expectation, Callable, "Expr"]] = None,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    validator_fn: Optional[Callable[[Any], bool]] = None,
    expr: Optional["Expr"] = None,
    max_execution_time_seconds: Optional[float] = None,
    max_execution_time: Optional[datetime.timedelta] = None,
    target_completion_time: Optional[datetime.datetime] = None,
    optimization_strategy: Union[
        OptimizationStrategy, str
    ] = OptimizationStrategy.BALANCED,
    error_on_failure: bool = True,
    expectation_type: Optional[ExpectationType] = None,
) -> Union[Callable, Expectation]:
    """Decorator to attach expectations to functions or create expectation objects.

    This decorator supports both enterprise data SLA requirements and data quality rules.
    It can be used in multiple ways:

    1. As a decorator without parentheses to attach a default expectation:
        @ray.data.expect
        def my_processing_function(data):
            return process(data)

    2. As a decorator with parentheses to attach SLA requirements:
        @ray.data.expect(max_execution_time_seconds=300, optimization_strategy="performance")
        def my_processing_function(data):
            return process(data)

    3. As a decorator with expressions for data quality rules:
        from ray.data.expressions import col
        @ray.data.expect(expr=col("value") > 0)
        def my_processing_function(data):
            return process(data)

    4. As a function to create expectation objects with expressions:
        from ray.data.expressions import col
        expectation = ray.data.expect(
            name="positive_values",
            expr=col("value") > 0
        )

    5. As a function to create expectation objects with validator functions:
        expectation = ray.data.expect(
            name="data_quality_check",
            validator_fn=lambda batch: batch["value"].min() > 0
        )

    6. With an existing expectation object:
        expectation = ray.data.expect(name="...", ...)
        @ray.data.expect(expectation)
        def my_function(data):
            return process(data)

    Examples:

        Enterprise SLA Requirements:
            >>> import ray
            >>> from ray.data.expectations import expect
            >>>
            >>> # Attach SLA requirement for performance optimization
            >>> @expect(max_execution_time_seconds=300, optimization_strategy="performance")
            >>> def process_data(batch):
            ...     return batch
            >>>
            >>> ds = ray.data.from_items([{"value": i} for i in range(100)])
            >>> ds = ds.map_batches(process_data)

        Data Quality Rules with Expressions (Pythonic/Ray-like):
            >>> from ray.data.expressions import col, lit
            >>>
            >>> # Simple expression as decorator (most Pythonic)
            >>> @expect(expr=col("value") > 0, name="positive_values")
            >>> def process_data(batch):
            ...     return batch
            >>>
            >>> # Or pass expression directly as positional arg
            >>> @expect(col("value") > 0)
            >>> def process_data(batch):
            ...     return batch
            >>>
            >>> ds = ray.data.from_items([{"value": i} for i in range(100)])
            >>> ds = ds.map_batches(process_data)

        Data Quality Rules with Complex Expressions:
            >>> from ray.data.expressions import col, lit
            >>>
            >>> # Complex expression for data quality
            >>> quality_check = expect(
            ...     name="valid_range",
            ...     expr=(col("value") >= lit(0)) & (col("value") <= lit(100))
            ... )
            >>>
            >>> ds = ray.data.from_items([{"value": i} for i in range(100)])
            >>> # Can use expression directly in Dataset.expect() too
            >>> validated_ds, result = ds.expect(quality_check)
            >>> # Or even simpler:
            >>> validated_ds, result = ds.expect(expr=col("value") > 0)

    Args:
        expectation: An existing Expectation object to attach, an expression (Expr),
            or a function when used as decorator without parentheses.
        name: Name for the expectation.
        description: Description of what this expectation checks.
        validator_fn: Function for data quality validation (takes batch, returns bool).
            Mutually exclusive with `expr`.
        expr: Expression for data quality validation (e.g., col("value") > 0).
            Mutually exclusive with `validator_fn`. Creates a data quality expectation
            that validates every row in each batch satisfies the expression.
        max_execution_time_seconds: Maximum execution time in seconds (for SLA).
        max_execution_time: Maximum execution time as timedelta (for SLA).
        target_completion_time: Target completion time as datetime (for SLA).
        optimization_strategy: Optimization strategy hint (cost/performance/balanced).
        error_on_failure: If True, raise exception on failure; if False, log warning.
        expectation_type: Type of expectation (auto-detected if not specified).

    Returns:
        If used as decorator without parentheses, returns the decorated function.
        If used as decorator with parentheses, returns a decorator function.
        If called directly, returns an Expectation object.

    Raises:
        ValueError: If both `validator_fn` and `expr` are provided, or if neither
            validator_fn/expr nor time constraints are provided.
    """
    # Handle expression-based data quality expectations
    # Check if expr is provided as keyword argument: @expect(expr=col("value") > 0)
    # Or as positional argument: @expect(col("value") > 0)
    _expr = expr

    # Check if expectation is an Expr (better type checking)
    if _expr is None and expectation is not None and _is_expr(expectation):
        _expr = expectation
        expectation = None

    if _expr is not None:
        if validator_fn is not None:
            raise ValueError(
                "Cannot specify both `validator_fn` and `expr` for data quality expectations. "
                "Use either `validator_fn` for custom validation logic or `expr` for expression-based validation."
            )
        validator_fn = _create_validator_from_expression(_expr)
        expectation_type = ExpectationType.DATA_QUALITY

    # Handle decorator usage without parentheses: @expect
    # When used as @expect, Python passes the function as the first positional arg
    if callable(expectation) and not any(
        [
            name,
            description,
            validator_fn,
            _expr,
            max_execution_time_seconds,
            max_execution_time,
            target_completion_time,
            optimization_strategy != OptimizationStrategy.BALANCED,
            not error_on_failure,
            expectation_type,
        ]
    ):
        func = expectation
        # Create a default expectation for the function
        default_exp = SLAExpectation(
            name=f"{func.__name__}_sla",
            description=f"SLA requirement for {func.__name__}",
            optimization_strategy=OptimizationStrategy.BALANCED,
            error_on_failure=False,
        )
        return _attach_expectation(func, default_exp)

    # Handle decorator usage with existing expectation object: @expect(exp_obj)
    if isinstance(expectation, Expectation):

        def decorator(func: Callable) -> Callable:
            return _attach_expectation(func, expectation)

        return decorator

    # Handle string optimization_strategy
    if isinstance(optimization_strategy, str):
        try:
            optimization_strategy = OptimizationStrategy(optimization_strategy.lower())
        except ValueError:
            raise ValueError(
                f"Invalid optimization_strategy: {optimization_strategy}. "
                f"Must be one of: {[s.value for s in OptimizationStrategy]}"
            )

    # Determine expectation type if not specified
    if expectation_type is None:
        if validator_fn is not None or _expr is not None:
            expectation_type = ExpectationType.DATA_QUALITY
        elif (
            max_execution_time_seconds is not None
            or max_execution_time is not None
            or target_completion_time is not None
        ):
            expectation_type = ExpectationType.SLA
        else:
            raise ValueError(
                "Must specify either validator_fn or expr (for data quality) "
                "or time constraints (for SLA)"
            )

    # Create expectation object
    if expectation_type == ExpectationType.DATA_QUALITY:
        if validator_fn is None:
            raise ValueError(
                "Either validator_fn or expr is required for data quality expectations"
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
        if name is None:
            name = "SLA Requirement"
        if description is None:
            description = "SLA performance requirement"
        exp = SLAExpectation(
            name=name,
            description=description,
            max_execution_time_seconds=max_execution_time_seconds,
            max_execution_time=max_execution_time,
            target_completion_time=target_completion_time,
            optimization_strategy=optimization_strategy,
            error_on_failure=error_on_failure,
        )

    # Return expectation object for direct function calls
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
    from ray.data.block import BlockAccessor
    import pyarrow as pa

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

    Handles different result types returned by eval_expr:
    - PyArrow Arrays/ChunkedArrays (https://arrow.apache.org/docs/python/)
    - Scalars
    - Other array-like objects

    This function ensures that:
    - Empty batches pass validation (no data to validate)
    - Null values in boolean arrays are handled correctly
    - All non-null values must be True for validation to pass

    Args:
        result: Result from eval_expr evaluation.

    Returns:
        True if all non-null values in result are True, False otherwise.
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

    This function converts an expression (e.g., col("value") > 0) into a
    validator function that can be used with DataQualityExpectation.

    The validator leverages Ray Data's existing expression evaluation infrastructure,
    using the same eval_expr function that powers filter(), with_column(), and other
    expression-based operations. This ensures consistent behavior and performance.

    Args:
        expr: A Ray Data expression that evaluates to boolean.

    Returns:
        A validator function that takes a batch and returns True if all rows
        in the batch satisfy the expression.
    """

    def validator_fn(batch: Any) -> bool:
        """Validate that all rows in batch satisfy the expression.

        Args:
            batch: Batch in any supported format (dict, pandas DataFrame,
                PyArrow Table, etc.). Can be empty.

        Returns:
            True if all rows satisfy the expression, False otherwise.
            Empty batches return True (no data to validate).
        """
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


def _attach_expectation(func: Callable, expectation: Expectation) -> Callable:
    """Attach an expectation to a function.

    This is an internal helper function used by the ``expect`` decorator
    to attach expectation objects to function objects.

    Args:
        func: The function to attach the expectation to.
        expectation: The expectation object to attach.

    Returns:
        The function with the expectation attached.
    """
    if not hasattr(func, "__ray_data_expectations__"):
        func.__ray_data_expectations__ = []
    func.__ray_data_expectations__.append(expectation)
    return func


@DeveloperAPI
def get_expectations_from_function(func: Callable) -> List[Expectation]:
    """Extract expectations attached to a function.

    This is a helper function used internally by Ray Data to extract
    expectations that were attached to functions via the ``@expect`` decorator.

    Args:
        func: The function to extract expectations from.

    Returns:
        List of expectations attached to the function, or empty list if none.
    """
    if hasattr(func, "__ray_data_expectations__"):
        return func.__ray_data_expectations__
    return []


@DeveloperAPI
def get_sla_expectations_from_function(func: Callable) -> List[SLAExpectation]:
    """Extract SLA expectations attached to a function.

    This is a helper function used internally by Ray Data to extract
    SLA expectations that were attached to functions via the ``@expect`` decorator.

    Args:
        func: The function to extract SLA expectations from.

    Returns:
        List of SLA expectations attached to the function, or empty list if none.
    """
    all_expectations = get_expectations_from_function(func)
    return [exp for exp in all_expectations if isinstance(exp, SLAExpectation)]


@DeveloperAPI
class ExpectationSuite:
    """A collection of expectations that can be applied together.

    Expectation suites allow you to organize multiple expectations into a
    reusable group. This is useful for validating datasets against multiple
    quality criteria at once.

    Examples:
        >>> from ray.data.expectations import ExpectationSuite, expect_column_values_to_be_between
        >>> from ray.data.expressions import col
        >>>
        >>> # Create suite with expectations in constructor (most Pythonic)
        >>> suite = ExpectationSuite("user_data_quality", [
        ...     expect_column_values_to_be_between("age", 0, 120),
        ...     expect(expr=col("email").is_not_null())
        ... ])
        >>>
        >>> # Or create empty and add expectations (list-like)
        >>> suite = ExpectationSuite("user_data_quality")
        >>> suite.append(expect_column_values_to_be_between("age", 0, 120))
        >>> suite.append(expect(expr=col("email").is_not_null()))
        >>>
        >>> # Or use list operations
        >>> expectations = [
        ...     expect_column_values_to_be_between("age", 0, 120),
        ...     expect(expr=col("email").is_not_null())
        ... ]
        >>> suite = ExpectationSuite("user_data_quality", expectations)
        >>>
        >>> # Apply suite to dataset
        >>> ds = ray.data.from_items([{"age": 25, "email": "user@example.com"}])
        >>> passed_ds, failed_ds, results = ds.expect(suite)

    Attributes:
        name: Name of this expectation suite.
        description: Description of what this suite validates.
    """

    def __init__(
        self,
        name: str,
        expectations: Optional[Union[List[Expectation], Expectation]] = None,
        *,
        description: Optional[str] = None,
    ):
        """Initialize an expectation suite.

        Args:
            name: Name for this expectation suite.
            expectations: Optional list of expectations or single expectation to add.
                Can also be added later using list-like methods (append, extend).
            description: Optional description of what this suite validates.
        """
        if not name:
            raise ValueError("ExpectationSuite name cannot be empty")
        self.name = name
        self.description = description or f"Expectation suite: {name}"
        self.expectations: List[Expectation] = []

        # Add initial expectations if provided
        if expectations is not None:
            if isinstance(expectations, Expectation):
                # Single expectation
                self.append(expectations)
            elif isinstance(expectations, (list, tuple)):
                # List of expectations
                self.extend(expectations)
            else:
                raise TypeError(
                    f"expectations must be an Expectation or list of Expectations, "
                    f"got {type(expectations).__name__}"
                )

    def append(self, expectation: Expectation) -> None:
        """Add an expectation to this suite.

        Args:
            expectation: The expectation to add.

        Raises:
            TypeError: If expectation is not an Expectation instance.
        """
        if not isinstance(expectation, Expectation):
            raise TypeError(f"Expected Expectation, got {type(expectation).__name__}")
        self.expectations.append(expectation)

    def extend(
        self, expectations: Union[List[Expectation], "ExpectationSuite"]
    ) -> None:
        """Add multiple expectations to this suite.

        Args:
            expectations: List of expectations or another ExpectationSuite to add.

        Raises:
            TypeError: If any expectation is not an Expectation instance.
        """
        if isinstance(expectations, ExpectationSuite):
            expectations = expectations.expectations
        for exp in expectations:
            self.append(exp)

    def __len__(self) -> int:
        """Return the number of expectations in this suite."""
        return len(self.expectations)

    def __getitem__(self, index: int) -> Expectation:
        """Get expectation by index."""
        return self.expectations[index]

    def __iter__(self):
        """Iterate over expectations."""
        return iter(self.expectations)

    def __repr__(self) -> str:
        """Return string representation of this suite."""
        return (
            f"ExpectationSuite(name='{self.name}', "
            f"expectations={len(self.expectations)})"
        )


# ============================================================================
# Helper Functions for Common Expectation Patterns
# ============================================================================
# These functions provide a more discoverable API for common validation patterns,
# similar to Great Expectations' approach while maintaining Ray Data's expression-based
# flexibility.


@DeveloperAPI
def expect_column_values_to_be_between(
    column: str,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    error_on_failure: bool = True,
) -> DataQualityExpectation:
    """Create an expectation that column values are between min and max.

    This is a convenience function for a common validation pattern. It creates
    an expression-based expectation that validates all values in a column fall
    within the specified range.

    Examples:
        >>> from ray.data.expectations import expect_column_values_to_be_between
        >>> from ray.data.expressions import col
        >>>
        >>> # Validate age is between 0 and 120
        >>> expectation = expect_column_values_to_be_between(
        ...     "age", min_value=0, max_value=120
        ... )
        >>> ds = ray.data.from_items([{"age": 25}, {"age": 150}])
        >>> passed_ds, failed_ds, result = ds.expect(expectation)
        >>> print(result.passed)
        False
        >>> print(failed_ds.take_all())
        [{'age': 150}]

    Args:
        column: Name of the column to validate.
        min_value: Minimum allowed value (inclusive). If None, no minimum check.
        max_value: Maximum allowed value (inclusive). If None, no maximum check.
        name: Optional name for this expectation. Defaults to auto-generated name.
        description: Optional description. Defaults to auto-generated description.
        error_on_failure: If True, raise exception on failure; if False, log warning.

    Returns:
        A DataQualityExpectation that validates the column range.

    Raises:
        ValueError: If both min_value and max_value are None.
    """
    from ray.data.expressions import col, lit

    if min_value is None and max_value is None:
        raise ValueError("At least one of min_value or max_value must be specified")

    # Build expression for range validation
    expr = None
    if min_value is not None and max_value is not None:
        expr = (col(column) >= lit(min_value)) & (col(column) <= lit(max_value))
        desc = f"Column '{column}' values between {min_value} and {max_value}"
    elif min_value is not None:
        expr = col(column) >= lit(min_value)
        desc = f"Column '{column}' values >= {min_value}"
    else:  # max_value is not None
        expr = col(column) <= lit(max_value)
        desc = f"Column '{column}' values <= {max_value}"

    if name is None:
        name = f"{column}_values_between_{min_value}_{max_value}"

    if description is None:
        description = desc

    return expect(
        name=name,
        description=description,
        expr=expr,
        error_on_failure=error_on_failure,
    )


@DeveloperAPI
def expect_column_values_to_not_be_null(
    column: str,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    error_on_failure: bool = True,
) -> DataQualityExpectation:
    """Create an expectation that column values are not null.

    Examples:
        >>> from ray.data.expectations import expect_column_values_to_not_be_null
        >>>
        >>> expectation = expect_column_values_to_not_be_null("email")
        >>> ds = ray.data.from_items([
        ...     {"email": "user@example.com"},
        ...     {"email": None}
        ... ])
        >>> passed_ds, failed_ds, result = ds.expect(expectation)

    Args:
        column: Name of the column to validate.
        name: Optional name for this expectation.
        description: Optional description.
        error_on_failure: If True, raise exception on failure; if False, log warning.

    Returns:
        A DataQualityExpectation that validates non-null values.
    """
    from ray.data.expressions import col

    if name is None:
        name = f"{column}_not_null"

    if description is None:
        description = f"Column '{column}' values must not be null"

    return expect(
        name=name,
        description=description,
        expr=col(column).is_not_null(),
        error_on_failure=error_on_failure,
    )


@DeveloperAPI
def expect_column_values_to_be_unique(
    column: str,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    error_on_failure: bool = True,
) -> DataQualityExpectation:
    """Create an expectation that column values are unique.

    Note: This validates uniqueness within each batch, not across the entire dataset.
    For full-dataset uniqueness, use Dataset operations like `unique()`.

    Examples:
        >>> from ray.data.expectations import expect_column_values_to_be_unique
        >>>
        >>> expectation = expect_column_values_to_be_unique("user_id")
        >>> ds = ray.data.from_items([
        ...     {"user_id": 1},
        ...     {"user_id": 2}
        ... ])
        >>> passed_ds, failed_ds, result = ds.expect(expectation)

    Args:
        column: Name of the column to validate.
        name: Optional name for this expectation.
        description: Optional description.
        error_on_failure: If True, raise exception on failure; if False, log warning.

    Returns:
        A DataQualityExpectation that validates uniqueness within batches.
    """

    def validator_fn(batch: Any) -> bool:
        """Validate uniqueness within a batch."""
        from ray.data.block import BlockAccessor

        try:
            accessor = BlockAccessor.for_block(batch)
            if accessor.num_rows() == 0:
                return True

            # Get column values
            block = accessor.to_arrow()
            column_values = block[column].to_pylist()

            # Check for duplicates (excluding None/null values)
            non_null_values = [v for v in column_values if v is not None]
            return len(non_null_values) == len(set(non_null_values))
        except Exception:
            return False

    if name is None:
        name = f"{column}_unique"

    if description is None:
        description = f"Column '{column}' values must be unique within each batch"

    return expect(
        name=name,
        description=description,
        validator_fn=validator_fn,
        error_on_failure=error_on_failure,
    )


@DeveloperAPI
def expect_column_values_to_be_in_set(
    column: str,
    value_set: Union[List[Any], set],
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    error_on_failure: bool = True,
) -> DataQualityExpectation:
    """Create an expectation that column values are in a set of allowed values.

    Examples:
        >>> from ray.data.expectations import expect_column_values_to_be_in_set
        >>>
        >>> expectation = expect_column_values_to_be_in_set(
        ...     "status", value_set=["active", "inactive", "pending"]
        ... )
        >>> ds = ray.data.from_items([{"status": "active"}, {"status": "invalid"}])
        >>> passed_ds, failed_ds, result = ds.expect(expectation)

    Args:
        column: Name of the column to validate.
        value_set: Set or list of allowed values.
        name: Optional name for this expectation.
        description: Optional description.
        error_on_failure: If True, raise exception on failure; if False, log warning.

    Returns:
        A DataQualityExpectation that validates values are in the set.
    """
    from ray.data.expressions import col

    if name is None:
        name = f"{column}_in_set"

    if description is None:
        desc_values = ", ".join(str(v) for v in list(value_set)[:5])
        if len(value_set) > 5:
            desc_values += f", ... ({len(value_set)} total)"
        description = f"Column '{column}' values must be in set: {desc_values}"

    # Convert set to list for expression API
    value_list = list(value_set)
    return expect(
        name=name,
        description=description,
        expr=col(column).is_in(value_list),
        error_on_failure=error_on_failure,
    )


@DeveloperAPI
def expect_table_row_count_to_be_between(
    min_count: Optional[int] = None,
    max_count: Optional[int] = None,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    error_on_failure: bool = True,
) -> DataQualityExpectation:
    """Create an expectation that table row count is between min and max.

    Note: This validates row count within each batch, not the entire dataset.
    For full-dataset row count validation, use Dataset.count().

    Examples:
        >>> from ray.data.expectations import expect_table_row_count_to_be_between
        >>>
        >>> expectation = expect_table_row_count_to_be_between(
        ...     min_count=10, max_count=1000
        ... )
        >>> ds = ray.data.from_items([{"value": i} for i in range(100)])
        >>> passed_ds, failed_ds, result = ds.expect(expectation)

    Args:
        min_count: Minimum allowed row count (inclusive). If None, no minimum check.
        max_count: Maximum allowed row count (inclusive). If None, no maximum check.
        name: Optional name for this expectation.
        description: Optional description.
        error_on_failure: If True, raise exception on failure; if False, log warning.

    Returns:
        A DataQualityExpectation that validates row count within batches.

    Raises:
        ValueError: If both min_count and max_count are None.
    """
    if min_count is None and max_count is None:
        raise ValueError("At least one of min_count or max_count must be specified")

    def validator_fn(batch: Any) -> bool:
        """Validate row count within a batch."""
        from ray.data.block import BlockAccessor

        try:
            accessor = BlockAccessor.for_block(batch)
            row_count = accessor.num_rows()

            if min_count is not None and row_count < min_count:
                return False
            if max_count is not None and row_count > max_count:
                return False
            return True
        except Exception:
            return False

    if name is None:
        name = f"row_count_between_{min_count}_{max_count}"

    if description is None:
        if min_count is not None and max_count is not None:
            desc = f"Row count between {min_count} and {max_count}"
        elif min_count is not None:
            desc = f"Row count >= {min_count}"
        else:
            desc = f"Row count <= {max_count}"
        description = desc

    return expect(
        name=name,
        description=description,
        validator_fn=validator_fn,
        error_on_failure=error_on_failure,
    )
