"""Tests for Ray Data Expectations API."""

import datetime
import pickle

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext
from ray.data.expectations import (
    DataQualityExpectation,
    ExecutionTimeExpectation,
    Expectation,
    ExpectationResult,
    ExpectationType,
    expect,
)
from ray.data.expressions import col
from ray.tests.conftest import *  # noqa


def test_expectation_creation():
    """Test creating basic expectation objects."""
    exp = Expectation(
        name="test",
        description="Test expectation",
        expectation_type=ExpectationType.DATA_QUALITY,
    )
    assert exp.name == "test"
    assert exp.description == "Test expectation"
    assert exp.expectation_type == ExpectationType.DATA_QUALITY
    assert exp.error_on_failure is True


def test_expectation_validation_raises():
    """Test that base expectation.validate() raises NotImplementedError."""
    exp = Expectation(
        name="test",
        description="Test",
        expectation_type=ExpectationType.DATA_QUALITY,
    )
    with pytest.raises(NotImplementedError):
        exp.validate(None)


def test_expectation_empty_name():
    """Test that empty name raises ValueError."""
    with pytest.raises(ValueError, match="name cannot be empty"):
        Expectation(
            name="",
            description="Test",
            expectation_type=ExpectationType.DATA_QUALITY,
        )


def test_expectation_empty_description():
    """Test that empty description raises ValueError."""
    with pytest.raises(ValueError, match="description cannot be empty"):
        Expectation(
            name="test",
            description="",
            expectation_type=ExpectationType.DATA_QUALITY,
        )


def test_data_quality_creation():
    """Test creating data quality expectations."""

    def validator(batch):
        return batch["value"].min() > 0

    exp = DataQualityExpectation(
        name="positive_values",
        description="Values must be positive",
        validator_fn=validator,
    )

    assert exp.name == "positive_values"
    assert exp.expectation_type == ExpectationType.DATA_QUALITY
    assert exp.validator_fn == validator


def test_data_quality_validation_passes():
    """Test data quality validation when it passes."""

    def validator(batch):
        return batch["value"].min() > 0

    exp = DataQualityExpectation(
        name="positive_values",
        description="Values must be positive",
        validator_fn=validator,
    )

    batch = {"value": np.array([1, 2, 3, 4])}
    assert exp.validate(batch) is True


def test_data_quality_validation_fails():
    """Test data quality validation when it fails."""

    def validator(batch):
        return batch["value"].min() > 0

    exp = DataQualityExpectation(
        name="positive_values",
        description="Values must be positive",
        validator_fn=validator,
    )

    batch = {"value": np.array([-1, 2, 3, 4])}
    assert exp.validate(batch) is False


def test_data_quality_with_pandas():
    """Test data quality validation with pandas DataFrames."""

    def validator(batch):
        return batch["value"].min() > 0

    exp = DataQualityExpectation(
        name="positive_values",
        description="Values must be positive",
        validator_fn=validator,
    )

    batch = pd.DataFrame({"value": [1, 2, 3, 4]})
    assert exp.validate(batch) is True


def test_execution_time_creation_with_seconds():
    """Test creating execution time expectations with seconds."""
    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish in 60 seconds",
        max_execution_time_seconds=60.0,
    )

    assert exp.name == "fast_job"
    assert exp.max_execution_time_seconds == 60.0


def test_execution_time_creation_with_timedelta():
    """Test creating execution time expectations with timedelta."""
    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish in 60 seconds",
        max_execution_time=datetime.timedelta(seconds=60),
    )
    assert exp.max_execution_time_seconds == 60.0


def test_execution_time_creation_with_target_time():
    """Test creating execution time expectations with target completion time."""
    target = datetime.datetime.now() + datetime.timedelta(seconds=60)

    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish by target time",
        target_completion_time=target,
    )

    assert exp.max_execution_time_seconds is not None
    assert exp.max_execution_time_seconds > 0


def test_execution_time_validation_passes():
    """Test execution time validation when it passes."""
    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish in 60 seconds",
        max_execution_time_seconds=60.0,
    )

    assert exp.validate(30.0) is True


def test_execution_time_validation_fails():
    """Test execution time validation when it fails."""
    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish in 60 seconds",
        max_execution_time_seconds=60.0,
    )
    assert exp.validate(90.0) is False


def test_execution_time_no_time_constraints_error():
    """Test that execution time expectation without time constraints raises error."""
    with pytest.raises(ValueError, match="at least one time constraint"):
        ExecutionTimeExpectation(
            name="fast_job",
            description="Job must finish quickly",
        )


def test_expect_with_expression(ray_start_regular_shared):
    """Test dataset.expect() with expression."""
    ds = ray.data.from_items([{"value": 1}, {"value": -1}])
    passed_ds, failed_ds, result = ds.expect(expr=col("value") > 0)

    assert result.passed is False
    assert failed_ds.count() == 1


def test_expect_complex_expression(ray_start_regular_shared):
    """Test expression with AND/OR conditions."""
    ds = ray.data.from_items([{"value": 50}, {"value": 150}])
    passed_ds, failed_ds, result = ds.expect(
        expr=(col("value") >= 0) & (col("value") <= 100)
    )
    assert result.passed is False
    assert failed_ds.count() == 1


def test_expect_expression_with_null_handling(ray_start_regular_shared):
    """Test expression with null value handling."""
    ds = ray.data.from_items([{"value": 1}, {"value": None}, {"value": 3}])

    passed_ds, failed_ds, result = ds.expect(expr=col("value").is_not_null())
    assert result.passed is False


def test_expect_cannot_specify_both_validator_fn_and_expr():
    """Test that specifying both validator_fn and expr raises error."""
    with pytest.raises(ValueError, match="Cannot specify both"):
        expect(
            validator_fn=lambda batch: True,
            expr=col("value") > 0,
        )


def test_dataset_expect_basic(ray_start_regular_shared):
    """Test basic dataset.expect() usage."""
    ds = ray.data.range(10)

    def validator(batch):
        return batch["id"] >= 0

    exp = DataQualityExpectation(
        name="non_negative",
        description="IDs must be non-negative",
        validator_fn=validator,
    )

    passed_ds, failed_ds, result = ds.expect(exp)

    assert result.passed is True
    assert passed_ds.count() == 10
    assert failed_ds.count() == 0


def test_dataset_expect_validation_fails(ray_start_regular_shared):
    """Test dataset.expect() when validation fails."""
    ds = ray.data.from_items([{"value": -1}, {"value": 2}])

    def validator(batch):
        return batch["value"] > 0

    exp = DataQualityExpectation(
        name="positive",
        description="Values must be positive",
        validator_fn=validator,
        error_on_failure=False,
    )

    passed_ds, failed_ds, result = ds.expect(exp)

    assert result.passed is False
    assert failed_ds.count() > 0


def test_dataset_expect_with_expression(ray_start_regular_shared):
    """Test dataset.expect() with expression."""
    ds = ray.data.range(10)

    passed_ds, failed_ds, result = ds.expect(expr=col("id") >= 0)

    assert result.passed is True
    assert passed_ds.count() == 10
    assert failed_ds.count() == 0


def test_dataset_expect_execution_time_expectation(ray_start_regular_shared):
    """Test dataset.expect() with execution time expectation."""
    ds = ray.data.range(10)

    execution_time_exp = ExecutionTimeExpectation(
        name="fast",
        description="Must be fast",
        max_execution_time_seconds=60.0,
    )

    passed_ds, failed_ds, result = ds.expect(execution_time_exp)
    assert isinstance(result, ExpectationResult)


def test_dataset_expect_data_unchanged(ray_start_regular_shared):
    """Test that dataset.expect() doesn't modify data."""
    original_data = [{"value": i} for i in range(10)]
    ds = ray.data.from_items(original_data)

    def validator(batch):
        return batch["value"] >= 0

    exp = DataQualityExpectation(
        name="non_negative",
        description="Values must be non-negative",
        validator_fn=validator,
    )

    passed_ds, failed_ds, result = ds.expect(exp)

    assert passed_ds.take_all() == original_data


def test_execution_plan_add_execution_time_expectation():
    """Test adding execution time expectations to execution plan."""
    context = DataContext()
    stats = DatasetStats(metadata={}, parent=None)
    plan = ExecutionPlan(stats, run_by_consumer=False, dataset_context=context)

    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish in 60 seconds",
        max_execution_time_seconds=60.0,
    )

    plan.add_execution_time_expectation(exp)

    assert plan.get_max_execution_time_seconds() == 60.0
    assert len(plan.get_execution_time_expectations()) == 1


def test_execution_plan_multiple_execution_time_expectations():
    """Test multiple execution time expectations with minimum time."""
    context = DataContext()
    stats = DatasetStats(metadata={}, parent=None)
    plan = ExecutionPlan(stats, run_by_consumer=False, dataset_context=context)

    plan.add_execution_time_expectation(
        ExecutionTimeExpectation(
            name="job1",
            description="Job 1",
            max_execution_time_seconds=120.0,
        )
    )

    plan.add_execution_time_expectation(
        ExecutionTimeExpectation(
            name="job2",
            description="Job 2",
            max_execution_time_seconds=60.0,
        )
    )

    assert plan.get_max_execution_time_seconds() == 60.0
    assert len(plan.get_execution_time_expectations()) == 2


def test_execution_plan_copy_preserves_execution_time_expectations():
    """Test that copying execution plan preserves execution time expectations."""
    context = DataContext()
    stats = DatasetStats(metadata={}, parent=None)
    plan = ExecutionPlan(stats, run_by_consumer=False, dataset_context=context)

    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish in 60 seconds",
        max_execution_time_seconds=60.0,
    )
    plan.add_execution_time_expectation(exp)

    copied_plan = plan.copy()

    assert copied_plan.get_max_execution_time_seconds() == 60.0


def test_expectations_persist_through_filter(ray_start_regular_shared):
    """Test execution time expectations persist through filter operation."""
    exp = expect(max_execution_time_seconds=60.0)
    ds = ray.data.range(100)
    ds._plan.add_execution_time_expectation(exp)
    filtered_ds = ds.filter(lambda row: row["id"] % 2 == 0)

    assert filtered_ds._plan.get_max_execution_time_seconds() == 60.0


def test_expectations_persist_through_union(ray_start_regular_shared):
    """Test execution time expectations persist through union operation."""
    exp = expect(max_execution_time_seconds=60.0)
    ds1 = ray.data.range(50)
    ds1._plan.add_execution_time_expectation(exp)
    ds2 = ray.data.range(50, 100)

    unioned_ds = ds1.union(ds2)

    assert unioned_ds._plan.get_max_execution_time_seconds() == 60.0


def test_expectations_persist_through_groupby(ray_start_regular_shared):
    """Test execution time expectations persist through groupby operation."""
    exp = expect(max_execution_time_seconds=60.0)
    ds = ray.data.range(100)
    ds._plan.add_execution_time_expectation(exp)
    grouped_ds = ds.groupby("id").count()

    assert grouped_ds._plan.get_max_execution_time_seconds() == 60.0


def test_expectations_persist_through_repartition(ray_start_regular_shared):
    """Test execution time expectations persist through repartition."""
    exp = expect(max_execution_time_seconds=60.0)
    ds = ray.data.range(100)
    ds._plan.add_execution_time_expectation(exp)
    repartitioned_ds = ds.repartition(5)

    assert repartitioned_ds._plan.get_max_execution_time_seconds() == 60.0


def test_expectations_persist_through_select_columns(ray_start_regular_shared):
    """Test execution time expectations persist through select_columns."""
    exp = expect(max_execution_time_seconds=60.0)
    ds = ray.data.range(100)
    ds._plan.add_execution_time_expectation(exp)
    selected_ds = ds.select_columns(cols=["id"])

    assert selected_ds._plan.get_max_execution_time_seconds() == 60.0


def test_expect_with_empty_dataset(ray_start_regular_shared):
    """Test expectations with empty dataset."""
    ds = ray.data.range(0)

    def validator(batch):
        return True

    exp = DataQualityExpectation(
        name="always_true",
        description="Always passes",
        validator_fn=validator,
    )

    passed_ds, failed_ds, result = ds.expect(exp)

    assert result.passed is True
    assert passed_ds.count() == 0
    assert failed_ds.count() == 0


def test_expression_with_all_nulls(ray_start_regular_shared):
    """Test expression validation with all null values."""
    ds = ray.data.from_items([{"value": None}, {"value": None}])

    passed_ds, failed_ds, result = ds.expect(
        expr=col("value").is_null(), error_on_failure=False
    )

    assert result.passed is True


def test_validator_function_raises_exception(ray_start_regular_shared):
    """Test that validator exceptions are handled."""

    def bad_validator(batch):
        raise RuntimeError("Validation error")

    ds = ray.data.range(10)
    exp = DataQualityExpectation(
        name="bad_validator",
        description="Validator that raises",
        validator_fn=bad_validator,
        error_on_failure=False,
    )

    passed_ds, failed_ds, result = ds.expect(exp)
    assert result.passed is False


def test_execution_time_with_zero_time():
    """Test execution time expectation with zero execution time."""
    with pytest.raises(ValueError, match="must be positive"):
        ExecutionTimeExpectation(
            name="instant_job",
            description="Job must finish instantly",
            max_execution_time_seconds=0.0,
        )


def test_execution_time_with_negative_time():
    """Test execution time expectation with negative execution time."""
    with pytest.raises(ValueError, match="must be positive"):
        ExecutionTimeExpectation(
            name="time_travel_job",
            description="Job finished yesterday",
            max_execution_time_seconds=-10.0,
        )


def test_expectation_serialization():
    """Test that expectations can be pickled for distributed execution."""
    exp = DataQualityExpectation(
        name="positive_values",
        description="Values must be positive",
        validator_fn=lambda batch: batch["value"] > 0,
    )

    pickled = pickle.dumps(exp)
    unpickled = pickle.loads(pickled)

    assert unpickled.name == exp.name
    assert unpickled.description == exp.description


def test_execution_time_expectation_serialization():
    """Test that execution time expectations can be pickled."""
    exp = ExecutionTimeExpectation(
        name="fast_job",
        description="Job must finish in 60 seconds",
        max_execution_time_seconds=60.0,
    )

    pickled = pickle.dumps(exp)
    unpickled = pickle.loads(pickled)

    assert unpickled.name == exp.name
    assert unpickled.max_execution_time_seconds == 60.0


def test_expectation_in_pipeline(ray_start_regular_shared):
    """Test expectations in multi-stage pipeline."""
    ds = ray.data.range(100)

    passed_ds, failed_ds, result1 = ds.expect(expr=col("id") >= 0)
    assert result1.passed is True

    processed_ds, remaining_ds, result2 = passed_ds.expect(
        max_execution_time_seconds=60.0
    )
    assert isinstance(result2, ExpectationResult)

    assert processed_ds.count() == 100


def test_expectation_chaining(ray_start_regular_shared):
    """Test chaining multiple dataset.expect() calls."""
    ds = ray.data.from_items([{"value": i} for i in range(1, 11)])

    passed_ds1, failed_ds1, result1 = ds.expect(expr=col("value") > 0)
    passed_ds2, failed_ds2, result2 = passed_ds1.expect(expr=col("value") < 100)

    assert result1.passed is True
    assert result2.passed is True
    assert passed_ds2.count() == 10


def test_both_execution_time_and_data_quality_expectations(ray_start_regular_shared):
    """Test using both execution time and data quality expectations together."""
    ds = ray.data.from_items([{"value": i} for i in range(1, 11)])

    passed_ds, failed_ds, dq_result = ds.expect(expr=col("value") > 0)
    assert dq_result.passed is True

    processed_ds, remaining_ds, execution_time_result = passed_ds.expect(
        max_execution_time_seconds=60.0
    )
    assert isinstance(execution_time_result, ExpectationResult)


def test_expectation_with_large_dataset(ray_start_regular_shared):
    """Test expectations work with larger datasets."""
    ds = ray.data.range(10000)

    passed_ds, failed_ds, result = ds.expect(expr=col("id") >= 0)

    assert result.passed is True
    assert passed_ds.count() == 10000


def test_expectation_result_with_all_fields():
    """Test ExpectationResult with all fields."""
    result = ExpectationResult(
        expectation_name="test_expectation",
        passed=True,
        total_rows_validated=1000,
        failed_rows=0,
        execution_time_seconds=5.2,
        message="All checks passed",
    )

    assert result.passed is True
    assert result.total_rows_validated == 1000
    assert result.failed_rows == 0
    assert result.execution_time_seconds == 5.2
    assert "test_expectation" in result.message


def test_expectations_import():
    """Test importing expectations from ray.data."""
    from ray.data import (
        DataQualityExpectation,
        ExecutionTimeExpectation,
        Expectation,
        ExpectationResult,
        ExpectationType,
        expect,
    )

    assert Expectation is not None
    assert DataQualityExpectation is not None
    assert ExecutionTimeExpectation is not None
    assert ExpectationResult is not None
    assert ExpectationType is not None
    assert expect is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
