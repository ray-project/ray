"""
Comprehensive tests for Ray Data Expectations API.

Tests cover:
1. Core expectation classes (Expectation, DataQualityExpectation, SLAExpectation)
2. Decorator patterns (@expect)
3. Expression-based data quality
4. Dataset.expect() method
5. SLA integration and optimization strategies
6. Dataset operation propagation
7. Edge cases and error handling
"""

import datetime
import pickle
import time

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.expressions import col
from ray.data.expectations import (
    DataQualityExpectation,
    Expectation,
    ExpectationResult,
    ExpectationType,
    OptimizationStrategy,
    SLAExpectation,
    expect,
    get_expectations_from_function,
    get_sla_expectations_from_function,
)
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext

from ray.tests.conftest import *  # noqa


# =============================================================================
# Core Expectation Classes
# =============================================================================


class TestExpectationBase:
    """Test base Expectation class."""

    def test_expectation_creation(self):
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

    def test_expectation_validation_raises(self):
        """Test that base expectation.validate() raises NotImplementedError."""
        exp = Expectation(
            name="test",
            description="Test",
            expectation_type=ExpectationType.DATA_QUALITY,
        )
        with pytest.raises(NotImplementedError):
            exp.validate(None)

    def test_expectation_empty_name(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="name cannot be empty"):
            Expectation(
                name="",
                description="Test",
                expectation_type=ExpectationType.DATA_QUALITY,
            )

    def test_expectation_empty_description(self):
        """Test that empty description raises ValueError."""
        with pytest.raises(ValueError, match="description cannot be empty"):
            Expectation(
                name="test",
                description="",
                expectation_type=ExpectationType.DATA_QUALITY,
            )


class TestDataQualityExpectation:
    """Test DataQualityExpectation class."""

    def test_data_quality_creation(self):
        """Test creating data quality expectations."""
        validator = lambda batch: batch["value"].min() > 0

        exp = DataQualityExpectation(
            name="positive_values",
            description="Values must be positive",
            validator_fn=validator,
        )

        assert exp.name == "positive_values"
        assert exp.expectation_type == ExpectationType.DATA_QUALITY
        assert exp.validator_fn == validator

    def test_data_quality_validation_passes(self):
        """Test data quality validation when it passes."""
        validator = lambda batch: batch["value"].min() > 0

        exp = DataQualityExpectation(
            name="positive_values",
            description="Values must be positive",
            validator_fn=validator,
        )

        batch = {"value": np.array([1, 2, 3, 4])}
        assert exp.validate(batch) is True

    def test_data_quality_validation_fails(self):
        """Test data quality validation when it fails."""
        validator = lambda batch: batch["value"].min() > 0

        exp = DataQualityExpectation(
            name="positive_values",
            description="Values must be positive",
            validator_fn=validator,
        )

        batch = {"value": np.array([-1, 2, 3, 4])}
        assert exp.validate(batch) is False

    def test_data_quality_with_pandas(self):
        """Test data quality validation with pandas DataFrames."""
        validator = lambda batch: batch["value"].min() > 0

        exp = DataQualityExpectation(
            name="positive_values",
            description="Values must be positive",
            validator_fn=validator,
        )

        batch = pd.DataFrame({"value": [1, 2, 3, 4]})
        assert exp.validate(batch) is True

    def test_data_quality_error_on_failure_false(self):
        """Test error_on_failure=False returns False instead of raising."""
        validator = lambda batch: batch["value"].min() > 0

        exp = DataQualityExpectation(
            name="positive_values",
            description="Values must be positive",
            validator_fn=validator,
            error_on_failure=False,
        )

        batch = {"value": np.array([-1, 2, 3, 4])}
        # Should return False instead of raising
        assert exp.validate(batch) is False


class TestSLAExpectation:
    """Test SLAExpectation class."""

    def test_sla_creation_with_seconds(self):
        """Test creating SLA expectations with seconds."""
        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish in 60 seconds",
            max_execution_time_seconds=60.0,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )

        assert exp.name == "fast_job"
        assert exp.max_execution_time_seconds == 60.0
        assert exp.optimization_strategy == OptimizationStrategy.PERFORMANCE

    def test_sla_creation_with_timedelta(self):
        """Test creating SLA expectations with timedelta."""
        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish in 60 seconds",
            max_execution_time=datetime.timedelta(seconds=60),
        )
        assert exp.max_execution_time_seconds == 60.0

    def test_sla_creation_with_target_time(self):
        """Test creating SLA expectations with target completion time."""
        target = datetime.datetime.now() + datetime.timedelta(seconds=60)

        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish by target time",
            target_completion_time=target,
        )

        # Should compute max_execution_time_seconds from target
        assert exp.max_execution_time_seconds is not None
        assert exp.max_execution_time_seconds > 0

    def test_sla_validation_passes(self):
        """Test SLA validation when it passes."""
        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish in 60 seconds",
            max_execution_time_seconds=60.0,
        )

        assert exp.validate(30.0) is True

    def test_sla_validation_fails(self):
        """Test SLA validation when it fails."""
        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish in 60 seconds",
            max_execution_time_seconds=60.0,
        )
        assert exp.validate(90.0) is False

    def test_sla_no_time_constraints_error(self):
        """Test that SLA without time constraints raises error."""
        with pytest.raises(ValueError, match="at least one time constraint"):
            SLAExpectation(
                name="fast_job",
                description="Job must finish quickly",
            )


# =============================================================================
# Decorator Patterns
# =============================================================================


class TestExpectDecorator:
    """Test @expect decorator."""

    def test_expect_decorator_without_parentheses(self):
        """Test @expect without parentheses."""

        @expect
        def process(batch):
            return batch

        exps = get_expectations_from_function(process)
        assert len(exps) == 1
        assert exps[0].name == "process"

    def test_expect_decorator_with_sla_params(self):
        """Test @expect with SLA parameters."""

        @expect(max_execution_time_seconds=60.0)
        def process(batch):
            return batch

        exps = get_sla_expectations_from_function(process)
        assert len(exps) == 1
        assert exps[0].max_execution_time_seconds == 60.0

    def test_expect_decorator_with_data_quality(self):
        """Test @expect with data quality validator."""

        @expect(
            name="positive_check",
            description="Check positive values",
            validator_fn=lambda batch: batch["value"].min() > 0,
        )
        def process(batch):
            return batch

        exps = get_expectations_from_function(process)
        assert len(exps) == 1
        assert exps[0].expectation_type == ExpectationType.DATA_QUALITY

    def test_expect_decorator_with_existing_expectation(self):
        """Test @expect with existing expectation object."""
        exp = DataQualityExpectation(
            name="positive_values",
            description="Values must be positive",
            validator_fn=lambda batch: batch["value"].min() > 0,
        )

        @expect(exp)
        def process(batch):
            return batch

        exps = get_expectations_from_function(process)
        assert len(exps) == 1
        assert exps[0] is exp

    def test_expect_multiple_expectations(self):
        """Test multiple @expect decorators on same function."""

        @expect(max_execution_time_seconds=60.0)
        @expect(
            validator_fn=lambda batch: batch["value"].min() > 0,
        )
        def process(batch):
            return batch

        all_exps = get_expectations_from_function(process)
        sla_exps = get_sla_expectations_from_function(process)

        assert len(all_exps) == 2
        assert len(sla_exps) == 1


# =============================================================================
# Expression-Based Data Quality
# =============================================================================


class TestExpressionsIntegration:
    """Test expression-based data quality."""

    def test_expect_decorator_with_expression(self, ray_start_regular_shared):
        """Test @expect with expression."""

        @expect(expr=col("value") > 0)
        def process(batch):
            return batch

        ds = ray.data.range(10).map_batches(process)
        exps = get_expectations_from_function(process)
        assert len(exps) == 1
        assert exps[0].expectation_type == ExpectationType.DATA_QUALITY

    def test_expect_direct_expression(self, ray_start_regular_shared):
        """Test @expect(expr) as positional argument."""

        @expect(col("value") > 0)
        def process(batch):
            return batch

        exps = get_expectations_from_function(process)
        assert len(exps) == 1

    def test_expect_complex_expression(self, ray_start_regular_shared):
        """Test expression with AND/OR conditions."""

        @expect(expr=(col("value") > 0) & (col("value") < 100))
        def process(batch):
            return batch

        ds = ray.data.range(50).map_batches(process)
        exps = get_expectations_from_function(process)
        assert len(exps) == 1

    def test_expect_expression_with_null_handling(self, ray_start_regular_shared):
        """Test expression with null value handling."""
        ds = ray.data.from_items([{"value": 1}, {"value": None}, {"value": 3}])

        # Expression should handle nulls gracefully
        validated_ds, result = ds.expect(expr=col("value").is_not_null())
        assert result.passed is False  # Some values are null

    def test_expect_cannot_specify_both_validator_fn_and_expr(self):
        """Test that specifying both validator_fn and expr raises error."""
        with pytest.raises(ValueError, match="Cannot specify both"):

            @expect(
                validator_fn=lambda batch: True,
                expr=col("value") > 0,
            )
            def process(batch):
                return batch


# =============================================================================
# Dataset.expect() Method
# =============================================================================


class TestDatasetExpect:
    """Test Dataset.expect() method."""

    def test_dataset_expect_basic(self, ray_start_regular_shared):
        """Test basic dataset.expect() usage."""
        ds = ray.data.range(10)

        validator = lambda batch: batch["id"] >= 0
        exp = DataQualityExpectation(
            name="non_negative",
            description="IDs must be non-negative",
            validator_fn=validator,
        )

        validated_ds, result = ds.expect(exp)

        assert result.passed is True
        assert validated_ds.count() == 10

    def test_dataset_expect_validation_fails(self, ray_start_regular_shared):
        """Test dataset.expect() when validation fails."""
        ds = ray.data.from_items([{"value": -1}, {"value": 2}])

        validator = lambda batch: batch["value"] > 0
        exp = DataQualityExpectation(
            name="positive",
            description="Values must be positive",
            validator_fn=validator,
            error_on_failure=False,
        )

        validated_ds, result = ds.expect(exp)

        assert result.passed is False

    def test_dataset_expect_with_expression(self, ray_start_regular_shared):
        """Test dataset.expect() with expression."""
        ds = ray.data.range(10)

        validated_ds, result = ds.expect(expr=col("id") >= 0)

        assert result.passed is True
        assert validated_ds.count() == 10

    def test_dataset_expect_wrong_type(self, ray_start_regular_shared):
        """Test that dataset.expect() rejects non-DataQuality expectations."""
        ds = ray.data.range(10)

        sla_exp = SLAExpectation(
            name="fast",
            description="Must be fast",
            max_execution_time_seconds=60.0,
        )

        with pytest.raises(TypeError, match="SLAExpectation"):
            ds.expect(sla_exp)

    def test_dataset_expect_data_unchanged(self, ray_start_regular_shared):
        """Test that dataset.expect() doesn't modify data."""
        original_data = [{"value": i} for i in range(10)]
        ds = ray.data.from_items(original_data)

        validator = lambda batch: batch["value"] >= 0
        exp = DataQualityExpectation(
            name="non_negative",
            description="Values must be non-negative",
            validator_fn=validator,
        )

        validated_ds, result = ds.expect(exp)

        # Data should be unchanged
        assert validated_ds.take_all() == original_data


# =============================================================================
# SLA Integration and Optimization Strategies
# =============================================================================


class TestSLAIntegration:
    """Test SLA expectation integration with execution plan."""

    def test_execution_plan_add_sla_expectation(self):
        """Test adding SLA expectations to execution plan."""
        context = DataContext()
        stats = DatasetStats(metadata={}, parent=None)
        plan = ExecutionPlan(stats, run_by_consumer=False, dataset_context=context)

        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish in 60 seconds",
            max_execution_time_seconds=60.0,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )

        plan.add_sla_expectation(exp)

        assert plan.get_optimization_strategy() == OptimizationStrategy.PERFORMANCE
        assert plan.get_max_execution_time_seconds() == 60.0

    def test_execution_plan_optimization_strategy_priority(self):
        """Test that PERFORMANCE strategy takes priority over COST."""
        context = DataContext()
        stats = DatasetStats(metadata={}, parent=None)
        plan = ExecutionPlan(stats, run_by_consumer=False, dataset_context=context)

        # Add COST first
        plan.add_sla_expectation(
            SLAExpectation(
                name="cost_job",
                description="Cost-optimized job",
                max_execution_time_seconds=120.0,
                optimization_strategy=OptimizationStrategy.COST,
            )
        )

        # Add PERFORMANCE second
        plan.add_sla_expectation(
            SLAExpectation(
                name="fast_job",
                description="Performance-optimized job",
                max_execution_time_seconds=60.0,
                optimization_strategy=OptimizationStrategy.PERFORMANCE,
            )
        )

        # PERFORMANCE should take priority
        assert plan.get_optimization_strategy() == OptimizationStrategy.PERFORMANCE
        # Minimum time should be 60.0
        assert plan.get_max_execution_time_seconds() == 60.0

    def test_sla_expectation_propagates_to_map_batches(self, ray_start_regular_shared):
        """Test that SLA expectations propagate through map_batches."""

        @expect(max_execution_time_seconds=60.0)
        def process(batch):
            return batch

        ds = ray.data.range(10)
        transformed_ds = ds.map_batches(process)

        # SLA should be attached to execution plan
        strategy = transformed_ds._plan.get_optimization_strategy()
        assert strategy == OptimizationStrategy.BALANCED

    def test_execution_plan_copy_preserves_sla_expectations(self):
        """Test that copying execution plan preserves SLA expectations."""
        context = DataContext()
        stats = DatasetStats(metadata={}, parent=None)
        plan = ExecutionPlan(stats, run_by_consumer=False, dataset_context=context)

        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish in 60 seconds",
            max_execution_time_seconds=60.0,
        )
        plan.add_sla_expectation(exp)

        copied_plan = plan.copy()

        assert copied_plan.get_max_execution_time_seconds() == 60.0


# =============================================================================
# Dataset Operation Propagation
# =============================================================================


class TestDatasetOperationPropagation:
    """Test that expectations propagate through dataset operations."""

    def test_expectations_persist_through_filter(self, ray_start_regular_shared):
        """Test expectations persist through filter operation."""

        @expect(max_execution_time_seconds=60.0)
        def process(batch):
            return batch

        ds = ray.data.range(100).map_batches(process)
        filtered_ds = ds.filter(lambda row: row["id"] % 2 == 0)

        # Verify execution time constraint persists
        assert filtered_ds._plan.get_max_execution_time_seconds() == 60.0

    def test_expectations_persist_through_union(self, ray_start_regular_shared):
        """Test expectations persist through union operation."""

        @expect(max_execution_time_seconds=60.0)
        def process(batch):
            return batch

        ds1 = ray.data.range(50).map_batches(process)
        ds2 = ray.data.range(50, 100)

        unioned_ds = ds1.union(ds2)

        # Verify execution time constraint persists
        assert unioned_ds._plan.get_max_execution_time_seconds() == 60.0

    def test_expectations_persist_through_groupby(self, ray_start_regular_shared):
        """Test expectations persist through groupby operation."""

        @expect(max_execution_time_seconds=60.0)
        def process(batch):
            batch["group"] = batch["id"] % 3
            return batch

        ds = ray.data.range(100).map_batches(process)
        grouped_ds = ds.groupby("group").count()

        # Verify execution time constraint persists
        assert grouped_ds._plan.get_max_execution_time_seconds() == 60.0

    def test_expectations_persist_through_repartition(self, ray_start_regular_shared):
        """Test expectations persist through repartition."""

        @expect(max_execution_time_seconds=60.0)
        def process(batch):
            return batch

        ds = ray.data.range(100).map_batches(process)
        repartitioned_ds = ds.repartition(5)

        assert repartitioned_ds._plan.get_max_execution_time_seconds() == 60.0

    def test_expectations_persist_through_select_columns(
        self, ray_start_regular_shared
    ):
        """Test expectations persist through select_columns."""

        @expect(max_execution_time_seconds=60.0)
        def process(batch):
            batch["new_col"] = batch["id"] * 2
            return batch

        ds = ray.data.range(100).map_batches(process)
        selected_ds = ds.select_columns(cols=["id"])

        assert selected_ds._plan.get_max_execution_time_seconds() == 60.0


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_expect_with_empty_dataset(self, ray_start_regular_shared):
        """Test expectations with empty dataset."""
        ds = ray.data.range(0)  # Empty dataset

        validator = lambda batch: True
        exp = DataQualityExpectation(
            name="always_true",
            description="Always passes",
            validator_fn=validator,
        )

        validated_ds, result = ds.expect(exp)

        # Empty dataset should pass validation
        assert result.passed is True
        assert validated_ds.count() == 0

    def test_expression_with_all_nulls(self, ray_start_regular_shared):
        """Test expression validation with all null values."""
        ds = ray.data.from_items([{"value": None}, {"value": None}])

        # Expression with nulls should not crash
        validated_ds, result = ds.expect(
            expr=col("value").is_null(), error_on_failure=False
        )

        # All values are null, so is_null() should pass
        assert result.passed is True

    def test_validator_function_raises_exception(self, ray_start_regular_shared):
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

        # Should handle exception gracefully
        validated_ds, result = ds.expect(exp)
        assert result.passed is False

    def test_sla_with_zero_time(self):
        """Test SLA with zero execution time."""
        with pytest.raises(ValueError, match="must be positive"):
            SLAExpectation(
                name="instant_job",
                description="Job must finish instantly",
                max_execution_time_seconds=0.0,
            )

    def test_sla_with_negative_time(self):
        """Test SLA with negative execution time."""
        with pytest.raises(ValueError, match="must be positive"):
            SLAExpectation(
                name="time_travel_job",
                description="Job finished yesterday",
                max_execution_time_seconds=-10.0,
            )

    def test_invalid_optimization_strategy_string(self):
        """Test that invalid optimization strategy string raises error."""
        with pytest.raises(ValueError, match="Invalid optimization strategy"):
            SLAExpectation(
                name="test",
                description="Test",
                max_execution_time_seconds=60.0,
                optimization_strategy="INVALID",
            )

    def test_expectation_serialization(self):
        """Test that expectations can be pickled for distributed execution."""
        exp = DataQualityExpectation(
            name="positive_values",
            description="Values must be positive",
            validator_fn=lambda batch: batch["value"] > 0,
        )

        # Should be serializable
        pickled = pickle.dumps(exp)
        unpickled = pickle.loads(pickled)

        assert unpickled.name == exp.name
        assert unpickled.description == exp.description

    def test_sla_expectation_serialization(self):
        """Test that SLA expectations can be pickled."""
        exp = SLAExpectation(
            name="fast_job",
            description="Job must finish in 60 seconds",
            max_execution_time_seconds=60.0,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )

        pickled = pickle.dumps(exp)
        unpickled = pickle.loads(pickled)

        assert unpickled.name == exp.name
        assert unpickled.max_execution_time_seconds == 60.0
        assert unpickled.optimization_strategy == OptimizationStrategy.PERFORMANCE


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests for expectations in real workflows."""

    def test_expectation_in_pipeline(self, ray_start_regular_shared):
        """Test expectations in multi-stage pipeline."""

        @expect(expr=col("value") > 0)
        def validate_positive(batch):
            return batch

        @expect(max_execution_time_seconds=60.0)
        def process_data(batch):
            batch["value"] = batch["value"] * 2
            return batch

        ds = ray.data.range(100)
        result_ds = ds.map_batches(validate_positive).map_batches(process_data)

        # Verify data processed correctly
        assert result_ds.count() == 100

    def test_expectation_chaining(self, ray_start_regular_shared):
        """Test chaining multiple dataset.expect() calls."""
        ds = ray.data.from_items([{"value": i} for i in range(1, 11)])

        # Chain multiple expectations
        ds, result1 = ds.expect(expr=col("value") > 0)
        ds, result2 = ds.expect(expr=col("value") < 100)

        assert result1.passed is True
        assert result2.passed is True
        assert ds.count() == 10

    def test_both_sla_and_data_quality_expectations(self, ray_start_regular_shared):
        """Test using both SLA and data quality expectations together."""

        @expect(max_execution_time_seconds=60.0)
        @expect(expr=col("value") > 0)
        def process(batch):
            return batch

        ds = ray.data.range(100)
        result_ds = ds.map_batches(process)

        # Verify both types of expectations are attached
        all_exps = get_expectations_from_function(process)
        sla_exps = get_sla_expectations_from_function(process)

        assert len(all_exps) == 2
        assert len(sla_exps) == 1

    def test_expectation_with_large_dataset(self, ray_start_regular_shared):
        """Test expectations work with larger datasets."""
        ds = ray.data.range(10000)

        validated_ds, result = ds.expect(expr=col("id") >= 0)

        assert result.passed is True
        assert validated_ds.count() == 10000

    def test_expectation_result_with_all_fields(self):
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


# =============================================================================
# API Exports
# =============================================================================


class TestAPIExports:
    """Test that expectations are properly exported."""

    def test_expectations_import(self):
        """Test importing expectations from ray.data."""
        from ray.data import (
            DataQualityExpectation,
            Expectation,
            ExpectationResult,
            ExpectationType,
            OptimizationStrategy,
            SLAExpectation,
            expect,
        )

        # Verify all classes are importable
        assert Expectation is not None
        assert DataQualityExpectation is not None
        assert SLAExpectation is not None
        assert ExpectationResult is not None
        assert ExpectationType is not None
        assert OptimizationStrategy is not None
        assert expect is not None

    def test_expectations_direct_import(self):
        """Test importing from expectations module directly."""
        from ray.data.expectations import (
            get_expectations_from_function,
            get_sla_expectations_from_function,
        )

        # Verify helper functions are importable
        assert get_expectations_from_function is not None
        assert get_sla_expectations_from_function is not None
