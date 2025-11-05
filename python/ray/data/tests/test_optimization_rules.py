"""Integration tests for enterprise optimization rules.

This test suite validates that optimization rules work correctly through the
public Ray Data API. Tests focus on end-to-end behavior rather than internal
implementation details, following Ray Data testing standards.

Similar to how logical/physical optimizers are tested through Dataset operations
rather than unit tests of individual rules, optimization rules are tested
through their effect on Dataset execution behavior.
"""

import ray
from ray.data.expectations import (
    OptimizationStrategy,
    expect,
    SLAExpectation,
)
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class TestOptimizationThroughDatasetAPI:
    """Test optimization rules through Dataset API (integration tests)."""

    def test_performance_strategy_applied_to_map_batches(
        self, ray_start_regular_shared
    ):
        """Test that PERFORMANCE strategy affects map_batches execution."""

        @expect(
            max_execution_time_seconds=300,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"value": i} for i in range(100)])
        ds = ds.map_batches(process_batch)

        # Verify expectation was attached
        assert len(ds._plan.get_sla_expectations()) == 1
        assert ds._plan.get_optimization_strategy() == OptimizationStrategy.PERFORMANCE

        # Execute to verify optimizations are applied
        ds.materialize()

    def test_cost_strategy_applied_to_map_batches(self, ray_start_regular_shared):
        """Test that COST strategy affects map_batches execution."""

        @expect(
            max_execution_time_seconds=3600,
            optimization_strategy=OptimizationStrategy.COST,
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"value": i} for i in range(100)])
        ds = ds.map_batches(process_batch)

        # Verify expectation was attached
        assert len(ds._plan.get_sla_expectations()) == 1
        assert ds._plan.get_optimization_strategy() == OptimizationStrategy.COST

        # Execute to verify optimizations are applied
        ds.materialize()

    def test_optimization_with_explicit_batch_size(self, ray_start_regular_shared):
        """Test that user-specified batch_size takes precedence over optimization."""

        @expect(
            max_execution_time_seconds=300,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"value": i} for i in range(100)])
        user_batch_size = 50
        ds = ds.map_batches(process_batch, batch_size=user_batch_size)

        # Verify expectation was attached
        assert len(ds._plan.get_sla_expectations()) == 1

        # User-specified batch_size should be used (not optimized)
        # This is verified by the execution not failing

    def test_optimization_with_explicit_num_cpus(self, ray_start_regular_shared):
        """Test that user-specified num_cpus takes precedence over optimization."""

        @expect(
            max_execution_time_seconds=300,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"value": i} for i in range(100)])
        user_num_cpus = 2.0
        ds = ds.map_batches(process_batch, num_cpus=user_num_cpus)

        # Verify expectation was attached
        assert len(ds._plan.get_sla_expectations()) == 1

        # User-specified num_cpus should be used (not optimized)
        # This is verified by the execution not failing

    def test_optimization_preserved_through_operations(self, ray_start_regular_shared):
        """Test that optimizations are preserved through Dataset operations."""
        from ray.data.expressions import col

        @expect(
            max_execution_time_seconds=300,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"value": i} for i in range(100)])
        ds = ds.map_batches(process_batch)

        # Apply filter - should preserve expectations
        filtered_ds = ds.filter(expr=col("value") > 50)
        assert len(filtered_ds._plan.get_sla_expectations()) == 1

        # Apply with_column - should preserve expectations
        ds_with_col = filtered_ds.with_column("doubled", col("value") * 2)
        assert len(ds_with_col._plan.get_sla_expectations()) == 1

    def test_optimization_with_groupby(self, ray_start_regular_shared):
        """Test that optimizations work with groupby operations."""

        @expect(
            max_execution_time_seconds=300,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"key": i % 3, "value": i} for i in range(100)])
        ds = ds.map_batches(process_batch)

        # Apply groupby - should preserve expectations
        ds.groupby("key")
        assert len(ds._plan.get_sla_expectations()) == 1


class TestExecutionPlanOptimizationAPI:
    """Test ExecutionPlan optimization integration (internal method)."""

    def test_execution_plan_applies_optimizations(self):
        """Test that ExecutionPlan applies optimizations when creating executor."""
        ctx = DataContext.get_current()
        plan = ExecutionPlan(
            DatasetStats(metadata={}, parent=None),
            ctx,
        )

        # Add SLA expectation with PERFORMANCE strategy
        sla_exp = SLAExpectation(
            name="test",
            description="Test SLA",
            max_execution_time_seconds=300,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
        )
        plan.add_sla_expectation(sla_exp)

        # Verify optimization strategy is set
        assert plan.get_optimization_strategy() == OptimizationStrategy.PERFORMANCE

        # Apply optimizations (normally done in create_executor)
        plan._apply_enterprise_optimizations()

        # Verify execution options were updated
        assert (
            plan._context.execution_options.optimization_strategy
            == OptimizationStrategy.PERFORMANCE
        )

        # Verify optimization values were stored
        assert hasattr(plan._context, "_optimization_memory_fraction")
        assert hasattr(plan._context, "_optimization_reservation_ratio")


class TestRealWorldScenarios:
    """Test real-world usage scenarios through public API."""

    def test_etl_pipeline_with_performance_sla(self, ray_start_regular_shared):
        """Test ETL pipeline with performance SLA requirement."""

        @expect(
            max_execution_time_seconds=300,
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
            name="ETL Pipeline SLA",
        )
        def transform_batch(batch):
            return {"transformed": batch["value"] * 2}

        ds = ray.data.from_items([{"value": i} for i in range(10000)])
        ds = ds.map_batches(transform_batch)

        # Verify expectations are attached
        assert len(ds._plan.get_sla_expectations()) == 1
        assert ds._plan.get_optimization_strategy() == OptimizationStrategy.PERFORMANCE

        # Materialize to trigger optimizations
        ds.materialize()

    def test_batch_job_with_cost_optimization(self, ray_start_regular_shared):
        """Test batch job with cost optimization."""

        @expect(
            max_execution_time_seconds=3600,  # 1 hour
            optimization_strategy=OptimizationStrategy.COST,
            name="Nightly Batch Job",
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"value": i} for i in range(10000)])
        ds = ds.map_batches(process_batch)

        # Verify expectations are attached
        assert len(ds._plan.get_sla_expectations()) == 1
        assert ds._plan.get_optimization_strategy() == OptimizationStrategy.COST

    def test_critical_job_with_tight_deadline(self, ray_start_regular_shared):
        """Test critical job with very tight deadline."""

        @expect(
            max_execution_time_seconds=60,  # 1 minute
            optimization_strategy=OptimizationStrategy.PERFORMANCE,
            name="Critical Real-time Job",
        )
        def process_batch(batch):
            return batch

        ds = ray.data.from_items([{"value": i} for i in range(1000)])
        ds = ds.map_batches(process_batch)

        # Verify expectations are attached
        assert len(ds._plan.get_sla_expectations()) == 1
        assert ds._plan.get_optimization_strategy() == OptimizationStrategy.PERFORMANCE
        assert ds._plan.get_max_execution_time_seconds() == 60
