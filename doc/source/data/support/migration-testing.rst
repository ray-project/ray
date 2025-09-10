.. _migration-testing:

Migration & Testing Guide
=========================

This guide provides comprehensive strategies for migrating to Ray Data from other data processing frameworks and testing Ray Data implementations to ensure reliability and performance.

**What you'll learn:**

* Migration strategies from pandas, Spark, Dask, and other frameworks
* Testing methodologies for Ray Data pipelines
* Performance benchmarking and validation
* Production deployment testing

Migration Strategies
--------------------

**From Pandas to Ray Data**

Pandas is excellent for single-machine data processing, but Ray Data provides distributed capabilities for larger datasets:

.. code-block:: python

    import pandas as pd
    import ray

    # Original pandas workflow
    def pandas_workflow():
        df = pd.read_csv("data.csv")
        df["new_column"] = df["value"] * 2
        result = df.groupby("category").sum()
        result.to_csv("output.csv")
        return result

    # Equivalent Ray Data workflow
    def ray_data_workflow():
        ds = ray.data.read_csv("data.csv")
        ds = ds.add_column("new_column", lambda row: row["value"] * 2)
        result = ds.groupby("category").aggregate(ray.data.aggregate.Sum("value"))
        result.write_csv("output.csv")
        return result

**Migration checklist:**
- [ ] Identify operations that benefit from distributed processing
- [ ] Convert pandas operations to Ray Data equivalents
- [ ] Test with representative data sizes
- [ ] Validate output consistency
- [ ] Benchmark performance improvements

**From Apache Spark to Ray Data**

Spark and Ray Data share similar distributed processing concepts:

.. code-block:: python

    # Spark (PySpark) equivalent
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    def spark_workflow():
        spark = SparkSession.builder.appName("example").getOrCreate()
        df = spark.read.parquet("data.parquet")
        df = df.filter(col("amount") > 100)
        df = df.groupBy("category").sum("amount")
        df.write.parquet("output.parquet")
        return df

    # Ray Data equivalent
    def ray_data_workflow():
        ds = ray.data.read_parquet("data.parquet")
        ds = ds.filter(lambda row: row["amount"] > 100)
        result = ds.groupby("category").aggregate(ray.data.aggregate.Sum("amount"))
        result.write_parquet("output.parquet")
        return result

**Key differences and considerations:**
- Ray Data uses Python-native APIs vs Spark's SQL-like operations
- Different resource management approaches
- Ray Data excels at multimodal data processing
- Consider data locality and cluster configuration

**From Dask to Ray Data**

Dask and Ray Data both provide distributed Python data processing:

.. code-block:: python

    import dask.dataframe as dd
    import ray

    # Dask workflow
    def dask_workflow():
        df = dd.read_csv("data.csv")
        df["processed"] = df["value"].apply(lambda x: x * 2, meta=('processed', 'f8'))
        result = df.groupby("category").sum().compute()
        return result

    # Ray Data equivalent
    def ray_data_workflow():
        ds = ray.data.read_csv("data.csv")
        ds = ds.map(lambda row: {**row, "processed": row["value"] * 2})
        result = ds.groupby("category").aggregate(ray.data.aggregate.Sum("value"))
        return result

**Migration benefits:**
- Unified platform for data processing and ML
- Better GPU support and multimodal data handling
- Integrated with Ray's broader ecosystem

Testing Methodologies
----------------------

**Unit Testing Ray Data Pipelines**

Create comprehensive unit tests for your Ray Data transformations:

.. code-block:: python

    import pytest
    import ray
    from ray.data.aggregate import Sum, Count, Mean

    class TestRayDataPipeline:
        """Comprehensive test suite for Ray Data pipeline."""
        
        @pytest.fixture(autouse=True)
        def setup_ray(self):
            """Initialize Ray for testing."""
            if not ray.is_initialized():
                ray.init(local_mode=True)
            yield
            ray.shutdown()
        
        def test_data_loading(self):
            """Test data loading from various sources."""
            # Test CSV loading
            test_data = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
            ds = ray.data.from_items(test_data)
            
            assert ds.count() == 2
            assert ds.schema().names == ["id", "value"]
        
        def test_data_transformation(self):
            """Test data transformation operations."""
            test_data = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
            ds = ray.data.from_items(test_data)
            
            # Test map operation
            transformed = ds.map(lambda row: {**row, "doubled": row["value"] * 2})
            result = transformed.take_all()
            
            assert len(result) == 2
            assert result[0]["doubled"] == 200
            assert result[1]["doubled"] == 400
        
        def test_aggregation_operations(self):
            """Test aggregation and groupby operations."""
            test_data = [
                {"category": "A", "value": 100},
                {"category": "A", "value": 150},
                {"category": "B", "value": 200},
                {"category": "B", "value": 250}
            ]
            ds = ray.data.from_items(test_data)
            
            # Test aggregation
            result = ds.groupby("category").aggregate(
                Sum("value"),
                Count("value"),
                Mean("value")
            ).take_all()
            
            # Validate results
            result_dict = {r["category"]: r for r in result}
            assert result_dict["A"]["sum(value)"] == 250
            assert result_dict["A"]["count(value)"] == 2
            assert result_dict["A"]["mean(value)"] == 125
        
        def test_error_handling(self):
            """Test error handling and recovery."""
            test_data = [{"id": 1, "value": "100"}, {"id": 2, "value": "invalid"}]
            ds = ray.data.from_items(test_data)
            
            def safe_convert(row):
                try:
                    return {**row, "numeric_value": float(row["value"])}
                except ValueError:
                    return {**row, "numeric_value": None}
            
            result = ds.map(safe_convert).take_all()
            
            assert result[0]["numeric_value"] == 100.0
            assert result[1]["numeric_value"] is None
        
        def test_performance_characteristics(self):
            """Test performance characteristics and resource usage."""
            import time
            
            # Generate test data
            large_data = [{"id": i, "value": i * 2} for i in range(10000)]
            ds = ray.data.from_items(large_data)
            
            # Measure processing time
            start_time = time.time()
            result = ds.map(lambda row: {**row, "processed": row["value"] * 2}) \
                      .groupby("id") \
                      .aggregate(Sum("value")) \
                      .count()
            end_time = time.time()
            
            processing_time = end_time - start_time
            assert processing_time < 30  # Should complete within 30 seconds
            assert result == 10000  # All records processed

**Integration Testing**

Test Ray Data integration with external systems:

.. code-block:: python

    import pytest
    import ray
    import tempfile
    import os

    class TestRayDataIntegration:
        """Integration tests for Ray Data with external systems."""
        
        def test_file_system_integration(self):
            """Test reading from and writing to file systems."""
            with tempfile.TemporaryDirectory() as temp_dir:
                # Create test data
                test_data = [{"id": i, "value": i * 10} for i in range(100)]
                ds = ray.data.from_items(test_data)
                
                # Write to file system
                output_path = os.path.join(temp_dir, "test_output")
                ds.write_parquet(output_path)
                
                # Read back and validate
                loaded_ds = ray.data.read_parquet(output_path)
                loaded_data = loaded_ds.take_all()
                
                assert len(loaded_data) == 100
                assert loaded_data[0]["id"] == 0
                assert loaded_data[0]["value"] == 0
        
        def test_database_integration(self):
            """Test database connectivity (mock example)."""
            # Note: In real tests, use test databases or mocking
            
            def mock_database_read():
                return [
                    {"customer_id": 1, "order_amount": 150.0},
                    {"customer_id": 2, "order_amount": 200.0}
                ]
            
            # Simulate database read
            ds = ray.data.from_items(mock_database_read())
            
            # Process data
            result = ds.groupby("customer_id") \
                      .aggregate(Sum("order_amount")) \
                      .take_all()
            
            assert len(result) == 2
            assert any(r["customer_id"] == 1 and r["sum(order_amount)"] == 150.0 for r in result)

**Performance Testing & Benchmarking**

Implement comprehensive performance testing:

.. code-block:: python

    import time
    import psutil
    import ray
    from ray.data import DataContext

    class PerformanceBenchmark:
        """Performance benchmarking for Ray Data operations."""
        
        def __init__(self):
            self.results = {}
        
        def benchmark_operation(self, name, operation_func, dataset):
            """Benchmark a specific Ray Data operation."""
            
            # Measure memory before
            memory_before = psutil.virtual_memory().used
            
            # Measure execution time
            start_time = time.time()
            result = operation_func(dataset)
            
            # Force materialization for accurate timing
            if hasattr(result, 'materialize'):
                result = result.materialize()
            elif hasattr(result, 'take_all'):
                result.take_all()
            
            end_time = time.time()
            
            # Measure memory after
            memory_after = psutil.virtual_memory().used
            
            # Store results
            self.results[name] = {
                'execution_time': end_time - start_time,
                'memory_delta': memory_after - memory_before,
                'timestamp': time.time()
            }
            
            return result
        
        def run_comprehensive_benchmark(self):
            """Run comprehensive performance benchmark."""
            
            # Generate test datasets of different sizes
            small_ds = ray.data.range(1000)
            medium_ds = ray.data.range(100000)
            large_ds = ray.data.range(1000000)
            
            datasets = [
                ("small", small_ds),
                ("medium", medium_ds),
                ("large", large_ds)
            ]
            
            for size_name, dataset in datasets:
                # Benchmark basic operations
                self.benchmark_operation(
                    f"{size_name}_map",
                    lambda ds: ds.map(lambda row: {"id": row["id"], "doubled": row["id"] * 2}),
                    dataset
                )
                
                self.benchmark_operation(
                    f"{size_name}_filter",
                    lambda ds: ds.filter(lambda row: row["id"] % 2 == 0),
                    dataset
                )
                
                self.benchmark_operation(
                    f"{size_name}_groupby",
                    lambda ds: ds.groupby(lambda row: row["id"] % 10).aggregate(Sum("id")),
                    dataset
                )
        
        def compare_configurations(self):
            """Compare performance across different Ray Data configurations."""
            
            test_data = ray.data.range(100000)
            
            # Test different block sizes
            for block_size in [32, 64, 128, 256]:  # MB
                ctx = DataContext.get_current()
                ctx.target_max_block_size = block_size * 1024 * 1024
                
                self.benchmark_operation(
                    f"block_size_{block_size}MB",
                    lambda ds: ds.map(lambda row: {"processed": row["id"] * 2}) \
                              .groupby(lambda row: row["id"] % 100) \
                              .aggregate(Sum("processed")),
                    test_data
                )
        
        def generate_report(self):
            """Generate performance benchmark report."""
            
            print("Ray Data Performance Benchmark Report")
            print("=" * 50)
            
            for operation, metrics in self.results.items():
                print(f"\nOperation: {operation}")
                print(f"  Execution Time: {metrics['execution_time']:.2f} seconds")
                print(f"  Memory Delta: {metrics['memory_delta'] / 1024 / 1024:.1f} MB")
            
            # Find fastest operations
            fastest_ops = sorted(self.results.items(), key=lambda x: x[1]['execution_time'])[:3]
            print(f"\nTop 3 Fastest Operations:")
            for i, (op, metrics) in enumerate(fastest_ops, 1):
                print(f"  {i}. {op}: {metrics['execution_time']:.2f}s")

**Data Quality Testing**

Implement data quality validation tests:

.. code-block:: python

    import ray
    from typing import Dict, Any, List

    class DataQualityValidator:
        """Comprehensive data quality validation for Ray Data pipelines."""
        
        def __init__(self):
            self.validation_results = {}
        
        def validate_schema(self, dataset, expected_schema: Dict[str, Any]):
            """Validate dataset schema matches expectations."""
            
            actual_schema = dataset.schema()
            
            # Check column names
            expected_columns = set(expected_schema.keys())
            actual_columns = set(actual_schema.names)
            
            missing_columns = expected_columns - actual_columns
            extra_columns = actual_columns - expected_columns
            
            self.validation_results['schema'] = {
                'valid': len(missing_columns) == 0 and len(extra_columns) == 0,
                'missing_columns': list(missing_columns),
                'extra_columns': list(extra_columns)
            }
            
            return self.validation_results['schema']['valid']
        
        def validate_data_ranges(self, dataset, column_ranges: Dict[str, tuple]):
            """Validate data falls within expected ranges."""
            
            validation_results = {}
            
            for column, (min_val, max_val) in column_ranges.items():
                # Get column statistics
                col_min = dataset.min(column)
                col_max = dataset.max(column)
                
                validation_results[column] = {
                    'valid': col_min >= min_val and col_max <= max_val,
                    'actual_min': col_min,
                    'actual_max': col_max,
                    'expected_min': min_val,
                    'expected_max': max_val
                }
            
            self.validation_results['ranges'] = validation_results
            return all(r['valid'] for r in validation_results.values())
        
        def validate_completeness(self, dataset, required_columns: List[str]):
            """Validate data completeness (no null values in required columns)."""
            
            completeness_results = {}
            
            for column in required_columns:
                # Count null values
                null_count = dataset.filter(lambda row: row.get(column) is None).count()
                total_count = dataset.count()
                
                completeness_results[column] = {
                    'valid': null_count == 0,
                    'null_count': null_count,
                    'total_count': total_count,
                    'completeness_ratio': (total_count - null_count) / total_count
                }
            
            self.validation_results['completeness'] = completeness_results
            return all(r['valid'] for r in completeness_results.values())
        
        def validate_uniqueness(self, dataset, unique_columns: List[str]):
            """Validate uniqueness constraints."""
            
            uniqueness_results = {}
            
            for column in unique_columns:
                # Count unique values vs total values
                unique_count = dataset.select_columns([column]).unique().count()
                total_count = dataset.count()
                
                uniqueness_results[column] = {
                    'valid': unique_count == total_count,
                    'unique_count': unique_count,
                    'total_count': total_count,
                    'duplicate_count': total_count - unique_count
                }
            
            self.validation_results['uniqueness'] = uniqueness_results
            return all(r['valid'] for r in uniqueness_results.values())
        
        def generate_quality_report(self):
            """Generate comprehensive data quality report."""
            
            print("Data Quality Validation Report")
            print("=" * 40)
            
            for category, results in self.validation_results.items():
                print(f"\n{category.upper()} Validation:")
                
                if category == 'schema':
                    print(f"  Valid: {results['valid']}")
                    if results['missing_columns']:
                        print(f"  Missing columns: {results['missing_columns']}")
                    if results['extra_columns']:
                        print(f"  Extra columns: {results['extra_columns']}")
                
                elif category in ['ranges', 'completeness', 'uniqueness']:
                    for column, metrics in results.items():
                        print(f"  {column}: {'✓' if metrics['valid'] else '✗'}")
                        for key, value in metrics.items():
                            if key != 'valid':
                                print(f"    {key}: {value}")

**Example: Complete Testing Pipeline**

.. code-block:: python

    def run_complete_testing_pipeline():
        """Run a complete testing pipeline for Ray Data implementation."""
        
        # Initialize Ray
        ray.init(local_mode=True)
        
        try:
            # 1. Unit tests
            print("Running unit tests...")
            pytest.main(["-v", "test_ray_data_pipeline.py"])
            
            # 2. Performance benchmarking
            print("\nRunning performance benchmarks...")
            benchmark = PerformanceBenchmark()
            benchmark.run_comprehensive_benchmark()
            benchmark.compare_configurations()
            benchmark.generate_report()
            
            # 3. Data quality validation
            print("\nRunning data quality validation...")
            test_data = ray.data.from_items([
                {"id": 1, "name": "Alice", "age": 25, "salary": 50000},
                {"id": 2, "name": "Bob", "age": 30, "salary": 60000},
                {"id": 3, "name": "Charlie", "age": 35, "salary": 70000}
            ])
            
            validator = DataQualityValidator()
            validator.validate_schema(test_data, {"id": int, "name": str, "age": int, "salary": float})
            validator.validate_data_ranges(test_data, {"age": (0, 100), "salary": (0, 1000000)})
            validator.validate_completeness(test_data, ["id", "name", "age"])
            validator.validate_uniqueness(test_data, ["id"])
            validator.generate_quality_report()
            
            print("\nTesting pipeline completed successfully!")
            
        finally:
            ray.shutdown()

Production Deployment Testing
-----------------------------

**Staging Environment Testing**

Create comprehensive staging environment tests:

.. code-block:: python

    import ray
    from ray.data import DataContext

    class ProductionReadinessTest:
        """Test Ray Data implementation for production readiness."""
        
        def test_cluster_connectivity(self):
            """Test connectivity to production-like cluster."""
            
            # Test cluster initialization
            ray.init(address="ray://staging-cluster:10001")
            
            # Verify cluster resources
            cluster_resources = ray.cluster_resources()
            assert cluster_resources.get("CPU", 0) > 0
            assert cluster_resources.get("memory", 0) > 0
            
            print(f"Cluster resources: {cluster_resources}")
        
        def test_data_source_connectivity(self):
            """Test connectivity to production data sources."""
            
            # Test database connectivity
            try:
                ds = ray.data.read_sql(
                    "postgresql://staging-db:5432/testdb",
                    "SELECT COUNT(*) as count FROM test_table LIMIT 1"
                )
                result = ds.take(1)
                assert len(result) == 1
                print("Database connectivity: ✓")
            except Exception as e:
                print(f"Database connectivity failed: {e}")
                raise
            
            # Test cloud storage connectivity
            try:
                ds = ray.data.read_parquet("s3://staging-bucket/test-data/")
                count = ds.count()
                assert count > 0
                print(f"Cloud storage connectivity: ✓ ({count} records)")
            except Exception as e:
                print(f"Cloud storage connectivity failed: {e}")
                raise
        
        def test_performance_at_scale(self):
            """Test performance with production-scale data."""
            
            # Configure for production-like settings
            ctx = DataContext.get_current()
            ctx.target_max_block_size = 128 * 1024 * 1024  # 128MB
            ctx.enable_auto_log_stats = True
            
            # Load large dataset
            large_ds = ray.data.read_parquet("s3://staging-bucket/large-dataset/")
            
            # Run typical production workload
            result = large_ds.filter(lambda row: row["amount"] > 100) \
                            .map_batches(production_transform_function) \
                            .groupby("category") \
                            .aggregate(Sum("processed_amount"))
            
            # Validate performance
            stats = result.stats()
            assert "execution completed" in stats
            
            print("Production-scale performance test: ✓")
        
        def test_error_recovery(self):
            """Test error handling and recovery in production scenarios."""
            
            # Test with some corrupted data
            mixed_data = ray.data.from_items([
                {"id": 1, "value": 100},
                {"id": 2, "value": "corrupted"},
                {"id": 3, "value": 300}
            ])
            
            # Configure error tolerance
            ctx = DataContext.get_current()
            ctx.max_errored_blocks = 1  # Allow some failures
            
            def robust_transform(batch):
                processed = []
                for row in batch.to_pylist():
                    try:
                        processed.append({
                            "id": row["id"],
                            "processed_value": float(row["value"]) * 2
                        })
                    except (ValueError, TypeError):
                        # Handle corrupted data gracefully
                        processed.append({
                            "id": row["id"],
                            "processed_value": 0  # Default value
                        })
                return ray.data.from_pylist(processed)
            
            result = mixed_data.map_batches(robust_transform)
            output = result.take_all()
            
            # Validate error handling
            assert len(output) == 3
            assert output[1]["processed_value"] == 0  # Corrupted data handled
            
            print("Error recovery test: ✓")

**Continuous Integration Testing**

Create CI/CD pipeline tests:

.. code-block:: yaml

    # .github/workflows/ray-data-tests.yml
    name: Ray Data Pipeline Tests
    
    on:
      push:
        branches: [main, develop]
      pull_request:
        branches: [main]
    
    jobs:
      test:
        runs-on: ubuntu-latest
        
        steps:
        - uses: actions/checkout@v3
        
        - name: Set up Python
          uses: actions/setup-python@v3
          with:
            python-version: '3.9'
        
        - name: Install dependencies
          run: |
            pip install ray[data] pytest
            pip install -r requirements.txt
        
        - name: Run Ray Data unit tests
          run: |
            pytest tests/test_ray_data_pipeline.py -v
        
        - name: Run integration tests
          run: |
            pytest tests/test_integration.py -v
        
        - name: Run performance benchmarks
          run: |
            python scripts/benchmark_pipeline.py
        
        - name: Validate data quality
          run: |
            python scripts/validate_data_quality.py

Next Steps
----------

**Migration Planning**

1. **Assessment Phase**:
   * Analyze current data processing workflows
   * Identify components that benefit from Ray Data
   * Plan migration timeline and resources

2. **Pilot Implementation**:
   * Start with non-critical workloads
   * Implement comprehensive testing
   * Gather performance metrics

3. **Production Rollout**:
   * Gradual migration of production workloads
   * Monitor performance and reliability
   * Maintain rollback capabilities

**Testing Strategy Development**

1. **Test Suite Creation**:
   * Unit tests for all transformations
   * Integration tests for external systems
   * Performance benchmarks for optimization

2. **Quality Assurance**:
   * Data quality validation frameworks
   * Error handling and recovery testing
   * Production readiness validation

3. **Continuous Improvement**:
   * Regular performance monitoring
   * Test suite maintenance and updates
   * Community feedback integration

For more detailed guidance on specific migration scenarios or testing approaches, consult the Ray Data community resources or consider professional services support.
