"""
Tests for Ray on Spark module structure and organization.

This test suite validates the proper separation of OSS and vendor-specific
functionality and ensures the module structure maintains clean architecture.
"""

import os
import sys
import pytest

# Test imports to ensure module structure is correct
import ray.util.spark.config as spark_config
import ray.util.spark.common as spark_common
import ray.util.spark.databricks as databricks
import ray.util.spark.spark_hook as spark_hook

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)


class TestSparkModuleStructure:
    """Test proper module structure and separation of concerns."""

    def test_databricks_utils_isolation(self):
        """Test that Databricks utilities are properly isolated."""
        # Databricks utilities should be self-contained
        assert hasattr(databricks, "is_in_databricks_runtime")
        assert hasattr(databricks, "verify_databricks_auth_env")
        assert hasattr(databricks, "get_databricks_temp_dir")

        # Should not have dependencies on Spark-specific code
        # (Databricks code should be isolated in its own package)

    def test_spark_hook_independence(self):
        """Test that Spark hook doesn't depend on Databricks-specific code."""
        # Spark hook should use common utilities
        assert hasattr(spark_hook, "SparkStartHook")

        # Should not import Databricks-specific functions directly
        hook = spark_hook.SparkStartHook(is_global=False)
        assert hasattr(hook, "get_default_temp_root_dir")
        assert hasattr(hook, "custom_environment_variables")

    def test_config_module_completeness(self):
        """Test that config module provides proper abstractions."""
        config = spark_config.get_config()

        # Should have environment detection
        assert hasattr(config, "environment")
        assert hasattr(config, "is_databricks_runtime")
        assert hasattr(config, "is_oss_spark_environment")

        # Should provide environment variables
        env_vars = config.get_environment_variables()
        assert isinstance(env_vars, dict)

    def test_common_utilities_availability(self):
        """Test that common utilities are available and functional."""
        # Resource detection should work
        cpu_count = spark_common.ResourceDetector.get_cpu_count()
        assert cpu_count > 0

        memory_info = spark_common.ResourceDetector.get_memory_info()
        assert "total" in memory_info
        assert "effective_total" in memory_info

        # Network detection should not fail
        interfaces = spark_common.NetworkDetector.get_available_interfaces()
        assert isinstance(interfaces, list)

    def test_hook_selection_logic(self):
        """Test that hook selection works correctly."""
        from ray.util.spark.cluster_init import _create_hook_entry

        # Should create OSS hook in non-Databricks environment
        original_env = os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)

        try:
            hook = _create_hook_entry(is_global=False)
            assert isinstance(hook, spark_hook.SparkStartHook)
        finally:
            if original_env:
                os.environ["DATABRICKS_RUNTIME_VERSION"] = original_env

    def test_databricks_hook_selection(self, monkeypatch):
        """Test that Databricks hook is selected in Databricks environment."""
        from ray.util.spark.cluster_init import _create_hook_entry

        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "12.2")

        hook = _create_hook_entry(is_global=False)
        assert isinstance(hook, databricks.DefaultDatabricksRayOnSparkStartHook)

    def test_environment_detection(self):
        """Test environment detection accuracy."""
        config = spark_config.get_config()
        env = config.environment

        # Should detect current environment correctly
        assert isinstance(env.is_databricks, bool)
        assert isinstance(env.is_kubernetes, bool)
        assert isinstance(env.is_yarn, bool)
        assert isinstance(env.is_containerized, bool)

    def test_no_cross_dependencies(self):
        """Test that modules don't have inappropriate cross-dependencies."""
        # Databricks utilities should not import OSS-specific modules
        import inspect
        import ray.util.spark.databricks.utils as db_utils

        source = inspect.getsource(db_utils)

        # Should not import common or config modules
        assert "from .common import" not in source
        assert "from .config import" not in source

        # Spark hook should not directly import Databricks functions
        source_spark = inspect.getsource(spark_hook)
        assert "databricks_hook" not in source_spark
        assert "get_databricks_" not in source_spark

    def test_configuration_dataclasses(self):
        """Test that configuration uses proper dataclasses."""
        # Test ResourceLimits dataclass
        limits = spark_config.ResourceLimits(cpu_cores=4, memory_bytes=8 * 1024**3)
        assert limits.is_valid()

        # Test invalid limits
        invalid_limits = spark_config.ResourceLimits(cpu_cores=-1)
        assert not invalid_limits.is_valid()

        # Test ClusterConfig dataclass
        cluster_config = spark_config.ClusterConfig(max_worker_nodes=4)
        assert cluster_config.max_worker_nodes == 4

    def test_path_utilities(self):
        """Test path utility functions."""
        temp_dir = spark_common.PathUtils.get_optimal_temp_dir()
        assert isinstance(temp_dir, str)
        assert len(temp_dir) > 0

        # Should be able to ensure directory exists
        test_dir = "/tmp/test_ray_spark"
        result = spark_common.PathUtils.ensure_directory_exists(test_dir)
        assert isinstance(result, bool)

        # Clean up
        if os.path.exists(test_dir):
            os.rmdir(test_dir)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
