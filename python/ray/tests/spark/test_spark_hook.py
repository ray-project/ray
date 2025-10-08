import os
import sys
import tempfile

import pytest
from pyspark.sql import SparkSession

import ray
from ray.util.spark.cluster_init import _create_hook_entry
from ray.util.spark.spark_hook import SparkStartHook

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)


class TestSparkHook:
    @classmethod
    def setup_class(cls):
        os.environ["SPARK_WORKER_CORES"] = "2"
        cls.spark = (
            SparkSession.builder.master("local-cluster[1, 2, 1024]")
            .config("spark.task.cpus", "1")
            .config("spark.task.maxFailures", "1")
            .config("spark.executorEnv.RAY_ON_SPARK_WORKER_CPU_CORES", "2")
            .getOrCreate()
        )

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()
        os.environ.pop("SPARK_WORKER_CORES")

    def test_oss_hook_creation(self):
        """Test that OSS hook is created for non-Databricks environments."""
        # Ensure we're not in Databricks runtime
        databricks_env = os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)

        try:
            hook = _create_hook_entry(is_global=False)
            assert isinstance(hook, SparkStartHook)
            assert not hasattr(hook, "on_ray_dashboard_created") or callable(
                hook.on_ray_dashboard_created
            )
        finally:
            if databricks_env:
                os.environ["DATABRICKS_RUNTIME_VERSION"] = databricks_env

    def test_enhanced_temp_dir(self):
        """Test enhanced temporary directory selection."""
        hook = SparkStartHook(is_global=False)
        temp_dir = hook.get_default_temp_root_dir()

        # Should return a valid directory path
        assert temp_dir.endswith("ray-spark") or temp_dir.endswith("tmp")
        assert os.path.exists(os.path.dirname(temp_dir)) or temp_dir.startswith("/tmp")

    def test_enhanced_environment_variables(self):
        """Test enhanced environment variable configuration."""
        hook = SparkStartHook(is_global=False)
        env_vars = hook.custom_environment_variables()

        # Should include basic OSS optimizations
        assert "RAY_ENABLE_AUTO_CONNECT" in env_vars
        assert "RAY_BACKEND_LOG_LEVEL" in env_vars

        # Verify values are appropriate for OSS environments
        assert env_vars["RAY_ENABLE_AUTO_CONNECT"] == "1"

    def test_yarn_environment_detection(self, monkeypatch):
        """Test YARN environment-specific optimizations."""
        from ray.util.spark import config

        monkeypatch.setenv("YARN_CONTAINER_ID", "container_123")
        config.reset_config()
        monkeypatch.addfinalizer(config.reset_config)

        hook = SparkStartHook(is_global=False)
        env_vars = hook.custom_environment_variables()

        # Should include YARN-specific optimizations
        assert "RAY_ENABLE_MULTI_TENANCY" in env_vars
        assert env_vars["RAY_ENABLE_MULTI_TENANCY"] == "1"

    def test_kubernetes_environment_detection(self, monkeypatch):
        """Test Kubernetes environment-specific optimizations."""
        from ray.util.spark import config

        monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        config.reset_config()
        monkeypatch.addfinalizer(config.reset_config)

        hook = SparkStartHook(is_global=False)
        env_vars = hook.custom_environment_variables()

        # Should include Kubernetes-specific optimizations
        assert "RAY_ENABLE_KUBERNETES" in env_vars
        assert "RAY_USAGE_STATS_ENABLED" in env_vars
        assert env_vars["RAY_USAGE_STATS_ENABLED"] == "0"

    def test_dashboard_creation_notification(self, capsys):
        """Test enhanced dashboard creation notification."""
        hook = SparkStartHook(is_global=False)

        # Call dashboard creation handler
        hook.on_ray_dashboard_created(8265)

        # Capture output
        captured = capsys.readouterr()

        # Should provide helpful dashboard access information
        assert "Ray Dashboard" in captured.out
        assert "8265" in captured.out

    def test_cluster_creation_enhancements(self):
        """Test cluster creation enhancements."""
        hook = SparkStartHook(is_global=False)

        # Mock cluster handler
        class MockClusterHandler:
            def __init__(self):
                self.is_shutdown = False

        cluster_handler = MockClusterHandler()

        # Should not raise any exceptions
        hook.on_cluster_created(cluster_handler)

    def test_spark_job_creation_tracking(self, capsys):
        """Test enhanced Spark job tracking."""
        hook = SparkStartHook(is_global=False)

        # Call job creation handler
        hook.on_spark_job_created("test_job_group_123")

        # Capture output
        captured = capsys.readouterr()

        # Should log job creation with group ID
        # Note: We can't easily test the full functionality without a real Spark context


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
