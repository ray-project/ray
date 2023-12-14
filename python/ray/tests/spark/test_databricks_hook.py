import sys

import pytest
import os
import time
import ray
from pyspark.sql import SparkSession
from ray.util.spark import setup_ray_cluster
import ray.util.spark.databricks_hook


pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)


class MockDbApiEntry:
    def __init__(self):
        self.created_time = time.time()
        self.registered_job_groups = []

    def getIdleTimeMillisSinceLastNotebookExecution(self):
        return (time.time() - self.created_time) * 1000


class TestDatabricksHook:
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
        time.sleep(10)  # Wait all background spark job canceled.
        cls.spark.stop()
        os.environ.pop("SPARK_WORKER_CORES")

    def test_hook(self, monkeypatch):
        monkeypatch.setattr(
            "ray.util.spark.databricks_hook._DATABRICKS_DEFAULT_TMP_ROOT_DIR", "/tmp"
        )
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "12.2")
        monkeypatch.setenv("DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES", "0.5")
        db_api_entry = MockDbApiEntry()
        monkeypatch.setattr(
            "ray.util.spark.databricks_hook.get_db_entry_point", lambda: db_api_entry
        )
        try:
            setup_ray_cluster(
                max_worker_nodes=2,
                head_node_options={"include_dashboard": False},
            )
            cluster = ray.util.spark.cluster_init._active_ray_cluster
            assert not cluster.is_shutdown
            time.sleep(35)
            assert cluster.is_shutdown
            assert ray.util.spark.cluster_init._active_ray_cluster is None
        finally:
            if ray.util.spark.cluster_init._active_ray_cluster is not None:
                # if the test raised error and does not destroy cluster,
                # destroy it here.
                ray.util.spark.cluster_init._active_ray_cluster.shutdown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
