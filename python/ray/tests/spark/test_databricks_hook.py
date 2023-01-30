import sys

import mock
import pytest
import os
import time
import ray
from pyspark.sql import SparkSession
from ray.util.spark import setup_ray_cluster


pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Ray on spark only supports running on Linux.",
)


class MockDbApiEntry:

    def __init__(self):
        self.idle_time = 0
        self.registered_job_groups = []

    def getIdleTimeMillisSinceLastNotebookExecution(self):
        return (time.time() - self.idle_time) * 1000

    def registerBackgroundSparkJobGroup(self, job_group_id):
        self.registered_job_groups.append(job_group_id)


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

    def test_hook(self):
        try:
            db_api_entry = MockDbApiEntry()
            with mock.patch(
                "ray.util.spark.databricks_hook._get_db_api_entry",
                return_value=db_api_entry,
            ), mock.patch.dict(
                os.environ,
                {'DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_TIMEOUT_MINUTES': "2"},
            ):
                setup_ray_cluster(
                    num_worker_nodes=2,
                    head_node_options={"include_dashboard": False},
                )
                cluster = ray.util.spark.cluster_init._active_ray_cluster
                assert not cluster.is_shutdown
                assert db_api_entry.registered_job_groups == \
                       [cluster.spark_job_group_id]
                time.sleep(2.5)
                assert cluster.is_shutdown
                assert ray.util.spark.cluster_init._active_ray_cluster is None
        finally:
            if ray.util.spark.cluster_init._active_ray_cluster is not None:
                # if the test raised error and does not destroy cluster,
                # destroy it here.
                ray.util.spark._active_ray_cluster.shutdown()
                time.sleep(5)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
