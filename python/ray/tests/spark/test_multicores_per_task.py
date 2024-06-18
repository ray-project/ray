import sys
import pytest
import os
from pyspark.sql import SparkSession
from ray.tests.spark.test_basic import _setup_ray_on_spark_envs
from ray.tests.spark.test_ACC import RayOnSparkACCClusterTestBase

pytestmark = [
    pytest.mark.skipif(
        os.name != "posix",
        reason="Ray on spark only supports running on POSIX system.",
    ),
    pytest.mark.timeout(1500),
]


def setup_module():
    _setup_ray_on_spark_envs()


class TestMultiCoresPerTaskCluster(RayOnSparkACCClusterTestBase):
    @classmethod
    def setup_class(cls):
        cls.num_total_cpus = 4
        cls.num_total_accs = 4
        cls.num_cpus_per_spark_task = 2
        cls.num_accs_per_spark_task = 2
        cls.max_spark_tasks = 2
        acc_discovery_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "discover_4_acc.sh"
        )
        os.environ["SPARK_WORKER_CORES"] = "4"
        cls.spark = (
            SparkSession.builder.master("local-cluster[1, 4, 1024]")
            .config("spark.task.cpus", "2")
            .config("spark.task.resource.acc.amount", "2")
            .config("spark.executor.cores", "4")
            .config("spark.worker.resource.acc.amount", "4")
            .config("spark.executor.resource.acc.amount", "4")
            .config("spark.task.maxFailures", "1")
            .config(
                "spark.worker.resource.acc.discoveryScript", acc_discovery_script_path
            )
            .config("spark.executorEnv.RAY_ON_SPARK_WORKER_CPU_CORES", "4")
            .config("spark.executorEnv.RAY_ON_SPARK_WORKER_ACC_NUM", "4")
            .getOrCreate()
        )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
