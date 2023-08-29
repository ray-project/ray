import subprocess

# Logger
import logging
from ray.cluster_utils import AutoscalingCluster
from rich.logging import RichHandler

FORMAT = "%(asctime)s - [%(filename)s:%(lineno)d] - %(message)s"

logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

logger = logging.getLogger(__name__)


WORKLOAD_SCRIPTS = [
    "test_core.py",
]


def setup_cluster() -> AutoscalingCluster:
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 4},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 10,
            },
        },
        idle_timeout_minutes=1 * 0.1,
    )

    cluster.start(_system_config={"enable_autoscaler_v2": True})
    return cluster


def run_test():
    failed_workloads = []
    for workload in WORKLOAD_SCRIPTS:
        # Run the python script.
        logger.info(f"Running workload {workload}:")
        try:
            subprocess.check_call(["python", "workloads/" + workload])
        except subprocess.CalledProcessError as e:
            failed_workloads.append((workload, e))

    if failed_workloads:
        for workload, e in failed_workloads:
            logger.error(f"Workload {workload} failed with {e}")
    else:
        logger.info("All workloads passed!")


if __name__ == "__main__":
    cluster = setup_cluster()
    run_test()

    cluster.shutdown()
