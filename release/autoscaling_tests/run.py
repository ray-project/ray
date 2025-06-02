import subprocess
import click
import json
import os
import time

from logger import logger

WORKLOAD_SCRIPTS = [
    "test_core.py",
]


def setup_cluster():
    from ray.cluster_utils import AutoscalingCluster

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
            subprocess.check_call(["python", workload])
        except subprocess.CalledProcessError as e:
            failed_workloads.append((workload, e))

    if failed_workloads:
        for workload, e in failed_workloads:
            logger.error(f"Workload {workload} failed with {e}")
    else:
        logger.info("All workloads passed!")


@click.command()
@click.option("--local", is_flag=True, help="Run locally.", default=False)
def run(local):
    start_time = time.time()
    cluster = None
    try:
        if local:
            cluster = setup_cluster()
            run_test()
            cluster.shutdown()
        else:
            run_test()

        success = "1"
    except Exception as e:
        logger.error(f"Test failed with {e}")
        success = "0"
    finally:
        if cluster:
            cluster.shutdown()

    results = {
        "time": time.time() - start_time,
        "success": success,
    }
    if "TEST_OUTPUT_JSON" in os.environ:
        with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
            json.dump(results, out_file)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    run()
