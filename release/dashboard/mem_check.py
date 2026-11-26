import argparse
import json
import os
import time
from typing import List

import ray
from ray._common.network_utils import build_address
from ray._common.test_utils import fetch_prometheus_metrics
from ray._private.memory_monitor import MemoryMonitor, get_top_n_memory_usage
from ray.job_submission import JobStatus, JobSubmissionClient

# Initialize ray to avoid autosuspend.
addr = ray.init()


def get_agent_uss_bytes() -> List[float]:
    """Read each agent's USS from its node, so the check doesn't depend on the
    platform's metrics pipeline."""
    addresses = [
        build_address(node["NodeManagerAddress"], node["MetricsExportPort"])
        for node in ray.nodes()
        if node["Alive"]
    ]
    samples = fetch_prometheus_metrics(addresses).get("ray_component_uss_bytes", [])
    return [s.value for s in samples if s.labels.get("Component") == "agent"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--working-dir",
        required=True,
        help="working_dir to use for the job within this test.",
    )
    args = parser.parse_args()
    client = JobSubmissionClient("http://127.0.0.1:8265")
    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python workload.py",
        runtime_env={"working_dir": args.working_dir},
    )
    print(job_id)

    # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
    client = JobSubmissionClient("http://127.0.0.1:8265")
    m = MemoryMonitor()
    start = time.time()
    # Run for 3 hours
    initial_used_gb = m.get_memory_usage()[0]

    terminal_states = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

    while time.time() - start < 3600 * 3:
        print(f"{round((time.time() - start) / 60, 2)}m passed...")
        m.raise_if_low_memory()
        used_gb = m.get_memory_usage()[0]
        print("Used GB: ", used_gb)
        print(get_top_n_memory_usage())
        print("\n\n")

        # Terminate the test if the job is failed.
        status = client.get_job_status(job_id)
        print(f"Job status: {status}")
        if status in terminal_states:
            break
        time.sleep(15)

    ending_used_gb = m.get_memory_usage()[0]

    mem_growth = ending_used_gb - initial_used_gb
    top_n_mem_usage = get_top_n_memory_usage()
    print(top_n_mem_usage)
    print(f"Memory growth: {mem_growth} GB")

    if status == JobStatus.FAILED or status == JobStatus.STOPPED:
        print(client.get_job_logs(job_id))
        assert False, "Job has failed."

    uss_bytes_for_agent_component = get_agent_uss_bytes()
    assert (
        len(uss_bytes_for_agent_component) > 0
    ), "Agent component memory metrics are not found."
    for bytes in uss_bytes_for_agent_component:
        print(f"Agent component memory usage: {bytes} bytes")
        assert bytes < 500 * 1024 * 1024, "Agent component memory usage is too high."

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        results = {
            "memory_growth_gb": mem_growth,
        }
        results["perf_metrics"] = [
            {
                "perf_metric_name": "memory_growth_gb",
                "perf_metric_value": mem_growth,
                "perf_metric_type": "LATENCY",
            }
        ]

        f.write(json.dumps(results))
