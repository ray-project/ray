import time
import os
import json

import ray

from ray._private.memory_monitor import MemoryMonitor, get_top_n_memory_usage
from ray._private.test_utils import raw_metrics
from ray.job_submission import JobSubmissionClient, JobStatus

# Initialize ray to avoid autosuspend.
addr = ray.init()

if __name__ == "__main__":
    client = JobSubmissionClient("http://127.0.0.1:8265")
    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint="python workload.py",
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

    me = raw_metrics(addr)
    found = False
    for metric, samples in me.items():
        if metric == "ray_component_uss_mb":
            for sample in samples:
                if sample.labels["Component"] == "agent":
                    print(f"Metrics found memory usage : {sample.value} MB")
                    found = True
                    # Make sure it doesn't use more than 500MB of data.
                    assert sample.value < 500

    assert found, "Agent memory metrics are not found."

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        results = {
            "memory_growth_gb": mem_growth,
            "success": 1,
        }
        results["perf_metrics"] = [
            {
                "perf_metric_name": "memory_growth_gb",
                "perf_metric_value": mem_growth,
                "perf_metric_type": "LATENCY",
            }
        ]

        f.write(json.dumps(results))
