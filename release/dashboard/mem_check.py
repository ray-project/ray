import time
import os
import json

import ray

from ray._private.memory_monitor import MemoryMonitor, get_top_n_memory_usage

# Initialize ray to avoid autosuspend.
ray.init()

if __name__ == "__main__":
    m = MemoryMonitor()
    start = time.time()
    # Run for 3 hours
    initial_used_gb = m.get_memory_usage()[0]
    while time.time() - start < 3600 * 3:
        print(f"{round((time.time() - start) / 60, 2)}m passed...")
        m.raise_if_low_memory()
        used_gb = m.get_memory_usage()[0]
        print("Used GB: ", used_gb)
        print(get_top_n_memory_usage())
        print("\n\n")
        time.sleep(15)
    ending_used_gb = m.get_memory_usage()[0]

    mem_growth = ending_used_gb - initial_used_gb
    print(get_top_n_memory_usage())
    print(f"Memory growth: {mem_growth}")

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
