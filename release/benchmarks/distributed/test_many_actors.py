import json
import os
import ray
import ray._private.test_utils as test_utils
import time
import tqdm

from dashboard_test import DashboardTestAtScale

is_smoke_test = True
if "SMOKE_TEST" in os.environ:
    MAX_ACTORS_IN_CLUSTER = 100
else:
    MAX_ACTORS_IN_CLUSTER = 10000
    is_smoke_test = False


def test_max_actors():
    # TODO (Alex): Dynamically set this based on number of cores
    cpus_per_actor = 0.25

    @ray.remote(num_cpus=cpus_per_actor)
    class Actor:
        def foo(self):
            pass

    actors = [
        Actor.remote()
        for _ in tqdm.trange(MAX_ACTORS_IN_CLUSTER, desc="Launching actors")
    ]

    done = ray.get([actor.foo.remote() for actor in actors])
    for result in done:
        assert result is None


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


addr = ray.init(address="auto")

test_utils.wait_for_condition(no_resource_leaks)
monitor_actor = test_utils.monitor_memory_usage()
dashboard_test = DashboardTestAtScale(addr)

start_time = time.time()
test_max_actors()
end_time = time.time()

ray.get(monitor_actor.stop_run.remote())
used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
print(f"Peak memory usage: {round(used_gb, 2)}GB")
print(f"Peak memory usage per processes:\n {usage}")
del monitor_actor

# Get the dashboard result
test_utils.wait_for_condition(no_resource_leaks)

rate = MAX_ACTORS_IN_CLUSTER / (end_time - start_time)
print(
    f"Success! Started {MAX_ACTORS_IN_CLUSTER} actors in "
    f"{end_time - start_time}s. ({rate} actors/s)"
)

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "actors_per_second": rate,
        "num_actors": MAX_ACTORS_IN_CLUSTER,
        "time": end_time - start_time,
        "success": "1",
        "_peak_memory": round(used_gb, 2),
        "_peak_process_memory": usage,
    }
    if not is_smoke_test:
        results["perf_metrics"] = [
            {
                "perf_metric_name": "actors_per_second",
                "perf_metric_value": rate,
                "perf_metric_type": "THROUGHPUT",
            }
        ]
    dashboard_test.update_release_test_result(results)
    json.dump(results, out_file)
