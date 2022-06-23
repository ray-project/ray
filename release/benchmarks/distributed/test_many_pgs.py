import json
import os
import ray
import ray._private.test_utils as test_utils
from ray.util.placement_group import placement_group, remove_placement_group
import time
import tqdm

is_smoke_test = True
if "SMOKE_TEST" in os.environ:
    MAX_PLACEMENT_GROUPS = 20
else:
    MAX_PLACEMENT_GROUPS = 1000
    is_smoke_test = False


def test_many_placement_groups():
    # @ray.remote(num_cpus=1, resources={"node": 0.02})
    @ray.remote
    class C1:
        def ping(self):
            return "pong"

    # @ray.remote(num_cpus=1)
    @ray.remote
    class C2:
        def ping(self):
            return "pong"

    # @ray.remote(resources={"node": 0.02})
    @ray.remote
    class C3:
        def ping(self):
            return "pong"

    bundle1 = {"node": 0.02, "CPU": 1}
    bundle2 = {"CPU": 1}
    bundle3 = {"node": 0.02}

    pgs = []
    for _ in tqdm.trange(MAX_PLACEMENT_GROUPS, desc="Creating pgs"):
        pg = placement_group(bundles=[bundle1, bundle2, bundle3])
        pgs.append(pg)

    for pg in tqdm.tqdm(pgs, desc="Waiting for pgs to be ready"):
        ray.get(pg.ready())

    actors = []
    for pg in tqdm.tqdm(pgs, desc="Scheduling tasks"):
        actors.append(C1.options(placement_group=pg).remote())
        actors.append(C2.options(placement_group=pg).remote())
        actors.append(C3.options(placement_group=pg).remote())

    not_ready = [actor.ping.remote() for actor in actors]
    for _ in tqdm.trange(len(actors)):
        ready, not_ready = ray.wait(not_ready)
        assert ray.get(*ready) == "pong"

    for pg in tqdm.tqdm(pgs, desc="Cleaning up pgs"):
        remove_placement_group(pg)


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


ray.init(address="auto")

test_utils.wait_for_condition(no_resource_leaks)
monitor_actor = test_utils.monitor_memory_usage()
start_time = time.time()
test_many_placement_groups()
end_time = time.time()
ray.get(monitor_actor.stop_run.remote())
used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())
print(f"Peak memory usage: {round(used_gb, 2)}GB")
print(f"Peak memory usage per processes:\n {usage}")
del monitor_actor
test_utils.wait_for_condition(no_resource_leaks)

rate = MAX_PLACEMENT_GROUPS / (end_time - start_time)
print(
    f"Success! Started {MAX_PLACEMENT_GROUPS} pgs in "
    f"{end_time - start_time}s. ({rate} pgs/s)"
)

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "pgs_per_second": rate,
        "num_pgs": MAX_PLACEMENT_GROUPS,
        "time": end_time - start_time,
        "success": "1",
        "_peak_memory": round(used_gb, 2),
        "_peak_process_memory": usage,
    }
    if not is_smoke_test:
        results["perf_metrics"] = [
            {
                "perf_metric_name": "pgs_per_second",
                "perf_metric_value": rate,
                "perf_metric_type": "THROUGHPUT",
            }
        ]
    json.dump(results, out_file)
