import json
import os
import ray
import ray._private.test_utils as test_utils
import time
import tqdm

if "SMOKE_TEST" in os.environ:
    MAX_ACTORS_IN_CLUSTER = 100
else:
    MAX_ACTORS_IN_CLUSTER = 10000


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

    not_ready = [actor.foo.remote() for actor in actors]

    for _ in tqdm.trange(len(actors)):
        ready, not_ready = ray.wait(not_ready)
        assert ray.get(*ready) is None


def no_resource_leaks():
    return ray.available_resources() == ray.cluster_resources()


ray.init(address="auto")

test_utils.wait_for_condition(no_resource_leaks)
start_time = time.time()
test_max_actors()
end_time = time.time()
test_utils.wait_for_condition(no_resource_leaks)

rate = MAX_ACTORS_IN_CLUSTER / (end_time - start_time)

print(f"Success! Started {MAX_ACTORS_IN_CLUSTER} actors in "
      f"{end_time - start_time}s. ({rate} actors/s)")

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "actors_per_second": rate,
        "num_actors": MAX_ACTORS_IN_CLUSTER,
        "time": end_time - start_time,
        "success": "1"
    }
    json.dump(results, out_file)
