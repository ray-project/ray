import ray
import ray._private.test_utils as test_utils
import time
import tqdm

MAX_ACTORS_IN_CLUSTER = 10000

def test_max_actors():
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


addr = ray.init(address="auto")

for _ in range(100):
    start_time = time.time()
    test_max_actors()
    end_time = time.time()

    # Get the result
    rate = MAX_ACTORS_IN_CLUSTER / (end_time - start_time)

    print(
        f"Success! Started {MAX_ACTORS_IN_CLUSTER} actors in "
        f"{end_time - start_time}s. ({rate} actors/s)"
    )
