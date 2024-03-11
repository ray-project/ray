import ray
import time
import tqdm

from ray.util.metrics import Counter, Gauge, Histogram
import ray._private.ray_constants as ray_constants
from ray.util.state import list_actors, list_jobs, list_tasks, list_nodes, list_objects, list_workers
from ray.scripts.scripts import status

MAX_ACTORS_IN_CLUSTER = 10000

error_count = Counter(
    "bm_num_errors",
    description="Number of errors that have occurred in the benchmark.",
)

# status_latency = Histogram(
#     "bm_status_latency",
#     description="Time in ms taken to execute ray status command.",
#     boundaries=[100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200],
# )

list_jobs_latency = Histogram(
    "bm_list_jobs_latency",
    description="Time in ms taken to execute ray list jobs command.",
    boundaries=[100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200],
)

list_actors_latency = Histogram(
    "bm_list_actors_latency",
    description="Time in ms taken to execute ray list actors command.",
    boundaries=[100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200],
)

list_tasks_latency = Histogram(
    "bm_list_tasks_latency",
    description="Time in ms taken to execute ray list tasks command.",
    boundaries=[100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200],
)

list_nodes_latency = Histogram(
    "bm_list_nodes_latency",
    description="Time in ms taken to execute ray list nodes command.",
    boundaries=[100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200],
)

list_workers_latency = Histogram(
    "bm_list_workers_latency",
    description="Time in ms taken to execute ray list workers command.",
    boundaries=[100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200],
)

list_objects_latency = Histogram(
    "bm_list_objects_latency",
    description="Time in ms taken to execute ray list objects command.",
    boundaries=[100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200],
)

addr = ray.init(address="auto")

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
    
    refs = [actor.foo.remote() for actor in actors]
    
    # Once a batch of actors is done being called, they will be garbage collected
    del actors

    while refs:
        done, refs = ray.wait(refs, num_returns=1000)
        for result in done:
            assert ray.get(result) is None
        
        print("=====================================")
        print("done:", len(done), "left:", len(refs))
        try:
            observe_state()
        except Exception as e:
            print("Error:", e)
            error_count.inc()
            continue


def observe_state():
    # start_time = time.time()
    # res = status()
    # status_latency.observe(1000*(time.time() - start_time))
    # print("status output:", len(res))
    
    start_time = time.time()
    res = list_jobs(raise_on_missing_output=False)
    list_jobs_latency.observe(1000*(time.time() - start_time))
    print("list_jobs output:", len(res))
    
    start_time = time.time()
    res = list_actors(raise_on_missing_output=False)
    list_actors_latency.observe(1000*(time.time() - start_time))
    print("list_actors output:",len(res))
    
    start_time = time.time()
    res = list_tasks(raise_on_missing_output=False)
    list_tasks_latency.observe(1000*(time.time() - start_time))
    print("list_tasks output:",len(res))
    
    start_time = time.time()
    res = list_nodes(raise_on_missing_output=False)
    list_nodes_latency.observe(1000*(time.time() - start_time))
    print("list_nodes output:",len(res))
    
    start_time = time.time()
    res = list_workers(raise_on_missing_output=False)
    list_workers_latency.observe(1000*(time.time() - start_time))
    print("list_workers output:",len(res))
    
    start_time = time.time()
    res = list_objects(raise_on_missing_output=False)
    list_objects_latency.observe(1000*(time.time() - start_time))
    print("list_objects output:",len(res))
    

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
