import argparse
import os
from time import sleep, perf_counter
import json
import ray


def test_max_actors_launch(cpus_per_actor, total_actors):
    @ray.remote(num_cpus=cpus_per_actor)
    class Actor:
        def foo(self):
            pass

    print("Start launch actors")
    actors = [Actor.options(max_restarts=-1).remote() for _ in range(total_actors)]
    return actors


def test_actor_ready(actors):
    remaining = [actor.foo.remote() for actor in actors]
    ray.get(remaining)


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpus-per-actor", type=float, default=0.2)
    parser.add_argument("--total-actors", type=int, default=5000)
    parser.add_argument("--no-report", default=False, action="store_true")
    parser.add_argument("--fail", default=False, action="store_true")
    return parser.parse_known_args()


def scale_and_wait_until_cluster_ready(total_cpus):
    import math

    total_cpus = int(math.floor(total_cpus))

    ray.autoscaler.sdk.request_resources(num_cpus=total_cpus)
    # Wait until the cluster is ready
    # ray.nodes()[0]['Resources']['CPU']
    curr_cpus = 0
    while curr_cpus < total_cpus:
        curr_cpus = sum(
            node.get("Resources", {}).get("CPU", 0)
            for node in ray.nodes()
            if node["Alive"]
        )
        print(f"Waiting for the cluster to be ready: {curr_cpus}/{total_cpus}")
        sleep(5)


def main():
    args, unknown = parse_script_args()

    ray.init(address="auto")
    total_cpus = args.cpus_per_actor * args.total_actors
    scale_and_wait_until_cluster_ready(total_cpus)
    actor_launch_start = perf_counter()
    actors = test_max_actors_launch(args.cpus_per_actor, args.total_actors)
    actor_launch_end = perf_counter()
    actor_launch_time = actor_launch_end - actor_launch_start

    if args.fail:
        sleep(10)
        return
    actor_ready_start = perf_counter()
    test_actor_ready(actors)
    actor_ready_end = perf_counter()
    actor_ready_time = actor_ready_end - actor_ready_start

    print(f"Actor launch time: {actor_launch_time} ({args.total_actors} actors)")
    print(f"Actor ready time: {actor_ready_time} ({args.total_actors} actors)")
    print(
        f"Total time: {actor_launch_time + actor_ready_time}"
        f" ({args.total_actors} actors)"
    )

    if "TEST_OUTPUT_JSON" in os.environ and not args.no_report:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {
            "actor_launch_time": actor_launch_time,
            "actor_ready_time": actor_ready_time,
            "total_time": actor_launch_time + actor_ready_time,
            "num_actors": args.total_actors,
            "success": "1",
        }
        json.dump(results, out_file)


if __name__ == "__main__":
    main()
