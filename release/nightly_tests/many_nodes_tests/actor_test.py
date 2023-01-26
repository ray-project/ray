import argparse
import os
import math
from time import sleep, perf_counter
import json
import ray
import psutil


def test_max_actors_launch(cpus_per_actor, total_actors):
    @ray.remote(num_cpus=cpus_per_actor)
    class Actor:
        def foo(self):
            pass

    print("Start launch actors")
    actors = [Actor.options(max_restarts=-1).remote() for _ in range(total_actors)]
    return actors


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpus-per-actor", type=float, default=0.2)
    parser.add_argument("--total-actors", type=int, default=5000)
    parser.add_argument("--no-report", default=False, action="store_true")
    parser.add_argument("--fail", default=False, action="store_true")
    return parser.parse_known_args()


def wait_until_nodes_ready(target):
    while True:
        curr_cpus = sum([r.get("Resources", {}).get("CPU", 0) for r in ray.nodes()])
        if curr_cpus >= target:
            return
        print(f"Waiting nodes to be ready: {curr_cpus}/{target}")
        sleep(5)


def scale_cluster_up(num_cpus):
    print(f"Start to scale up to {num_cpus} cpus")
    step = 1000
    curr_cpus = int(sum([r.get("Resources", {}).get("CPU", 0) for r in ray.nodes()]))
    for i in range(curr_cpus, num_cpus, step):
        target = min(num_cpus, i + step)
        print(f"Start to scale up to {target} cpus")
        ray.autoscaler.sdk.request_resources(num_cpus=target)
        wait_until_nodes_ready(target)


def main():
    args, unknown = parse_script_args()

    ray.init(address="auto")
    total_cpus = args.cpus_per_actor * args.total_actors + psutil.cpu_count()
    total_cpus = int(math.ceil(total_cpus))
    scale_cluster_up(total_cpus)

    actor_launch_start = perf_counter()
    actors = test_max_actors_launch(args.cpus_per_actor, args.total_actors)
    actor_launch_end = perf_counter()
    actor_launch_time = actor_launch_end - actor_launch_start
    if args.fail:
        sleep(10)
        return
    actor_ready_start = perf_counter()
    total_actors = len(actors)
    objs = [actor.foo.remote() for actor in actors]

    while len(objs) != 0:
        objs_ready, objs = ray.wait(objs, timeout=10)
        print(
            f"Status: {total_actors - len(objs)}/{total_actors}, "
            f"{perf_counter() - actor_ready_start}"
        )
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
