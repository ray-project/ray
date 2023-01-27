import argparse
import os
import math
from time import sleep, perf_counter
import json
import ray
import psutil

from dashboard_test import DashboardTestAtScale


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
    parser.add_argument("--no-wait", default=False, action="store_true")
    return parser.parse_known_args()


def scale_cluster_up(num_cpus):
    print(f"Start to scale up to {num_cpus} cpus")
    def get_curr_cpus():
        return int(sum([r.get("Resources", {}).get("CPU", 0) for r in ray.nodes()]))
    step = 1000
    curr_cpus = get_curr_cpus()
    target_cpus = curr_cpus

    while curr_cpus < num_cpus:
        curr_cpus = get_curr_cpus()
        new_target_cpus = min(curr_cpus + step, num_cpus)
        if new_target_cpus != target_cpus:
            target_cpus = new_target_cpus
            ray.autoscaler.sdk.request_resources(num_cpus=target)
        print(f"Waiting for cluster to be up: {curr_cpus}->{target_cpus}->{num_cpus}")
        sleep(10)


def main():
    args, unknown = parse_script_args()

    addr = ray.init(address="auto")
    dashboard_test = DashboardTestAtScale(addr)

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
        timeout = None if args.no_wait else 30
        objs_ready, objs = ray.wait(objs, timeout=timeout)
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
        rate = args.total_actors / (actor_ready_time + actor_launch_time)
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {
            "actor_launch_time": actor_launch_time,
            "actor_ready_time": actor_ready_time,
            "total_time": actor_launch_time + actor_ready_time,
            "num_actors": args.total_actors,
            "success": "1",
        }
        results["perf_metrics"] = [
            {
                "perf_metric_name": f"actors_per_second_{args.total_actors}",
                "perf_metric_value": rate,
                "perf_metric_type": "THROUGHPUT",
            }
        ]
        dashboard_test.update_release_test_result(results)

        json.dump(results, out_file)


if __name__ == "__main__":
    main()
