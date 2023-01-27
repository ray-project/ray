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
    parser.add_argument("--total-actors", nargs="+", type=int, required=True)
    parser.add_argument("--no-report", default=False, action="store_true")
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
            ray.autoscaler.sdk.request_resources(num_cpus=target_cpus)
        print(f"Waiting for cluster to be up: {curr_cpus}->{target_cpus}->{num_cpus}")
        sleep(10)


def run_one(total_actors, cpus_per_actor, no_wait):
    total_cpus = cpus_per_actor * total_actors + psutil.cpu_count()
    total_cpus = int(math.ceil(total_cpus))
    scale_cluster_up(total_cpus)

    actor_launch_start = perf_counter()
    actors = test_max_actors_launch(cpus_per_actor, total_actors)
    actor_launch_end = perf_counter()
    actor_launch_time = actor_launch_end - actor_launch_start
    actor_ready_start = perf_counter()
    total_actors = len(actors)
    objs = [actor.foo.remote() for actor in actors]

    while len(objs) != 0:
        timeout = None if no_wait else 30
        objs_ready, objs = ray.wait(objs, num_returns=len(objs), timeout=timeout)
        print(
            f"Status: {total_actors - len(objs)}/{total_actors}, "
            f"{perf_counter() - actor_ready_start}"
        )
    actor_ready_end = perf_counter()
    actor_ready_time = actor_ready_end - actor_ready_start

    throughput = total_actors / (actor_ready_time + actor_launch_time)
    print(f"Actor launch time: {actor_launch_time} ({total_actors} actors)")
    print(f"Actor ready time: {actor_ready_time} ({total_actors} actors)")
    print(
        f"Total time: {actor_launch_time + actor_ready_time}"
        f" ({total_actors} actors)"
    )
    print(f"Through put: {throughput}")

    return {
        "actor_launch_time": actor_launch_time,
        "actor_ready_time": actor_ready_time,
        "total_time": actor_launch_time + actor_ready_time,
        "num_actors": total_actors,
        "success": "1",
        "throughput": throughput,
    }


def main():
    args, unknown = parse_script_args()
    args.total_actors.sort()

    ray.init(address="auto")

    dashboard_test = None
    # Enable it once v2 support prometheus
    # dashboard_test = DashboardTestAtScale(addr)
    result = {}
    for i in args.total_actors:
        result[f"many_nodes_actor_tests_{i}"] = run_one(
            i, args.cpus_per_actor, args.no_wait
        )

    if "TEST_OUTPUT_JSON" in os.environ and not args.no_report:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        if dashboard_test is not None:
            perf = [
                {
                    "perf_metric_name": name,
                    "perf_metric_value": r["throughput"],
                    "perf_metric_type": "THROUGHPUT",
                }
                for (name, r) in result.items()
            ]
            result["perf_metrics"] = perf
            dashboard_test.update_release_test_result(result)

        json.dump(result, out_file)


if __name__ == "__main__":
    main()
