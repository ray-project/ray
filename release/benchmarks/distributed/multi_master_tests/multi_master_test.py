import argparse
import os
from time import sleep, perf_counter
import json
import ray


def test_max_actors_launch(cpus_per_actor, total_actors):
    # Each group has 1 master and 99 slaves.
    num_masters = int(total_actors / 100)
    num_slaves_per_master = 99

    @ray.remote(num_cpus=cpus_per_actor)
    class Actor:
        def foo(self):
            pass

        def create(self):
            return [
                Actor.options(max_restarts=-1).remote()
                for _ in range(num_slaves_per_master)
            ]

    print("Start launch actors")
    # The masters are spreaded.
    actors = [
        Actor.options(max_restarts=-1, scheduling_strategy="SPREAD").remote()
        for _ in range(num_masters)
    ]
    slaves_per_master = []
    for master in actors:
        # Each master creates 99 slaves.
        slaves_per_master.append(master.create.remote())
    for slaves in slaves_per_master:
        actors.extend(ray.get(slaves))
    return actors


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpus-per-actor", type=float, default=0.2)
    parser.add_argument("--total-actors", nargs="+", type=int, required=True)
    parser.add_argument("--no-report", default=False, action="store_true")
    parser.add_argument("--no-wait", default=False, action="store_true")
    return parser.parse_known_args()


def run_one(total_actors, cpus_per_actor, no_wait):
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
        result[f"multi_master_tests_{i}"] = run_one(
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

        print(f"Writing data into file: {os.environ['TEST_OUTPUT_JSON']}")
        json.dump(result, out_file)
    print(f"Result: {json.dumps(result, indent=2)}")
    print("Test finished successfully!")
    ray.shutdown()

    # We need to make sure GCS cool down otherwise, testing infra
    # might get timeout when fetching the result.
    print("Sleep for 60s, waiting for the cluster to cool down.")
    sleep(60)


if __name__ == "__main__":
    main()
