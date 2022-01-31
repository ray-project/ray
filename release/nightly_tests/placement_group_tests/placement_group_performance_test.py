import argparse
import json
import os

import ray
from ray._private.ray_microbenchmark_helpers import timeit

RESOURCES_VALUE = 0.01


def test_placement_group_perf(num_pgs, num_bundles, num_pending_pgs):
    # Run the placement group performance benchmark given arguments.
    assert ray.cluster_resources()["custom"] >= (
        RESOURCES_VALUE * num_pgs * num_bundles
    )

    def placement_group_create(num_pgs):
        pgs = [
            ray.util.placement_group(
                bundles=[{"custom": 0.001} for _ in range(num_bundles)],
                strategy="SPREAD",
            )
            for _ in range(num_pgs)
        ]
        [pg.wait(timeout_seconds=30) for pg in pgs]
        for pg in pgs:
            ray.util.remove_placement_group(pg)

    print(
        f"Num pending pgs: {num_pending_pgs}, "
        f"Num pgs: {num_pgs}, "
        f"Num bundles {num_bundles}"
    )

    # Get the throughput.
    throughput = timeit(
        "placement group create per second",
        lambda: placement_group_create(num_pgs),
        num_pgs,
    )

    # Get fine-grained scheduling stats.
    latencies = []
    e2e_latencies = []
    scheduling_attempts = []
    for entry in ray.util.placement_group_table().values():
        latency = entry["stats"]["scheduling_latency_ms"]
        e2e_latency = entry["stats"]["end_to_end_creation_latency_ms"]
        scheduling_attempt = entry["stats"]["scheduling_attempt"]
        latencies.append(latency)
        e2e_latencies.append(e2e_latency)
        scheduling_attempts.append(scheduling_attempt)
    latencies = sorted(latencies)
    e2e_latencies = sorted(e2e_latencies)
    scheduling_attempts = sorted(scheduling_attempts)

    # Pure scheduling latency without queuing time.
    print("P50 scheduling latency ms: " f"{latencies[int(len(latencies) * 0.5)]}")
    print("P95 scheduling latency ms: " f"{latencies[int(len(latencies) * 0.95)]}")
    print("P99 scheduling latency ms: " f"{latencies[int(len(latencies) * 0.99)]}")

    # Scheduling latency including queueing time.
    print(
        "P50 e2e scheduling latency ms: "
        f"{e2e_latencies[int(len(e2e_latencies) * 0.5)]}"
    )
    print(
        "P95 e2e scheduling latency ms: "
        f"{e2e_latencies[int(len(e2e_latencies) * 0.95)]}"
    )
    print(
        "P99 e2e scheduling latency ms: "
        f"{e2e_latencies[int(len(e2e_latencies) * 0.99)]}"
    )

    # Number of time scheduling was retried before succeeds.
    print(
        "P50 scheduling attempts: "
        f"{scheduling_attempts[int(len(scheduling_attempts) * 0.5)]}"
    )
    print(
        "P95 scheduling attempts: "
        f"{scheduling_attempts[int(len(scheduling_attempts) * 0.95)]}"
    )
    print(
        "P99 scheduling attempts: "
        f"{scheduling_attempts[int(len(scheduling_attempts) * 0.99)]}"
    )

    return {
        "pg_creation_per_second": throughput[0][1],
        "p50_scheduling_latency_ms": latencies[int(len(latencies) * 0.5)],
        "p50_e2e_pg_creation_latency_ms": e2e_latencies[int(len(e2e_latencies) * 0.5)],
    }


def run_full_benchmark(num_pending_pgs):
    # Run the benchmark with different num_bundles & num_pgs params.
    num_bundles = 1
    num_pgs_test = [10, 100, 200, 400, 800, 1600]
    results = []
    for num_pgs in num_pgs_test:
        results.append(test_placement_group_perf(num_pgs, num_bundles, num_pending_pgs))

    def print_result(num_pending_pgs, num_pgs, num_bundles, result):
        print(
            f"Num pending pgs: {num_pending_pgs}, "
            f"Num pgs: {num_pgs}, "
            f"Num bundles {num_bundles}"
        )
        print("Throughput: ")
        for k, v in result.items():
            print(f"\t{k}: {v}")

    for i in range(len(results)):
        num_pgs = num_pgs_test[i]
        result = results[i]
        print_result(num_pending_pgs, num_pgs, num_bundles, result)

    # Test with different length of bundles.
    num_bundles_list = [1, 10, 20, 40]
    num_pgs = 100
    results = []
    for num_bundles in num_bundles_list:
        results.append(test_placement_group_perf(num_pgs, num_bundles, num_pending_pgs))

    for i in range(len(results)):
        num_bundles = num_bundles_list[i]
        result = results[i]
        print_result(num_pending_pgs, num_pgs, num_bundles, result)


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-pgs", type=int, default=-1)
    parser.add_argument("--num-bundles", type=int, default=-1)
    parser.add_argument("--num-pending_pgs", type=int, default=0)
    parser.add_argument("--local", action="store_true")
    return parser.parse_known_args()


if __name__ == "__main__":
    args, _ = parse_script_args()
    if args.local:
        ray.init(resources={"custom": 100, "pending": 1})
    else:
        ray.init(address="auto")

    # Create pending placement groups.
    # It is used to see the impact of pending placement group.
    # Currently, pending placement groups could increase the load of
    # GCS server.
    assert ray.cluster_resources()["pending"] >= 1
    pending_pgs = [
        ray.util.placement_group(bundles=[{"pending": 1}])
        for _ in range(args.num_pending_pgs + 1)
    ]

    # If arguments are given, run a single test.
    # It is used to analyze the scheduling performance trace with
    # Grafana.
    if args.num_pgs != -1 and args.num_bundles != -1:
        test_placement_group_perf(args.num_pgs, args.num_bundles, args.num_pending_pgs)
    else:
        run_full_benchmark(args.num_pending_pgs)

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {"success": 1}
        json.dump(results, out_file)
