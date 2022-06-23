import argparse
import os
import json
import logging

from time import perf_counter

import ray

from ray.util.placement_group import placement_group, remove_placement_group
from ray._private.test_utils import wait_for_condition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_trial(total_stage, num_pg_per_stage):
    creating_e2e_s = []
    removing_e2e_s = []
    # Create and remove placement groups.
    for i in range(total_stage):
        # Create pgs.
        pgs = []
        start = perf_counter()
        for _ in range(num_pg_per_stage):
            pgs.append(
                placement_group(
                    bundles=[{"custom": 0.025} for _ in range(4)], strategy="PACK"
                )
            )
        logger.info(f"Created {num_pg_per_stage} pgs.")
        ray.get([pg.ready() for pg in pgs])
        end = perf_counter()
        total_creating_time = end - start
        logger.info(
            f"Creating {num_pg_per_stage} took "
            f"{total_creating_time} seconds at stage {i}"
        )
        creating_e2e_s.append(total_creating_time * 1000.0)

        # Remove pgs
        start = perf_counter()
        for _, pg in enumerate(pgs):
            remove_placement_group(pg)
        end = perf_counter()
        total_removal_time = end - start
        logger.info(
            f"removed {num_pg_per_stage} pgs took "
            f"{total_removal_time} seconds at stage {i}"
        )
        removing_e2e_s.append(total_removal_time * 1000.0)
        # time.sleep(1)

    # Calculate the scheduling latency (excluding queueing time).
    latencies = []
    for entry in ray.util.placement_group_table().values():
        latency = entry["stats"]["scheduling_latency_ms"]
        latencies.append(latency)
    latencies = sorted(latencies)
    removing_e2e_s = sorted(removing_e2e_s)
    creating_e2e_s = sorted(creating_e2e_s)

    def get_scheduling_perf(latencies):
        """Return P10, 50, 95, 99 latency"""
        p10 = latencies[int(len(latencies) * 0.1)]
        p50 = latencies[int(len(latencies) * 0.5)]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        return {"p10_ms": p10, "p50_ms": p50, "p95_ms": p95, "p99_ms": p99}

    scheduling_perf = get_scheduling_perf(latencies)
    removing_perf = get_scheduling_perf(removing_e2e_s)
    creation_perf = get_scheduling_perf(creating_e2e_s)

    wait_for_condition(
        lambda: (
            ray.cluster_resources()["custom"] == ray.available_resources()["custom"]
        ),
        timeout=30,
    )
    wait_for_condition(
        lambda: (
            ray.cluster_resources()["pending"] == ray.available_resources()["pending"]
        ),
        timeout=30,
    )

    return scheduling_perf, removing_perf, creation_perf


def parse_script_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-pgs-stage", type=int, default=100)
    parser.add_argument("--num-stages", type=int, default=100)
    parser.add_argument("--num-pending-pgs", type=int, default=0)
    parser.add_argument("--local", action="store_true", default=False)
    return parser.parse_known_args()


def main():
    """Run a long running placement group creation/removal tests.

    This test runs 20 trials first and measure the P50 performance.

    After that it runs trials for a long time and make sure the
    P50 creation/scheduling/removal performance is not regressed
    after the long running job.
    """
    args, _ = parse_script_args()
    NUM_PG_AT_EACH_STAGE = args.num_pgs_stage
    NUM_PENDING_PG = args.num_pending_pgs
    TOTAL_STAGE = args.num_stages

    if args.local:
        ray.init(resources={"custom": 100, "pending": 1})
    else:
        ray.init(address="auto")

    assert ray.cluster_resources()["custom"] >= NUM_PG_AT_EACH_STAGE * 4
    assert ray.cluster_resources()["pending"] >= 1

    # Create pending placement groups.
    pending_pgs = []
    for _ in range(NUM_PENDING_PG):
        # Right now, we don't have infeasible pgs,
        # so this will simulate the pending pgs.
        pending_pgs.append(placement_group([{"pending": 1}], strategy="PACK"))

    (scheduling_perf, removing_perf, creation_perf) = run_trial(
        20, NUM_PG_AT_EACH_STAGE
    )
    (scheduling_perf_final, removing_perf_final, creation_perf_final) = run_trial(
        TOTAL_STAGE, NUM_PG_AT_EACH_STAGE
    )

    print(f"Scheduling performance 20 trials: {scheduling_perf}")
    print(f"Scheduling performance {TOTAL_STAGE} trials: {scheduling_perf_final}")
    print(f"Removal performance 20 trials: {removing_perf}")
    print(f"Removal performance {TOTAL_STAGE} trials: {removing_perf_final}")
    print(f"Creation performance 20 trials: {creation_perf}")
    print(f"Creation performance {TOTAL_STAGE} trials: {creation_perf_final}")

    assert scheduling_perf["p50_ms"] * 100 > scheduling_perf_final["p50_ms"]
    assert removing_perf["p50_ms"] * 100 > removing_perf_final["p50_ms"]
    assert creation_perf["p50_ms"] * 100 > creation_perf_final["p50_ms"]

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {}
        json.dump(results, out_file)


main()
