"""Times placement group creation and actor-into-PG scheduling throughput, at scale.

Each bundle is a whole worker node; actors-per-node actors are scheduled into each.

pg-per-node: one single-bundle PG per worker node.

pg-per-rack: one PG per rack, one bundle per node in the rack.
"""
import argparse
import time

from many_nodes_tests.dashboard_test import DashboardTestAtScale

import ray
import ray._common.test_utils
import ray._private.test_utils as test_utils
import ray.autoscaler.sdk
from ray.util.placement_group import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

RACK_LABEL = "ray.io/gpu-domain"

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["pg-per-node", "pg-per-rack"], required=True)
parser.add_argument("--node-counts", nargs="+", type=int, default=[])
parser.add_argument("--actors-per-node", type=int, default=4)
args, _ = parser.parse_known_args()


def no_resource_leaks():
    return test_utils.no_resource_leaks_excluding_node_resources()


def worker_nodes():
    head_node_id = ray.get_runtime_context().get_node_id()
    nodes = [n for n in ray.nodes() if n["Alive"] and n["NodeID"] != head_node_id]
    return nodes or [n for n in ray.nodes() if n["Alive"]]


def total_cluster_cpus():
    return int(sum(n["Resources"].get("CPU", 0) for n in ray.nodes() if n["Alive"]))


def scale_up_to(num_nodes, cpu_per_node):
    head_cpus = total_cluster_cpus() - len(worker_nodes()) * cpu_per_node
    ray.autoscaler.sdk.request_resources(num_cpus=head_cpus + num_nodes * cpu_per_node)
    while len(worker_nodes()) < num_nodes:
        print(f"Waiting for {len(worker_nodes())}/{num_nodes} worker nodes")
        time.sleep(5)


def measure(num_pgs, bundles_per_pg, pg_kwargs, cpu_per_node):
    @ray.remote(num_cpus=cpu_per_node / args.actors_per_node)
    class Actor:
        def ping(self):
            return "pong"

    start = time.time()
    pgs = [placement_group(**pg_kwargs) for _ in range(num_pgs)]
    ray.get([pg.ready() for pg in pgs])
    pg_create_throughput = num_pgs / (time.time() - start)

    start = time.time()
    actors = [
        Actor.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
        for pg in pgs
        for _ in range(bundles_per_pg * args.actors_per_node)
    ]
    ray.get([a.ping.remote() for a in actors])
    actor_throughput = len(actors) / (time.time() - start)

    result = {
        "mode": args.mode,
        "num_pgs": num_pgs,
        "bundles_per_pg": bundles_per_pg,
        "num_actors": len(actors),
        "pg_create_throughput": pg_create_throughput,
        "actor_throughput": actor_throughput,
    }
    for pg in pgs:
        remove_placement_group(pg)
    ray._common.test_utils.wait_for_condition(no_resource_leaks)
    return result


def run_benchmark():
    cpu_per_node = int(worker_nodes()[0]["Resources"]["CPU"])
    bundle = {"CPU": cpu_per_node}
    results = {}
    if args.mode == "pg-per-node":
        for n in sorted(args.node_counts):
            scale_up_to(n, cpu_per_node)
            results[f"pg_per_node_{n}"] = measure(
                n, 1, dict(bundles=[bundle]), cpu_per_node
            )
    else:
        workers = worker_nodes()
        num_racks = len({n["Labels"][RACK_LABEL] for n in workers})
        bundles_per_pg = len(workers) // num_racks
        results[f"pg_per_rack_{num_racks}"] = measure(
            num_racks,
            bundles_per_pg,
            dict(
                bundles=[bundle] * bundles_per_pg,
                topology_strategy={RACK_LABEL: "STRICT_PACK"},
            ),
            cpu_per_node,
        )
    return results


addr = ray.init(address="auto")

dashboard_test = DashboardTestAtScale(addr)

results = run_benchmark()

print(f"Result: {results}")
perf_metrics = []
for name, r in results.items():
    perf_metrics.append(
        {
            "perf_metric_name": f"{name}_pg_create_throughput",
            "perf_metric_value": r["pg_create_throughput"],
            "perf_metric_type": "THROUGHPUT",
        }
    )
    perf_metrics.append(
        {
            "perf_metric_name": f"{name}_actor_throughput",
            "perf_metric_value": r["actor_throughput"],
            "perf_metric_type": "THROUGHPUT",
        }
    )
results["perf_metrics"] = perf_metrics
dashboard_test.update_release_test_result(results)
test_utils.safe_write_to_results_json(results)
