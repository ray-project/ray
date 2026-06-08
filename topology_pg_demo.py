"""Demo: a placement group using topology_strategy, parked for inspection.

Spins up a small in-process Ray cluster with two GPU domains (rack-1, rack-2),
creates a placement group that uses ``topology_strategy``, prints where each
bundle landed, and blocks so you can inspect it from another terminal with:

    ray list placement-groups --detail

Run:
    python ~/topology_pg_demo.py

In another terminal, run the command above. The detail output will show the
new ``topology_strategy`` and ``topology_assignments`` fields populated for
this placement group.

Ctrl-C to tear down.

Prereq: Ray rebuilt from this branch so that ``topology_strategy`` exists on
``placement_group(...)`` and the proto-rename + state-API rename are in
effect. If you get ``TypeError`` on the placement_group call, your Ray build
is stale -- run the C++/Cython rebuild first.
"""

import time

import ray
from ray.cluster_utils import Cluster
from ray.util.placement_group import placement_group, placement_group_table

GPU_DOMAIN = "ray.io/gpu-domain"

# Port the demo's dashboard binds to. 8265 is the Ray default; the demo uses
# 8266 instead so it doesn't collide with another Ray cluster already using
# 8265 on this host. Set to 0 to let the OS pick any free port -- the `ray`
# CLI will still find the dashboard via GCS internal KV regardless.
DASHBOARD_PORT = 8266


def main():
    cluster = Cluster()
    # Head node, no compute. include_dashboard=True is required so that
    # `ray list placement-groups --detail` from another terminal can resolve
    # the dashboard URL via GCS internal KV; without it the CLI spins in a
    # ~20-second retry loop before erroring with "Couldn't obtain the API
    # server address from GCS".
    cluster.add_node(
        num_cpus=0,
        include_dashboard=True,
    )

    # Two GPU domains, two 2-CPU worker nodes each. Total 4 candidate nodes,
    # 8 CPUs. Enough slack to land a 4-bundle PG either packed or spread,
    # and enough to demonstrate the domain pinning.
    for domain in ("rack-1", "rack-2"):
        for _ in range(4):
            cluster.add_node(num_cpus=4, labels={GPU_DOMAIN: domain})

    ray.init(address=cluster.address)

    bundles = [{"CPU": 1}] * 4
    pg = placement_group(
        bundles=bundles,
        topology_strategy=[
            {
                # Node-level: PACK -- cram bundles onto as few nodes as
                # possible within the chosen domain.
                "ray.io/node-id": "STRICT_SPREAD",
                # Domain-level: STRICT_PACK -- all bundles must share a single
                # ray.io/gpu-domain value.
                GPU_DOMAIN: "STRICT_PACK",
            }
        ],
    )
    ray.get(pg.ready(), timeout=30)

    info = placement_group_table(pg)
    node_labels = {n["NodeID"]: n["Labels"] for n in ray.nodes()}

    print("\n=== Placement group ready ===")
    print(f"  id:    {info['placement_group_id']}")
    print(f"  name:  {info['name'] or '(unnamed)'}")
    print(f"  state: {info['state']}")
    print(f"  strategy (proto-level): {info['strategy']}")
    print(f"  bundles: {len(info['bundles'])}")
    selected_domains = set()
    for bundle_id, node_id in sorted(info["bundles_to_node_id"].items()):
        labels = node_labels.get(node_id, {})
        domain = labels.get(GPU_DOMAIN, "<no-domain>")
        selected_domains.add(domain)
        print(
            f"    bundle {bundle_id}  ->  node {node_id[:8]}...  ({GPU_DOMAIN}={domain})"
        )
    print(f"  domain selection: {selected_domains}")
    print()
    print("Now run this from another terminal:")
    print(f"    RAY_API_SERVER_ADDRESS=http://127.0.0.1:{DASHBOARD_PORT} \\")
    print("        ray list placement-groups --detail")
    print()
    print("(Or omit RAY_API_SERVER_ADDRESS -- the `ray` CLI will discover")
    print(" the dashboard port via GCS internal KV automatically.)")
    print()
    print("Look for these new fields on the placement group entry:")
    print("    topology_strategy:    [{entries: {ray.io/gpu-domain: STRICT_PACK}}]")
    print(f"    topology_assignments: [{{assignments: {{{GPU_DOMAIN}: <selected>}}}}]")
    print()
    print("Press Ctrl-C to tear down.")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()
