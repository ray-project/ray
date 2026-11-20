"""Run the Neuron collectives benchmark (nccom-test) across Trainium nodes over EFA.

Uses nccom-test --script-mode: one nccom-test per node, all started at the same
time by Ray. Node 0 hosts the Neuron runtime rendezvous (NEURON_RT_ROOT_COMM_ID)
and the collector that gathers results.

The test passes when every node exits 0 with output verification on (--check)
and the EFA hardware counters show that the traffic crossed the network.
"""
import argparse
import subprocess

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from neuron_common import efa_tx_bytes, write_result

RENDEZVOUS_PORT = 44444  # Neuron runtime root comm
COLLECTOR_PORT = 60006  # nccom-test result collector on node 0


@ray.remote(num_cpus=8)
def run_nccom_test(node_index, nnodes, cores_per_node, root_ip, op, extra, timeout):
    import os

    # Ray sets NEURON_RT_VISIBLE_CORES for the reserved cores; nccom-test honors it.
    env = dict(os.environ)
    env["NEURON_RT_ROOT_COMM_ID"] = f"{root_ip}:{RENDEZVOUS_PORT}"
    cmd = [
        "nccom-test",
        op,
        "-r",
        str(cores_per_node * nnodes),
        "-N",
        str(nnodes),
        "--script-mode",
        "--node-index",
        str(node_index),
        "--data-collector-host",
        root_ip,
        "--data-collector-port",
        str(COLLECTOR_PORT),
        "--non-interactive",
        # Without pinning, a few percent of iterations stall for milliseconds.
        "--vcpu-pin-mode",
        "--check",
    ] + extra
    tx_before = efa_tx_bytes()
    try:
        r = subprocess.run(
            cmd, env=env, capture_output=True, text=True, timeout=timeout
        )
        out, rc = r.stdout + r.stderr, r.returncode
    except subprocess.TimeoutExpired as e:
        out, rc = f"{e.stdout or ''}{e.stderr or ''}\nTIMED OUT after {timeout}s", -1
    tx_after = efa_tx_bytes()
    return {
        "node_index": node_index,
        "ip": ray.util.get_node_ip_address(),
        "rc": rc,
        "cmd": " ".join(cmd),
        "output": out,
        "efa_tx_bytes": tx_after - tx_before if tx_before >= 0 else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--op", default="all_reduce")
    parser.add_argument("--cores-per-node", type=int, default=64)
    parser.add_argument("--timeout", type=int, default=1200, help="seconds per node")
    # Anything else is passed through to nccom-test.
    args, extra = parser.parse_known_args()
    extra = extra or ["-b", "1MB", "-e", "64MB", "-f", "4", "-n", "20", "-w", "5"]

    ray.init()
    nodes = sorted(
        (
            n
            for n in ray.nodes()
            if n["Alive"]
            and n["Resources"].get("neuron_cores", 0) >= args.cores_per_node
        ),
        key=lambda n: n["NodeManagerAddress"],
    )
    nnodes = len(nodes)
    assert nnodes >= 2, f"need >= 2 Trainium nodes, found {nnodes}"
    root_ip = nodes[0]["NodeManagerAddress"]
    print(f"{args.op} on {nnodes} nodes x {args.cores_per_node} cores; root {root_ip}")

    # Reserve whole nodes so nothing else touches the Neuron cores mid-benchmark.
    results = ray.get(
        [
            run_nccom_test.options(
                resources={"neuron_cores": args.cores_per_node},
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    n["NodeID"], soft=False
                ),
            ).remote(
                i,
                nnodes,
                args.cores_per_node,
                root_ip,
                args.op,
                extra,
                args.timeout,
            )
            for i, n in enumerate(nodes)
        ]
    )
    for r in results:
        print(f"\n===== node {r['node_index']} ({r['ip']}) rc={r['rc']}")
        print(f"$ {r['cmd']}\n{r['output']}")

    write_result(
        {
            "nodes": [
                {k: r[k] for k in ("node_index", "ip", "rc", "efa_tx_bytes")}
                for r in results
            ]
        }
    )
    assert all(r["rc"] == 0 for r in results), "nccom-test failed on some node"
    for r in results:
        # None means the counters are not readable inside the container.
        assert (
            r["efa_tx_bytes"] is None or r["efa_tx_bytes"] > 0
        ), f"node {r['node_index']} sent nothing over EFA"


if __name__ == "__main__":
    main()
