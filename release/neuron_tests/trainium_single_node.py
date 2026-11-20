"""Check one Trainium node: detection, core pinning and on-node collectives.

Needs Neuron hardware. Defaults match trn2.48xlarge with the default logical
core config of 2: 16 devices, 4 logical cores each.
"""
import argparse
import json
import subprocess

import ray

from neuron_common import write_result


@ray.remote
def pinned_core() -> dict:
    import os

    return {
        "ids": ray.get_runtime_context().get_accelerator_ids()["neuron_cores"],
        "env": os.environ.get("NEURON_RT_VISIBLE_CORES"),
    }


@ray.remote
def run_on_trainium(cmd: list, timeout: int) -> dict:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"rc": r.returncode, "output": r.stdout + r.stderr}
    except subprocess.TimeoutExpired as e:
        return {"rc": -1, "output": f"{e.stdout or ''}{e.stderr or ''}\nTIMED OUT"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--devices", type=int, default=16)
    parser.add_argument("--cores-per-node", type=int, default=64)
    args = parser.parse_args()

    ray.init()
    result = {}

    # Ray detected the Neuron cores (through neuron-ls) on the Trainium node.
    nodes = [
        n
        for n in ray.nodes()
        if n["Alive"] and n["Resources"].get("neuron_cores", 0) > 0
    ]
    assert nodes, "no node reports neuron_cores"
    cores = int(nodes[0]["Resources"]["neuron_cores"])
    result["neuron_cores"] = cores
    assert cores == args.cores_per_node, f"expected {args.cores_per_node}, got {cores}"

    # A task that asks for one core gets exactly one, through the env var.
    pins = ray.get(
        [pinned_core.options(resources={"neuron_cores": 1}).remote() for _ in range(4)]
    )
    result["pinned_cores"] = pins
    for pin in pins:
        assert len(pin["ids"]) == 1 and pin["env"] == pin["ids"][0], pin

    whole_node = {"neuron_cores": cores}

    # The Neuron tools in the image see every device of the host.
    ls = ray.get(
        run_on_trainium.options(resources=whole_node).remote(
            ["neuron-ls", "--json-output"], 120
        )
    )
    assert ls["rc"] == 0, ls["output"]
    devices = json.loads(ls["output"])
    result["neuron_devices"] = len(devices)
    assert len(devices) == args.devices, f"expected {args.devices}: {ls['output']}"

    # Collectives across every core of the node, with output verification.
    cmd = ["nccom-test", "all_reduce", "-r", str(cores), "--non-interactive"]
    cmd += ["-b", "1MB", "-e", "64MB", "-f", "4", "-n", "20", "-w", "5", "--check"]
    nccom = ray.get(run_on_trainium.options(resources=whole_node).remote(cmd, 1200))
    print(nccom["output"])
    result["nccom_test_rc"] = nccom["rc"]
    write_result(result)
    assert nccom["rc"] == 0, "nccom-test failed"


if __name__ == "__main__":
    main()
