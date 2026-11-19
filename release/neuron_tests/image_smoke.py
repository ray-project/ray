"""Check that the Neuron image is usable, without Neuron hardware.

Runs on ordinary CPU nodes. It covers what breaks at the image level: the glibc
that TorchNeuron needs, the libfabric that Neuron collectives need, and the
Neuron user-space packages and environment. Each check runs on every node,
inside a Ray task, so it sees the same environment a workload would.
"""
import ray

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from neuron_common import write_result

MIN_GLIBC = (2, 43)  # torch-neuronx is built against GLIBC_2.43
MIN_LIBFABRIC = (2, 4)  # Neuron runtime / collectives 2.35

NATIVE_LIBRARY_CHECK = """
import ctypes, importlib.util, os
import torch
import neuronxcc
pkg = importlib.util.find_spec("torch_neuronx").submodule_search_locations[0]
ctypes.CDLL(os.path.join(pkg, "_C.so"))
print("torch", torch.__version__, "torch_neuronx native library loads")
"""


# num_cpus=0: the head node advertises no CPUs, and it has to be checked too.
@ray.remote(num_cpus=0)
def check_node() -> dict:
    import os
    import platform
    import re
    import shutil
    import subprocess
    import sys

    def run(cmd, env=None):
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=env)
        return r.returncode, r.stdout + r.stderr

    errors = []
    info = {"host": platform.node()}

    info["glibc"] = platform.libc_ver()[1]
    if tuple(int(x) for x in info["glibc"].split(".")[:2]) < MIN_GLIBC:
        errors.append(f"glibc {info['glibc']} is older than {MIN_GLIBC}")

    rc, out = run(["fi_info", "--version"])
    m = re.search(r"^libfabric:\s*(\d+)\.(\d+)\S*", out, re.M)
    info["libfabric"] = m.group(0) if m else out.strip()
    if rc != 0 or not m or (int(m.group(1)), int(m.group(2))) < MIN_LIBFABRIC:
        errors.append(f"libfabric too old or fi_info failed: {out.strip()}")

    # Neuron collectives must pick up the EFA installer's libfabric.
    rc, out = run(["ldd", "/opt/aws/neuron/lib/libnccom-net.so"])
    fabric = [line.strip() for line in out.splitlines() if "libfabric" in line]
    info["libnccom_net_libfabric"] = fabric
    if rc != 0 or not any("/opt/amazon/efa/" in line for line in fabric):
        errors.append(f"libnccom-net.so does not link the EFA libfabric: {out}")

    for tool in ("neuron-ls", "nccom-test", "fi_info"):
        info[tool] = shutil.which(tool)
        if not info[tool]:
            errors.append(f"{tool} is not on PATH ({os.environ.get('PATH')})")

    if "/opt/aws/neuron/lib" not in os.environ.get("LD_LIBRARY_PATH", ""):
        errors.append(
            f"LD_LIBRARY_PATH lacks /opt/aws/neuron/lib: "
            f"{os.environ.get('LD_LIBRARY_PATH')}"
        )

    # torch auto-loads torch_neuronx as a device backend, and that import asks
    # the Neuron driver for the instance type, so it only works on Neuron
    # hardware. Here, check that the native library resolves its glibc and
    # libstdc++ requirements, which is what breaks on an older base image.
    rc, out = run(
        [sys.executable, "-c", NATIVE_LIBRARY_CHECK],
        env={**os.environ, "TORCH_DEVICE_BACKEND_AUTOLOAD": "0"},
    )
    info["torch_neuronx"] = out.strip().splitlines()[-1] if out.strip() else ""
    if rc != 0:
        errors.append(f"torch_neuronx native library does not load: {out}")

    info["errors"] = errors
    return info


def main():
    ray.init()
    node_ids = [n["NodeID"] for n in ray.nodes() if n["Alive"]]
    results = ray.get(
        [
            check_node.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False)
            ).remote()
            for node_id in node_ids
        ]
    )
    write_result({"nodes": results})
    failed = [r for r in results if r["errors"]]
    assert not failed, f"Neuron image checks failed: {failed}"
    print(f"Neuron image checks passed on {len(results)} node(s).")


if __name__ == "__main__":
    main()
