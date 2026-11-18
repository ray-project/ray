"""Check that NCCL runs across nodes over EFA through the aws-ofi-nccl plugin.

The CUDA base-extra images ship the EFA user-space stack (libfabric) and build
aws-ofi-nccl against it. Nothing else in the release suite attaches EFA
interfaces, so a broken or mismatched libfabric / aws-ofi-nccl pair in the
image would otherwise go unnoticed: NCCL silently falls back to TCP sockets.

This test starts one torch.distributed (NCCL backend) rank per GPU on every
GPU node, runs an all-reduce sweep across the nodes and fails unless:

- each node exposes an EFA device and libfabric's efa provider inside the
  container,
- every rank loaded an aws-ofi-nccl plugin and NCCL selected the efa provider,
- the all-reduce results are correct, and
- the EFA hardware counters moved (when they are readable).

It needs a cluster whose GPU nodes have EFA attached, see compute_efa_*.yaml.
"""
import argparse
import glob
import json
import os
import re
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

MASTER_PORT = 29500
NCCL_LOG_DIR = "/tmp/efa_nccl_logs"
EFA_COUNTER_GLOB = "/sys/class/infiniband/*/ports/1/hw_counters/tx_bytes"

# The source-built plugin is installed under /usr/local, the EFA installer's
# libfabric under /opt/amazon/efa. NCCL dlopen()s the plugin by name, so both
# must be on the loader path.
LD_LIBRARY_PATH = "/usr/local/lib:/opt/amazon/efa/lib:${LD_LIBRARY_PATH}"

SIZES = [
    ("1MB", 1 << 20),
    ("32MB", 32 << 20),
    ("256MB", 256 << 20),
    ("1GB", 1 << 30),
]

PROVIDER_RE = re.compile(r"NET/OFI.*Selected [Pp]rovider is (\S+)")
PLUGIN_VERSION_RE = re.compile(r"NET/OFI Initializing aws-ofi-nccl (\S+)")
NCCL_VERSION_RE = re.compile(r"NCCL version (\S+)")


def _run(cmd: List[str]) -> Tuple[int, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return r.returncode, (r.stdout + r.stderr).strip()
    except (OSError, subprocess.TimeoutExpired) as e:
        return -1, str(e)


def _efa_tx_bytes() -> Optional[int]:
    """Sum of the EFA transmit counters, or None if they are not readable."""
    total, found = 0, False
    for path in glob.glob(EFA_COUNTER_GLOB):
        try:
            with open(path) as f:
                total += int(f.read().strip())
            found = True
        except (OSError, ValueError):
            pass
    return total if found else None


@ray.remote(num_cpus=1)
def probe_node() -> Dict[str, Any]:
    """Report what the container on this node sees of the EFA stack."""
    fi_rc, fi_out = _run(
        ["/opt/amazon/efa/bin/fi_info", "-p", "efa", "-t", "FI_EP_RDM"]
    )
    _, fi_version = _run(["/opt/amazon/efa/bin/fi_info", "--version"])
    return {
        "ip": ray.util.get_node_ip_address(),
        "uverbs": sorted(glob.glob("/dev/infiniband/uverbs*")),
        "fi_info_rc": fi_rc,
        "fi_info": fi_out[:2000],
        "fi_version": fi_version,
        "plugins": sorted(
            glob.glob("/usr/local/lib/libnccl-net*.so")
            + glob.glob("/opt/amazon/ofi-nccl/lib*/libnccl-net*.so")
        ),
        "efa_tx_bytes": _efa_tx_bytes(),
    }


@ray.remote(num_gpus=1, num_cpus=1)
class Rank:
    def init(self, rank: int, world_size: int, master_addr: str) -> str:
        import torch

        # NCCL does not create the directory of NCCL_DEBUG_FILE.
        os.makedirs(NCCL_LOG_DIR, exist_ok=True)
        self.torch = torch
        self.rank = rank
        self.world_size = world_size
        torch.distributed.init_process_group(
            backend="nccl",
            init_method=f"tcp://{master_addr}:{MASTER_PORT}",
            world_size=world_size,
            rank=rank,
        )
        return ray.util.get_node_ip_address()

    def all_reduce(
        self, size_in_bytes: int, iters: int, warmup: int
    ) -> Tuple[float, bool]:
        """Returns (seconds per all-reduce, whether every result was right)."""
        torch = self.torch
        dist = torch.distributed
        buf = torch.empty(size_in_bytes // 4, dtype=torch.float32, device="cuda")
        ok = True
        elapsed = 0.0
        for i in range(warmup + iters):
            buf.fill_(1.0)
            torch.cuda.synchronize()
            dist.barrier()
            start = time.perf_counter()
            dist.all_reduce(buf)
            torch.cuda.synchronize()
            if i >= warmup:
                elapsed += time.perf_counter() - start
            # Every rank contributed ones, so every element is the world size.
            ok = ok and bool((buf == float(self.world_size)).all().item())
        return elapsed / iters, ok

    def report(self) -> Dict[str, Any]:
        """NCCL debug log and loaded libraries of this rank's process."""
        log = ""
        for path in glob.glob(os.path.join(NCCL_LOG_DIR, f"*.{os.getpid()}.log")):
            with open(path, errors="replace") as f:
                log += f.read()
        with open("/proc/self/maps") as f:
            libs = sorted(
                {
                    line.split()[-1]
                    for line in f
                    if re.search(r"libnccl|libfabric|libefa", line)
                }
            )
        return {
            "rank": self.rank,
            "ip": ray.util.get_node_ip_address(),
            "log": log,
            "libs": libs,
            "efa_tx_bytes": _efa_tx_bytes(),
        }

    def shutdown(self) -> None:
        self.torch.distributed.destroy_process_group()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iters", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument(
        "--expected-plugin-version",
        default=None,
        help="Fail unless NCCL reports this aws-ofi-nccl version, e.g. 1.22.0.",
    )
    args = parser.parse_args()

    ray.init(
        runtime_env={
            "env_vars": {
                "LD_LIBRARY_PATH": LD_LIBRARY_PATH,
                "NCCL_DEBUG": "INFO",
                "NCCL_DEBUG_FILE": os.path.join(NCCL_LOG_DIR, "nccl.%h.%p.log"),
                "FI_PROVIDER": "efa",
            }
        }
    )

    gpu_nodes = sorted(
        (n for n in ray.nodes() if n["Alive"] and n["Resources"].get("GPU", 0) >= 1),
        key=lambda n: n["NodeManagerAddress"],
    )
    assert len(gpu_nodes) >= 2, f"need at least 2 GPU nodes, found {len(gpu_nodes)}"

    def on(node):
        return NodeAffinitySchedulingStrategy(node["NodeID"], soft=False)

    # 1. The container on every GPU node must see EFA.
    probes = ray.get(
        [probe_node.options(scheduling_strategy=on(n)).remote() for n in gpu_nodes]
    )
    errors = []
    for p in probes:
        print(f"[{p['ip']}] {p['fi_version']}")
        print(f"[{p['ip']}] uverbs devices: {p['uverbs']}")
        print(f"[{p['ip']}] aws-ofi-nccl plugins: {p['plugins']}")
        if not p["uverbs"]:
            errors.append(
                f"{p['ip']}: no /dev/infiniband/uverbs* device (EFA not attached?)"
            )
        if p["fi_info_rc"] != 0:
            errors.append(f"{p['ip']}: `fi_info -p efa` failed: {p['fi_info']}")
        if not p["plugins"]:
            errors.append(f"{p['ip']}: no libnccl-net*.so plugin in the image")
    assert not errors, "EFA is not usable on this cluster:\n" + "\n".join(errors)

    # 2. One NCCL rank per GPU, ranks grouped by node.
    ranks = []
    for node in gpu_nodes:
        for _ in range(int(node["Resources"]["GPU"])):
            ranks.append(Rank.options(scheduling_strategy=on(node)).remote())
    world_size = len(ranks)
    master_addr = gpu_nodes[0]["NodeManagerAddress"]
    ips = ray.get(
        [r.init.remote(i, world_size, master_addr) for i, r in enumerate(ranks)]
    )
    print(f"{world_size} ranks on {len(gpu_nodes)} nodes: {ips}")

    # 3. All-reduce sweep. Bus bandwidth uses the nccl-tests definition.
    perf_metrics = []
    all_ok = True
    for size_str, size in SIZES:
        results = ray.get(
            [r.all_reduce.remote(size, args.iters, args.warmup) for r in ranks]
        )
        seconds = max(t for t, _ in results)
        all_ok = all_ok and all(ok for _, ok in results)
        busbw = size / seconds * 2 * (world_size - 1) / world_size / 1e9
        print(f"all_reduce {size_str:>6}: {seconds * 1e3:9.3f} ms  {busbw:8.2f} GB/s")
        perf_metrics.append(
            {
                "perf_metric_name": f"all_reduce-{size_str}-busbw_gbps",
                "perf_metric_value": busbw,
                "perf_metric_type": "THROUGHPUT",
            }
        )

    # 4. Evidence that the traffic went through aws-ofi-nccl on EFA.
    reports = ray.get([r.report.remote() for r in ranks])
    ray.get([r.shutdown.remote() for r in ranks])

    tx_before = {p["ip"]: p["efa_tx_bytes"] for p in probes}
    providers, plugin_versions, nccl_versions = set(), set(), set()
    for rep in reports:
        provider = PROVIDER_RE.search(rep["log"])
        version = PLUGIN_VERSION_RE.search(rep["log"])
        nccl_version = NCCL_VERSION_RE.search(rep["log"])
        providers.add(provider.group(1).rstrip("*") if provider else None)
        plugin_versions.add(version.group(1) if version else None)
        nccl_versions.add(nccl_version.group(1) if nccl_version else None)
        if not any("libnccl-net" in lib for lib in rep["libs"]):
            errors.append(
                f"rank {rep['rank']} ({rep['ip']}): no aws-ofi-nccl plugin loaded, "
                f"NCCL is not using libfabric. Loaded: {rep['libs']}"
            )
        if not provider:
            errors.append(
                f"rank {rep['rank']} ({rep['ip']}): no 'NET/OFI Selected provider' "
                "line in the NCCL log"
            )
        elif provider.group(1).rstrip("*") != "efa":
            errors.append(
                f"rank {rep['rank']} ({rep['ip']}): NCCL selected provider "
                f"{provider.group(1)!r}, expected efa"
            )
        before, after = tx_before.get(rep["ip"]), rep["efa_tx_bytes"]
        if before is not None and after is not None and after <= before:
            errors.append(
                f"rank {rep['rank']} ({rep['ip']}): EFA tx_bytes did not move "
                f"({before} -> {after}), traffic did not use EFA"
            )

    print(f"NCCL versions: {nccl_versions}")
    print(f"aws-ofi-nccl versions: {plugin_versions}")
    print(f"libfabric providers: {providers}")
    print(f"libraries loaded by rank 0: {reports[0]['libs']}")
    if any("EFA tx_bytes" not in e for e in errors) or not reports[0]["log"]:
        print("---- NCCL log of rank 0 ----")
        print(reports[0]["log"][-8000:])

    if not all_ok:
        errors.append("all_reduce returned wrong values")
    if args.expected_plugin_version and plugin_versions != {
        args.expected_plugin_version
    }:
        errors.append(
            f"aws-ofi-nccl versions {plugin_versions}, "
            f"expected {args.expected_plugin_version}"
        )
    assert not errors, "NCCL over EFA check failed:\n" + "\n".join(sorted(set(errors)))

    if "TEST_OUTPUT_JSON" in os.environ:
        with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
            json.dump(
                {
                    "perf_metrics": perf_metrics,
                    "world_size": world_size,
                    "num_nodes": len(gpu_nodes),
                    "nccl_version": sorted(str(v) for v in nccl_versions),
                    "aws_ofi_nccl_version": sorted(str(v) for v in plugin_versions),
                    "libfabric": probes[0]["fi_version"],
                },
                out_file,
            )
    print("PASSED: NCCL ran across nodes over EFA")


if __name__ == "__main__":
    main()
