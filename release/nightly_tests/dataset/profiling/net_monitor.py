# ABOUTME: Samples psutil network I/O counters on every worker node.
# ABOUTME: Writes per-node CSV files to shared storage for post-run analysis.

import os
import threading
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@ray.remote(num_cpus=0, num_gpus=0)
def _start_net_monitor(outdir):
    """Sample psutil.net_io_counters every second, write to shared storage."""
    import csv
    import psutil

    node_ip = ray.util.get_node_ip_address()
    os.makedirs(outdir, exist_ok=True)
    path = f"{outdir}/net_counters_{node_ip.replace('.', '_')}.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["timestamp", "bytes_sent", "bytes_recv", "packets_sent", "packets_recv"]
        )
        while True:
            c = psutil.net_io_counters()
            writer.writerow(
                [
                    time.time(),
                    c.bytes_sent,
                    c.bytes_recv,
                    c.packets_sent,
                    c.packets_recv,
                ]
            )
            f.flush()
            time.sleep(1)


def _net_monitor_loop(outdir):
    """Launch net_io_counters sampling on every worker node as it joins."""
    monitored_node_ids = set()
    while True:
        for node in ray.nodes():
            if not node["Alive"] or node["NodeID"] in monitored_node_ids:
                continue
            try:
                _start_net_monitor.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=node["NodeID"], soft=False
                    )
                ).remote(outdir)
                monitored_node_ids.add(node["NodeID"])
                print(
                    f"Net monitor on {node['NodeManagerAddress']} "
                    f"({len(monitored_node_ids)} nodes)"
                )
            except Exception as e:
                print(f"Net monitor failed on {node['NodeManagerAddress']}: {e}")
        time.sleep(2)


def start(outdir):
    """Start network monitoring on all nodes in a background thread.

    Args:
        outdir: Shared storage directory for output files.
    """
    thread = threading.Thread(target=_net_monitor_loop, args=(outdir,), daemon=True)
    thread.start()
    return thread
