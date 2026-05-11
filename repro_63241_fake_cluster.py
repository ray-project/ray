"""End-to-end repro for #63241 on Mac using a fake autoscaling cluster.

Spins up an AutoscalingCluster (autoscaler v2) with TWO worker pools that have
different labels:
- ``xlarge_pool`` labeled ``instance-type=m6i.xlarge`` (4 CPU per node)
- ``large_pool``  labeled ``instance-type=m6i.large``  (2 CPU per node)

Submits a tiny TorchTrainer with ``label_selector={"instance-type": "m6i.xlarge"}``
and (a) prints the labels of the node the worker actually landed on, (b) polls
the autoscaler to see which pools got scaled.

Note: fake_multi_node uses ``use_node_id_as_ip: True``, so every node reports
``127.0.0.1`` — don't use IP to identify pools. Use the node labels instead.

Usage:
    /opt/homebrew/Caskroom/miniconda/base/envs/raytrain/bin/python repro_63241_fake_cluster.py
"""
import threading
import time

import ray
from ray.cluster_utils import AutoscalingCluster

cluster = AutoscalingCluster(
    head_resources={"CPU": 2, "GPU": 0},
    worker_node_types={
        # 1-CPU labeled nodes — each can hold exactly ONE bundle.
        # This prevents the autoscaler from bin-packing both the labeled PG demand
        # and the (pre-fix) unlabeled request_resources demand onto a single node.
        "xlarge_pool": {
            "resources": {"CPU": 1},
            "labels": {"instance-type": "m6i.xlarge"},
            "node_config": {},
            "min_workers": 0,
            "max_workers": 4,
        },
        "large_pool": {
            "resources": {"CPU": 1},
            "labels": {"instance-type": "m6i.large"},
            "node_config": {},
            "min_workers": 0,
            "max_workers": 4,
        },
    },
    autoscaler_v2=True,
)

cluster.start()
ray.init("auto")

import ray.train  # noqa: E402
import ray.train.torch  # noqa: E402
from ray.autoscaler.v2.sdk import get_cluster_status  # noqa: E402

GCS_ADDRESS = ray.get_runtime_context().gcs_address


def train_fn():
    ctx = ray.get_runtime_context()
    node_id = ctx.get_node_id()
    # Look up our own node's labels via ray.nodes()
    me = next((n for n in ray.nodes() if n["NodeID"] == node_id), None)
    labels = me.get("Labels", {}) if me else {}
    print(f"[worker] running on node_id={node_id} labels={labels}")
    time.sleep(120)


def run_trainer():
    try:
        ray.train.torch.TorchTrainer(
            train_fn,
            scaling_config=ray.train.ScalingConfig(
                num_workers=1,
                resources_per_worker={"CPU": 1},
                label_selector={"instance-type": "m6i.xlarge"},
            ),
        ).fit()
    except Exception as e:
        print(f"(expected) fit() ended: {type(e).__name__}: {e}")


trainer_thread = threading.Thread(target=run_trainer, daemon=True)
trainer_thread.start()

# Poll every few seconds and print what the autoscaler is doing.
for i in range(12):
    time.sleep(5)
    status = get_cluster_status(GCS_ADDRESS)

    def by_pool(nodes):
        out = {}
        for n in nodes:
            out[n.ray_node_type_name] = out.get(n.ray_node_type_name, 0) + 1
        return out

    print(
        f"[t+{(i + 1) * 5}s] "
        f"active={by_pool(status.active_nodes)} "
        f"pending={by_pool(status.pending_nodes)} "
        f"launches={[(l.instance_type_name, l.count) for l in status.pending_launches]}"
    )

cluster.shutdown()
