"""End-to-end tests for Ray Data usage collection across processes.

Executions run in whichever process hosts the ``StreamingExecutor``. These tests
start real clusters and check that every execution lands in the single
``data_usage`` tag in GCS, whether the executors live in ``SplitCoordinator``
actors (Ray Train), user actors, or concurrent threads of one driver.
"""

import json
import sys
import threading

import pytest

import ray
import ray.train
from ray._common.test_utils import wait_for_condition
from ray._common.usage import usage_lib
from ray._raylet import GcsClient  # pyrefly: ignore[missing-module-attribute]
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def _reported_execution_ids():
    gcs = GcsClient(address=ray.get_runtime_context().gcs_address)
    tags = usage_lib.get_extra_usage_tags_to_report(gcs)
    payload = json.loads(tags.get("data_usage", '{"executions": []}'))
    return {e["id"] for e in payload["executions"]}


def test_train_streaming_split_reports_every_dataset(shutdown_only):
    """The workload that surfaced the bug: Ray Train runs each dataset's
    executor in its own ``SplitCoordinator`` actor, so two datasets are two
    processes. Both executions must be reported, not just the last writer's."""
    # ``ray.init()`` disables usage stats in the driver env, which the
    # coordinators inherit; the job runtime env re-enables it for them.
    ray.init(runtime_env={"env_vars": {"RAY_USAGE_STATS_ENABLED": "1"}})

    train_ds = ray.data.range(1)
    val_ds = ray.data.range(1)

    def train_func():
        for name in ("train", "val"):
            for _ in ray.train.get_dataset_shard(name).iter_batches():
                pass

    TorchTrainer(
        train_func,
        # pyrefly: ignore[bad-argument-type]  # ray.train.ScalingConfig is a v1|v2 union
        scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
        datasets={"train": train_ds, "val": val_ds},
    ).fit()

    wait_for_condition(lambda: len(_reported_execution_ids()) == 2)


def test_concurrent_subcluster_datasets_report_every_execution(
    ray_start_cluster, monkeypatch
):
    """Two datasets pinned to different subclusters via per-dataset
    ``DataContext`` label selectors, executed concurrently from two driver
    threads. Both executors call into the usage actor at once, so this also
    covers the actor's get-or-create path under thread contention."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1, labels={"ray-subcluster": "tenant_a"})
    cluster.add_node(num_cpus=1, labels={"ray-subcluster": "tenant_b"})
    ray.init(address=cluster.address)
    # The driver hosts both executors, so opt this process in through the
    # documented env var (it takes priority over ``~/.ray/config.json``).
    monkeypatch.setenv("RAY_USAGE_STATS_ENABLED", "1")

    def make_dataset(subcluster: str) -> ray.data.Dataset:
        ctx = ray.data.DataContext.get_current().copy()
        ctx.execution_options.label_selector = {"ray-subcluster": subcluster}
        with ray.data.DataContext.current(ctx):
            return ray.data.range(1)

    # Construct each Dataset in the main thread so the temporary contexts
    # don't race on the process-global default context; then run concurrently.
    ds_a = make_dataset("tenant_a")
    ds_b = make_dataset("tenant_b")
    threads = [
        threading.Thread(target=ds_a.materialize),
        threading.Thread(target=ds_b.materialize),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
        assert not t.is_alive(), "materialize() did not finish"

    wait_for_condition(lambda: len(_reported_execution_ids()) == 2)


def test_executions_from_user_actors_are_merged(shutdown_only):
    """Minimal form of the cross-process case with no Train dependency: two
    worker processes each run one execution."""
    ray.init()

    # Re-enable usage stats in the worker processes that host the executions.
    @ray.remote(runtime_env={"env_vars": {"RAY_USAGE_STATS_ENABLED": "1"}})
    def run_one():
        ray.data.range(1).materialize()

    ray.get([run_one.remote() for _ in range(2)])

    wait_for_condition(lambda: len(_reported_execution_ids()) == 2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
