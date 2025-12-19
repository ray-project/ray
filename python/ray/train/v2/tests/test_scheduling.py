from collections import defaultdict

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.train import ScalingConfig
from ray.train.v2._internal.constants import WORKER_GROUP_START_TIMEOUT_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def multi_cpu_node_cluster():
    """Yields a CPU cluster with 4 CPU nodes (4x4 CPU)."""
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=4)
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def multi_cpu_node_labeled_cluster():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=4, labels={"subcluster": "my_subcluster"})
    for _ in range(2):
        cluster.add_node(num_cpus=4, labels={"subcluster": "other_subcluster"})
    cluster.wait_for_nodes()
    cluster.connect()
    yield cluster
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.parametrize("placement_strategy", ["PACK", "SPREAD"])
def test_placement_strategy(multi_cpu_node_cluster, tmp_path, placement_strategy):
    """Tests the placement strategy of the worker group.
    Nodes in this test are virtual but still have unique `node_id`s.
    """
    scaling_config = ScalingConfig(
        num_workers=4,
        resources_per_worker={"CPU": 1},
        placement_strategy=placement_strategy,
    )

    def train_fn(config):
        node_id = ray.get_runtime_context().get_node_id()
        (tmp_path / node_id).touch()

    trainer = DataParallelTrainer(train_fn, scaling_config=scaling_config)
    trainer.fit()

    if placement_strategy == "PACK":
        assert len(list(tmp_path.iterdir())) == 1
    elif placement_strategy == "SPREAD":
        assert len(list(tmp_path.iterdir())) == 4


@pytest.mark.parametrize("placement_strategy", ["STRICT_PACK", "STRICT_SPREAD"])
def test_infeasible_placement_strategy(
    multi_cpu_node_cluster, tmp_path, monkeypatch, placement_strategy
):
    """Infeasible requests will continue retrying until the resources are available."""

    @ray.remote(num_cpus=0)
    def run():
        monkeypatch.setenv(WORKER_GROUP_START_TIMEOUT_S_ENV_VAR, "0.25")
        scaling_config = ScalingConfig(
            num_workers=8,
            resources_per_worker={"CPU": 1},
            placement_strategy=placement_strategy,
        )
        trainer = DataParallelTrainer(lambda: None, scaling_config=scaling_config)
        trainer.fit()

    future = run.remote()

    ready, _ = ray.wait([future], timeout=0.5)
    assert not ready

    # Add 4 more 8-CPU nodes to the cluster to make the request feasible.
    for _ in range(4):
        multi_cpu_node_cluster.add_node(num_cpus=8)

    ray.get(future)


@pytest.mark.parametrize(
    "label_selector, expected_subcluster_counts",
    [
        ({"subcluster": "my_subcluster"}, {"my_subcluster": 2}),
        (
            [{"subcluster": "my_subcluster"}, {"subcluster": "my_subcluster"}],
            {"my_subcluster": 2},
        ),
        (
            [{"subcluster": "my_subcluster"}, {"subcluster": "other_subcluster"}],
            {"my_subcluster": 1, "other_subcluster": 1},
        ),
    ],
)
def test_label_selector(
    multi_cpu_node_labeled_cluster, label_selector, expected_subcluster_counts
):
    @ray.remote
    class Counter:
        def __init__(self):
            self.subcluster_counts = defaultdict(int)

        def increment(self, subcluster):
            self.subcluster_counts[subcluster] += 1

        def get(self):
            return self.subcluster_counts

    counter = Counter.remote()
    scaling_config = ScalingConfig(
        num_workers=2,
        label_selector=label_selector,
    )

    def train_fn():
        ray.get(
            counter.increment.remote(
                ray.get_runtime_context().get_node_labels()["subcluster"]
            )
        )

    trainer = DataParallelTrainer(train_fn, scaling_config=scaling_config)
    trainer.fit()
    assert ray.get(counter.get.remote()) == expected_subcluster_counts


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
