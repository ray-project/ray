import sys
import time
from pathlib import Path

import pytest

import ray
import ray.train
from ray.cluster_utils import Cluster
from ray.train.health import (
    Evaluator,
    Evict,
    HealthConfig,
    NodeProbe,
    Noop,
    PeriodicHealthCheck,
    ProbeResult,
)
from ray.train.v2._internal.constants import WORKER_GROUP_START_TIMEOUT_S_ENV_VAR
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def cluster():
    cluster = Cluster(initialize_head=True, head_node_args=dict(num_cpus=0))
    for _ in range(2):
        cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(
        address=cluster.address,
        runtime_env={
            "working_dir": str(Path(__file__).parent),
            "env_vars": {WORKER_GROUP_START_TIMEOUT_S_ENV_VAR: "10"},
        },
    )
    yield cluster
    ray.shutdown()
    cluster.shutdown()


class Up(NodeProbe):
    def poll(self) -> ProbeResult:
        return ProbeResult()


class EvictEveryNode(Evaluator):
    def evaluate(self, state):
        nodes = list(state.probe_results.get("Up", {}))
        return Evict(reason="test", target_nodes=nodes) if nodes else Noop()


@ray.remote(num_cpus=0)
class NodeLog:
    def __init__(self):
        self.node_ids = []

    def append(self, node_id):
        self.node_ids.append(node_id)

    def get(self):
        return self.node_ids


def train_fn():
    log = ray.get_actor("node_log")
    ray.get(log.append.remote(ray.get_runtime_context().get_node_id()))
    time.sleep(10)


def test_evicted_nodes_are_not_used_again(cluster):
    log = NodeLog.options(name="node_log").remote()
    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=1),
        run_config=ray.train.RunConfig(
            failure_config=ray.train.FailureConfig(
                max_failures=0, controller_failure_limit=0
            ),
            health_config=HealthConfig(
                inflight_checks=[
                    PeriodicHealthCheck(
                        probe_creator=lambda: [Up()],
                        evaluator_creator=lambda: [EvictEveryNode()],
                        poll_interval_s=0.1,
                    )
                ]
            ),
        ),
    )
    # Each attempt's node is evicted, until no node is left to start on.
    with pytest.raises(ray.train.TrainingFailedError):
        trainer.fit()

    node_ids = ray.get(log.get.remote())
    assert len(node_ids) == 2
    assert len(set(node_ids)) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
