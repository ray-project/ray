import json
import os

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.train import ScalingConfig
from ray.train._internal.state.schema import (
    TrainDatasetInfo,
    TrainRunInfo,
    TrainWorkerInfo,
)
from ray.train._internal.state.state_actor import (
    TRAIN_STATE_ACTOR_NAME,
    TRAIN_STATE_ACTOR_NAMESPACE,
    get_or_create_state_actor,
)
from ray.train.data_parallel_trainer import DataParallelTrainer

STATE_TRACKING_RUNTIME_ENV = {"env_vars": {"RAY_TRAIN_ENABLE_STATE_TRACKING": "1"}}


@pytest.fixture
def ray_start_1_node():
    cluster = Cluster()
    cluster.add_node(num_gpus=4, num_cpus=5)

    ray.init(address=cluster.address, runtime_env=STATE_TRACKING_RUNTIME_ENV)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_start_2_nodes():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_gpus=2, num_cpus=3)

    ray.init(address=cluster.address, runtime_env=STATE_TRACKING_RUNTIME_ENV)

    yield

    ray.shutdown()
    cluster.shutdown()


RUN_INFO_JSON_SAMPLE = """{
    "name": "default_run",
    "id": "ad5256bc64c04c83833a8b006f531799",
    "job_id": "0000000001",
    "controller_actor_id": "3abd1972a19148d78acc78dd9414736e",
    "workers": [
        {
        "actor_id": "3d86c25634a71832dac32c8802000000",
        "world_rank": 0,
        "local_rank": 0,
        "node_rank": 0,
        "node_id": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "node_ip": "10.0.208.100",
        "pid": 76071,
        "gpu_ids": [0]
        },
        {
        "actor_id": "8f162dd8365346d1b5c98ebd7338c4f9",
        "world_rank": 1,
        "local_rank": 1,
        "node_rank": 0,
        "node_id": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        "node_ip": "10.0.208.100",
        "pid": 76072,
        "gpu_ids": [1]
        }
    ],
    "datasets": [
        {
        "name": "train",
        "dataset_name": "train_dataset",
        "dataset_uuid": "1"
        }
    ]
}"""


def _get_run_info_sample(run_id=None, run_name=None) -> TrainRunInfo:
    dataset_info = TrainDatasetInfo(
        name="train", dataset_name="train_dataset", dataset_uuid="1"
    )

    worker_info_0 = TrainWorkerInfo(
        actor_id="3d86c25634a71832dac32c8802000000",
        world_rank=0,
        local_rank=0,
        node_rank=0,
        node_id="ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        node_ip="10.0.208.100",
        pid=76071,
        gpu_ids=[0],
    )

    worker_info_1 = TrainWorkerInfo(
        actor_id="8f162dd8365346d1b5c98ebd7338c4f9",
        world_rank=1,
        local_rank=1,
        node_rank=0,
        node_id="ffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        node_ip="10.0.208.100",
        pid=76072,
        gpu_ids=[1],
    )

    run_info = TrainRunInfo(
        name=run_name if run_name else "default_run",
        id=run_id if run_id else "ad5256bc64c04c83833a8b006f531799",
        job_id="0000000001",
        controller_actor_id="3abd1972a19148d78acc78dd9414736e",
        workers=[worker_info_0, worker_info_1],
        datasets=[dataset_info],
    )
    return run_info


def test_schema_equivalance():
    json_sample = RUN_INFO_JSON_SAMPLE
    dict_sample = json.loads(RUN_INFO_JSON_SAMPLE)

    run_info_from_json = TrainRunInfo.parse_raw(json_sample)
    run_info_from_obj = TrainRunInfo.parse_obj(dict_sample)

    # Test serialization equivalence
    assert run_info_from_json == run_info_from_obj

    # Test dict deserialization equivalence
    assert run_info_from_json.dict() == dict_sample

    # Test json deserialization equivalence
    assert json.loads(run_info_from_json.json()) == json.loads(json_sample)

    # Test constructors equivalence
    assert _get_run_info_sample() == run_info_from_json


def test_state_actor_api():
    state_actor = get_or_create_state_actor()
    assert not ray.get(state_actor.get_all_train_runs.remote())

    # Register new train runs
    run_info_1 = _get_run_info_sample(run_id="1")
    run_info_2 = _get_run_info_sample(run_id="2")
    ray.get(state_actor.register_train_run.remote(run_info_1))
    ray.get(state_actor.register_train_run.remote(run_info_2))

    # Test get all runs
    train_runs = ray.get(state_actor.get_all_train_runs.remote())
    assert train_runs.keys() == {"1", "2"}

    # Test get a single run
    assert ray.get(state_actor.get_train_run.remote(run_id="1")) == run_info_1
    assert ray.get(state_actor.get_train_run.remote(run_id="2")) == run_info_2


def test_single_node_training(ray_start_1_node, use_gpu=False):
    os.environ["RAY_TRAIN_ENABLE_STATE_TRACKING"] = "1"

    def train_func():
        pass

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=use_gpu),
    )

    # Ensure the the StateActor is created on driver
    named_actors = ray.util.list_named_actors(all_namespaces=True)
    assert named_actors == [
        {"name": TRAIN_STATE_ACTOR_NAME, "namespace": TRAIN_STATE_ACTOR_NAMESPACE}
    ]

    trainer.fit()

    state_actor = get_or_create_state_actor()

    runs = ray.get(state_actor.get_all_train_runs.remote())
    assert len(runs) == 1

    trainer.fit()
    runs = ray.get(state_actor.get_all_train_runs.remote())
    assert len(runs) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
