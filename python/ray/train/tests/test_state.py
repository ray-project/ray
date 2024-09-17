import json
import os
import time

import pytest

import ray
from ray.cluster_utils import Cluster
from ray.train import RunConfig, ScalingConfig
from ray.train._internal.state.schema import (
    RunStatusEnum,
    TrainDatasetInfo,
    TrainRunInfo,
    TrainWorkerInfo,
)
from ray.train._internal.state.state_actor import (
    TRAIN_STATE_ACTOR_NAME,
    TRAIN_STATE_ACTOR_NAMESPACE,
    get_or_create_state_actor,
)
from ray.train._internal.state.state_manager import TrainRunStateManager
from ray.train._internal.worker_group import WorkerGroup
from ray.train.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def ray_start_gpu_cluster():
    cluster = Cluster()
    cluster.add_node(num_gpus=8, num_cpus=9)

    ray.shutdown()
    ray.init(
        address=cluster.address,
        runtime_env={"env_vars": {"RAY_TRAIN_ENABLE_STATE_TRACKING": "1"}},
        ignore_reinit_error=True,
    )

    yield

    ray.shutdown()
    cluster.shutdown()


RUN_INFO_JSON_SAMPLE = """{
    "name": "default_run",
    "id": "ad5256bc64c04c83833a8b006f531799",
    "job_id": "0000000001",
    "controller_actor_id": "3abd1972a19148d78acc78dd9414736e",
    "start_time_ms": 1717448423000,
    "run_status": "RUNNING",
    "status_detail": "",
    "end_time_ms": null,
    "workers": [
        {
        "actor_id": "3d86c25634a71832dac32c8802000000",
        "world_rank": 0,
        "local_rank": 0,
        "node_rank": 0,
        "node_id": "b1e6cbed8533ae2def4e7e7ced9d19858ceb1ed8ab9ba81ab9c07825",
        "node_ip": "10.0.208.100",
        "pid": 76071,
        "gpu_ids": [0],
        "status": null
        },
        {
        "actor_id": "8f162dd8365346d1b5c98ebd7338c4f9",
        "world_rank": 1,
        "local_rank": 1,
        "node_rank": 0,
        "node_id": "b1e6cbed8533ae2def4e7e7ced9d19858ceb1ed8ab9ba81ab9c07825",
        "node_ip": "10.0.208.100",
        "pid": 76072,
        "gpu_ids": [1],
        "status": null
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
        node_id="b1e6cbed8533ae2def4e7e7ced9d19858ceb1ed8ab9ba81ab9c07825",
        node_ip="10.0.208.100",
        pid=76071,
        gpu_ids=[0],
    )

    worker_info_1 = TrainWorkerInfo(
        actor_id="8f162dd8365346d1b5c98ebd7338c4f9",
        world_rank=1,
        local_rank=1,
        node_rank=0,
        node_id="b1e6cbed8533ae2def4e7e7ced9d19858ceb1ed8ab9ba81ab9c07825",
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
        start_time_ms=1717448423000,
        run_status=RunStatusEnum.RUNNING,
        status_detail="",
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


def test_state_actor_api(ray_start_4_cpus):
    state_actor = get_or_create_state_actor()
    named_actors = ray.util.list_named_actors(all_namespaces=True)
    assert {
        "name": TRAIN_STATE_ACTOR_NAME,
        "namespace": TRAIN_STATE_ACTOR_NAMESPACE,
    } in named_actors

    # Concurrently register 100 runs
    num_runs = 100
    info_list = [_get_run_info_sample(run_id=str(i)) for i in range(num_runs)]
    ray.get([state_actor.register_train_run.remote(run) for run in info_list])

    # Test get all runs
    train_runs = ray.get(state_actor.get_all_train_runs.remote())
    assert len(train_runs) == num_runs

    # Test get a single run by run_id
    for i in range(num_runs):
        run_info = ray.get(state_actor.get_train_run.remote(run_id=str(i)))
        assert run_info == info_list[i]


def test_state_manager(ray_start_gpu_cluster):
    worker_group = WorkerGroup(num_workers=4, resources_per_worker={"GPU": 1})

    # No errors raised if TrainStateActor is not started
    state_manager = TrainRunStateManager(state_actor=None)
    state_manager.register_train_run(
        run_id="run_id",
        run_name="run_name",
        job_id="0000000001",
        controller_actor_id="3abd1972a19148d78acc78dd9414736e",
        datasets={},
        worker_group=worker_group,
        start_time_ms=int(time.time() * 1000),
        run_status=RunStatusEnum.RUNNING,
    )

    # Register 100 runs with 10 TrainRunStateManagers
    state_actor = get_or_create_state_actor()
    for i in range(10):
        state_manager = TrainRunStateManager(state_actor=state_actor)
        for j in range(10):
            run_id = i * 10 + j
            state_manager.register_train_run(
                run_id=str(run_id),
                run_name="run_name",
                job_id="0000000001",
                controller_actor_id="3abd1972a19148d78acc78dd9414736e",
                datasets={
                    "train": ray.data.from_items(list(range(4))),
                    "eval": ray.data.from_items(list(range(4))),
                },
                worker_group=worker_group,
                start_time_ms=int(time.time() * 1000),
                run_status=RunStatusEnum.RUNNING,
            )

    runs = ray.get(state_actor.get_all_train_runs.remote())
    assert len(runs) == 100

    for i in range(100):
        run_id = str(i)
        run_info = ray.get(state_actor.get_train_run.remote(run_id=run_id))
        assert run_info and run_info.id == run_id


@pytest.mark.parametrize("gpus_per_worker", [0, 1, 2])
def test_track_e2e_training(ray_start_gpu_cluster, gpus_per_worker):
    os.environ["RAY_TRAIN_ENABLE_STATE_TRACKING"] = "1"
    num_workers = 4
    run_name = "test"
    datasets = {
        "train": ray.data.from_items(list(range(4))),
        "eval": ray.data.from_items(list(range(4))),
    }

    if gpus_per_worker == 0:
        use_gpu = False
        resources_per_worker = {"CPU": 1}
    else:
        use_gpu = True
        resources_per_worker = {"GPU": gpus_per_worker}

    trainer = DataParallelTrainer(
        train_loop_per_worker=lambda: None,
        run_config=RunConfig(name=run_name),
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker=resources_per_worker,
        ),
        datasets=datasets,
    )

    trainer.fit()

    state_actor = ray.get_actor(
        name=TRAIN_STATE_ACTOR_NAME, namespace=TRAIN_STATE_ACTOR_NAMESPACE
    )

    runs = ray.get(state_actor.get_all_train_runs.remote())
    run_id = next(iter(runs.keys()))
    run = next(iter(runs.values()))

    # Check Run Info
    assert run.id == run_id
    assert run.name == run_name
    assert len(run.workers) == num_workers
    assert run.controller_actor_id and run.job_id

    world_ranks = [worker.world_rank for worker in run.workers]
    local_ranks = [worker.local_rank for worker in run.workers]
    node_ranks = [worker.node_rank for worker in run.workers]

    # Ensure that the workers are sorted by global rank
    assert world_ranks == [0, 1, 2, 3]
    assert local_ranks == [0, 1, 2, 3]
    assert node_ranks == [0, 0, 0, 0]

    # Check GPU ids
    gpu_ids = [worker.gpu_ids for worker in run.workers]
    if gpus_per_worker == 0:
        assert gpu_ids == [[], [], [], []]
    elif gpus_per_worker == 1:
        assert gpu_ids == [[0], [1], [2], [3]]
    elif gpus_per_worker == 2:
        flat_gpu_ids = set()
        for ids in gpu_ids:
            flat_gpu_ids.update(ids)
        assert flat_gpu_ids == set(range(8))

    # Check Datasets
    for dataset_info in run.datasets:
        dataset = datasets[dataset_info.name]
        assert dataset_info.dataset_name == dataset._plan._dataset_name
        assert dataset_info.dataset_uuid == dataset._plan._dataset_uuid


@pytest.mark.parametrize("raise_error", [True, False])
def test_train_run_status(ray_start_gpu_cluster, raise_error):
    os.environ["RAY_TRAIN_ENABLE_STATE_TRACKING"] = "1"

    def get_train_run():
        state_actor = ray.get_actor(
            name=TRAIN_STATE_ACTOR_NAME, namespace=TRAIN_STATE_ACTOR_NAMESPACE
        )
        runs = ray.get(state_actor.get_all_train_runs.remote())
        return next(iter(runs.values()))

    def check_run_status(expected_status):
        run = get_train_run()
        assert run.run_status == expected_status

    def check_run_error(failed_rank, error_message):
        run = get_train_run()
        assert run.status_detail
        assert f"Rank {failed_rank} worker raised an error" in run.status_detail
        assert error_message in run.status_detail

    failed_rank = 0
    error_message = "User Application Error"

    def train_func():
        check_run_status(expected_status=RunStatusEnum.RUNNING)
        if raise_error and ray.train.get_context().get_world_rank() == failed_rank:
            raise RuntimeError(error_message)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=False),
    )

    try:
        trainer.fit()
    except Exception:
        pass

    if raise_error:
        check_run_status(expected_status=RunStatusEnum.ERRORED)
        check_run_error(failed_rank=failed_rank, error_message=error_message)
    else:
        check_run_status(expected_status=RunStatusEnum.FINISHED)

    ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
