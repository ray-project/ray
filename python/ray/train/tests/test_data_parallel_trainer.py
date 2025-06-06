import os
import time
from unittest.mock import patch

import pytest

import ray
from ray import train, tune
from ray._common.utils import RESOURCE_CONSTRAINT_PREFIX
from ray.cluster_utils import Cluster
from ray.train import RunConfig, ScalingConfig
from ray.train._internal.backend_executor import BackendExecutor
from ray.train._internal.worker_group import WorkerGroup
from ray.train.backend import Backend, BackendConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.tune.callback import Callback
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from ray.util.accelerators import NVIDIA_A100, NVIDIA_TESLA_A10G


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_4_gpus_4_extra():
    address_info = ray.init(num_cpus=4, num_gpus=4, resources={"extra": 4})
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_heterogenous_cluster():
    """
    Start a heterogenous cluster with 6 nodes:
        - 2 node with 4 x A100
        - 2 node with 4 x A10G
        - 2 node with 4 x GPU without accelerator_type
    """
    cluster = Cluster()

    for accelerator_type in [NVIDIA_A100, NVIDIA_TESLA_A10G, None]:
        for _ in range(2):
            cluster.add_node(
                num_cpus=4,
                num_gpus=4,
                resources=(
                    {f"{RESOURCE_CONSTRAINT_PREFIX}{accelerator_type}": 1.0}
                    if accelerator_type
                    else {}
                ),
            )

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


def gen_execute_single_async_special(special_f):
    def execute_single_async_special(self, i, f, *args, **kwargs):
        assert len(self.workers) == 2
        if i == 0 and hasattr(self, "should_fail") and self.should_fail:
            kwargs["train_func"] = special_f
        return (
            self.workers[i]
            .actor._RayTrainWorker__execute.options(name=f.__name__)
            .remote(f, *args, **kwargs)
        )

    return execute_single_async_special


def gen_new_backend_executor(special_f):
    """Returns a BackendExecutor that runs special_f on worker 0 once."""

    class TestBackendExecutor(BackendExecutor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._has_failed = False

        def start_training(self, *args, **kwargs):
            special_execute = gen_execute_single_async_special(special_f)
            if not self._has_failed:
                self.worker_group.should_fail = True
                self._has_failed = True
            else:
                self.worker_group.should_fail = False
            with patch.object(WorkerGroup, "execute_single_async", special_execute):
                super().start_training(*args, **kwargs)

    return TestBackendExecutor


class CaptureReportCallback(Callback):
    def __init__(self):
        self.result_list = []

    def on_trial_result(self, iteration, trials, trial, result, **info):
        self.result_list.append(result)


scale_config = ScalingConfig(num_workers=2)


def test_fit_train(ray_start_4_cpus):
    def train_func():
        train.report({"loss": 1})

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    assert trainer.fit().metrics["loss"] == 1


def test_scaling_config(ray_start_4_cpus):
    def train_func():
        assert ray.available_resources()["CPU"] == 1
        train.report({"loss": 1})

    assert ray.available_resources()["CPU"] == 4
    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=ScalingConfig(num_workers=2)
    )
    trainer.fit()


def test_fit_train_config(ray_start_4_cpus):
    def train_func(config):
        train.report({"loss": config["x"]})

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        train_loop_config={"x": 100},
    )
    assert trainer.fit().metrics["loss"] == 100


def test_datasets(ray_start_4_cpus):
    num_train_data = 10
    num_val_data = 6

    train_dataset = ray.data.range(num_train_data)
    val_dataset = ray.data.range(num_val_data)

    def get_dataset():
        # Train dataset should be sharded.
        train_dataset = train.get_dataset_shard("train")
        train_ds_count = len(list(train_dataset.iter_rows()))
        assert train_ds_count == num_train_data / scale_config.num_workers
        # All other datasets should not be sharded.
        val_dataset = train.get_dataset_shard("val")
        val_ds_count = len(list(val_dataset.iter_rows()))
        assert val_ds_count == num_val_data / scale_config.num_workers

    trainer = DataParallelTrainer(
        train_loop_per_worker=get_dataset,
        scaling_config=scale_config,
        datasets={"train": train_dataset, "val": val_dataset},
    )
    trainer.fit()


def test_invalid_train_loop():
    def train_loop(config, extra_arg):
        pass

    with pytest.raises(ValueError):
        DataParallelTrainer(train_loop_per_worker=train_loop)


def test_bad_return_in_train_loop(ray_start_4_cpus):
    """Test to check if returns from train loop are discarded."""

    # Simulates what happens with eg. torch models
    class FailOnUnpickle:
        def __reduce__(self):
            raise RuntimeError("Failing")

    def train_loop(config):
        train.report({"loss": 1})
        return FailOnUnpickle()

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_loop, scaling_config=scale_config
    )

    # No exception should happen here
    trainer.fit()


def test_tune(ray_start_4_cpus):
    def train_func(config):
        train.report({"loss": config["x"]})

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"x": 100},
        scaling_config=scale_config,
    )

    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"x": tune.choice([200, 300])}},
        tune_config=TuneConfig(num_samples=2),
    )
    result_grid = tuner.fit()
    assert result_grid[0].metrics["loss"] in [200, 300]

    # Make sure original Trainer is not affected.
    assert trainer._train_loop_config["x"] == 100


def test_fast_slow(ray_start_4_cpus):
    def train_func():
        for i in range(2):
            with create_dict_checkpoint({"epoch": i}) as checkpoint:
                train.report(dict(index=i), checkpoint=checkpoint)

    def train_slow():
        for i in range(2):
            with create_dict_checkpoint({"epoch": i}) as checkpoint:
                train.report(dict(index=i), checkpoint=checkpoint)
            time.sleep(5)

    new_backend_executor_cls = gen_new_backend_executor(train_slow)
    callback = CaptureReportCallback()

    class DataParallelTrainerPatched(DataParallelTrainer):
        _backend_executor_cls = new_backend_executor_cls

    trainer = DataParallelTrainerPatched(
        train_func,
        scaling_config=scale_config,
        run_config=RunConfig(callbacks=[callback]),
    )
    results = trainer.fit()

    assert load_dict_checkpoint(results.checkpoint)["epoch"] == 1

    result_list = callback.result_list
    assert len(result_list) == 2


def test_mismatch_report(ray_start_4_cpus):
    def train_func():
        for _ in range(2):
            train.report(dict(loss=1))

    def train_mismatch():
        train.report(dict(loss=1))

    new_backend_executor_cls = gen_new_backend_executor(train_mismatch)

    class DataParallelTrainerPatched(DataParallelTrainer):
        _backend_executor_cls = new_backend_executor_cls

    trainer = DataParallelTrainerPatched(
        train_func,
        scaling_config=scale_config,
    )
    with pytest.raises(RuntimeError):
        trainer.fit()


def test_world_rank(ray_start_4_cpus, tmp_path):
    def train_func():
        world_rank = train.get_context().get_world_rank()
        (tmp_path / f"{world_rank}").touch()
        train.report(dict(world_rank=world_rank))

    trainer = DataParallelTrainer(train_func, scaling_config=scale_config)
    trainer.fit()

    created_files = list(tmp_path.glob("*"))
    assert len(created_files) == 2
    assert {int(file.name) for file in created_files} == {0, 1}


def test_gpu_requests(ray_start_4_cpus_4_gpus_4_extra, tmp_path):
    def get_visible_devices_for_workers():
        return [file.read_text() for file in tmp_path.glob("*")]

    class CudaTestBackend(Backend):
        share_cuda_visible_devices = True

    class CudaTestConfig(BackendConfig):
        @property
        def backend_cls(self):
            return CudaTestBackend

    def get_resources():
        cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]
        world_rank = train.get_context().get_world_rank()
        (tmp_path / f"{world_rank}").write_text(cuda_visible_devices)
        train.report(dict(devices=cuda_visible_devices))

    # 0 GPUs will be requested and should not raise an error.
    trainer = DataParallelTrainer(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    )
    trainer.fit()
    assert get_visible_devices_for_workers() == ["", ""]

    # 1 GPU will be requested and should not raise an error.
    trainer = DataParallelTrainer(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
    )
    trainer.fit()
    visible_devices = get_visible_devices_for_workers()
    # Sort the cuda visible devices to have exact match with expected result.
    visible_devices = [",".join(sorted(r.split(","))) for r in visible_devices]
    assert visible_devices == ["0,1", "0,1"]

    # Partial GPUs should not raise an error.
    trainer = DataParallelTrainer(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(
            num_workers=2, use_gpu=True, resources_per_worker={"GPU": 0.1}
        ),
    )
    trainer.fit()
    visible_devices = get_visible_devices_for_workers()
    assert visible_devices == ["0", "0"]

    # Multiple GPUs should not raise an error.
    trainer = DataParallelTrainer(
        get_resources,
        backend_config=CudaTestConfig(),
        scaling_config=ScalingConfig(
            num_workers=2, use_gpu=True, resources_per_worker={"GPU": 2}
        ),
    )
    trainer.fit()
    visible_devices = get_visible_devices_for_workers()
    # Sort the cuda visible devices to have exact match with expected result.
    visible_devices = [",".join(sorted(r.split(","))) for r in visible_devices]
    assert visible_devices == ["0,1,2,3", "0,1,2,3"]


@pytest.mark.parametrize("num_gpus", [1, 2])
@pytest.mark.parametrize("accelerator_type", [NVIDIA_A100, NVIDIA_TESLA_A10G, None])
def test_config_accelerator_type(
    ray_start_heterogenous_cluster, num_gpus, accelerator_type
):
    def train_func():
        # Ensure all workers are scheduled on nodes with specified accelerators
        assigned_resources = ray.get_runtime_context().get_assigned_resources()
        assert assigned_resources["GPU"] == num_gpus
        if accelerator_type:
            accelerator_key = f"{RESOURCE_CONSTRAINT_PREFIX}{accelerator_type}"
            assert accelerator_key in assigned_resources

    trainer = DataParallelTrainer(
        train_func,
        scaling_config=ScalingConfig(
            num_workers=4,
            use_gpu=True,
            accelerator_type=accelerator_type,
            resources_per_worker={"GPU": num_gpus},
        ),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
