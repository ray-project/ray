import multiprocessing
import os
import signal
import tempfile
from pathlib import Path

import boto3
import pyarrow.fs
import pytest
import torch

import ray
from ray._common.test_utils import simulate_s3_bucket
from ray.air._internal.uri_utils import URI
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.train import (
    BackendConfig,
    Checkpoint,
    CheckpointUploadMode,
    RunConfig,
    ScalingConfig,
    UserCallback,
)
from ray.train.backend import Backend
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR, _get_ray_train_session_dir
from ray.train.tests.util import create_dict_checkpoint
from ray.train.v2._internal.constants import is_v2_enabled
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.exceptions import TrainingFailedError, WorkerGroupError
from ray.train.v2.api.result import Result

assert is_v2_enabled()


@pytest.fixture(scope="module", autouse=True)
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_backend_setup(tmp_path):
    class ValidationBackend(Backend):
        def on_start(self, worker_group, backend_config):
            tmp_path.joinpath("on_start").touch()

        def on_training_start(self, worker_group, backend_config):
            tmp_path.joinpath("on_training_start").touch()

        def on_shutdown(self, worker_group, backend_config):
            tmp_path.joinpath("on_shutdown").touch()

    class ValidationBackendConfig(BackendConfig):
        @property
        def backend_cls(self):
            return ValidationBackend

    trainer = DataParallelTrainer(
        lambda: None,
        backend_config=ValidationBackendConfig(),
        scaling_config=ScalingConfig(num_workers=2),
    )
    trainer.fit()

    assert tmp_path.joinpath("on_start").exists()
    assert tmp_path.joinpath("on_training_start").exists()
    assert tmp_path.joinpath("on_shutdown").exists()


def test_result_output(tmp_path):
    trainer = DataParallelTrainer(
        lambda: None,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="test", storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert isinstance(result, Result)
    assert result.path == str(tmp_path / "test")
    assert isinstance(result.filesystem, pyarrow.fs.FileSystem)


def test_no_optional_arguments():
    """Check that the DataParallelTrainer can be instantiated without optional arguments."""
    trainer = DataParallelTrainer(lambda: "not used")
    trainer.fit()


def test_train_loop_config():
    """Check that the train loop config is passed to the train function
    if a config parameter is accepted."""

    def train_fn(config):
        with create_dict_checkpoint({}) as checkpoint:
            ray.train.report(metrics=config, checkpoint=checkpoint)

    train_loop_config = {"x": 1}
    trainer = DataParallelTrainer(
        train_fn,
        train_loop_config=train_loop_config,
        scaling_config=ScalingConfig(num_workers=2),
    )
    result = trainer.fit()
    assert result.metrics == train_loop_config


def test_report_checkpoint_rank0(tmp_path):
    """Check that checkpoints can be reported from rank 0 only."""

    def train_fn():
        metrics = {"rank": ray.train.get_context().get_world_rank()}
        if ray.train.get_context().get_world_rank() == 0:
            with create_dict_checkpoint({}) as checkpoint:
                ray.train.report(metrics=metrics, checkpoint=checkpoint)
        else:
            ray.train.report(metrics=metrics, checkpoint=None)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert result.metrics == {"rank": 0}
    assert result.checkpoint


def test_report_checkpoint_multirank(tmp_path):
    """Check that checkpoints can be reported from multiple ranks."""

    ranks_to_report = [1, 3]

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        metrics = {"rank": rank}
        if rank in ranks_to_report:
            with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                Path(temp_checkpoint_dir).joinpath(str(rank)).touch()
                checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)
                ray.train.report(metrics=metrics, checkpoint=checkpoint)
        else:
            ray.train.report(metrics=metrics, checkpoint=None)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=4),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert result.checkpoint
    result.checkpoint.to_directory(tmp_path / "validate")
    for rank in ranks_to_report:
        assert tmp_path.joinpath("validate", str(rank)).exists()


def test_error(tmp_path):
    def _error_func_rank_0():
        """An example train_fun that raises an error on rank 0."""
        if ray.train.get_context().get_world_rank() == 0:
            raise ValueError("user error")

    trainer = DataParallelTrainer(
        _error_func_rank_0,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name="test", storage_path=str(tmp_path)),
    )
    with pytest.raises(TrainingFailedError) as exc_info:
        trainer.fit()
    assert isinstance(exc_info.value, WorkerGroupError)
    assert "user error" in str(exc_info.value.worker_failures[0])
    assert len(exc_info.value.worker_failures) == 1


@pytest.mark.parametrize("env_disabled", [True, False])
def test_setup_working_directory(tmp_path, monkeypatch, env_disabled):
    # Set the environment variable to control the working directory setup
    monkeypatch.setenv(RAY_CHDIR_TO_TRIAL_DIR, str(int(not env_disabled)))

    experiment_dir_name = "test"
    reference_working_dir = (
        Path(_get_ray_train_session_dir(), "test").resolve().as_posix()
    )

    def _check_same_working_directory():
        worker_working_dir = os.getcwd()
        if env_disabled:
            assert worker_working_dir != reference_working_dir
        else:
            assert worker_working_dir == reference_working_dir

    trainer = DataParallelTrainer(
        _check_same_working_directory,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(name=experiment_dir_name, storage_path=str(tmp_path)),
    )
    trainer.fit()


def test_user_callback(tmp_path):
    """Test end to end usage of user callbacks."""
    num_workers = 2

    class MyUserCallback(UserCallback):
        def after_report(self, run_context, metrics, checkpoint):
            assert len(metrics) == num_workers
            assert not checkpoint

        def after_exception(self, run_context, worker_exceptions):
            assert len(worker_exceptions) == 1
            assert worker_exceptions.get(0) is not None

    def _train_fn(config):
        ray.train.report(metrics={"rank": ray.train.get_context().get_world_rank()})
        if ray.train.get_context().get_world_rank() == 0:
            raise ValueError("error")

    trainer = DataParallelTrainer(
        _train_fn,
        scaling_config=ScalingConfig(num_workers=num_workers),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            callbacks=[MyUserCallback()],
        ),
    )
    # The error should NOT be an assertion error from the user callback.
    with pytest.raises(WorkerGroupError):
        trainer.fit()


def run_process_for_sigint_abort(abort_terminates):
    # Lives outside test_sigint_abort because cannot pickle nested functions.

    # Needed to reuse current ray cluster.
    ray.init(address="auto")

    if not abort_terminates:

        async def fake_abort():
            while True:
                pass

        from ray.train.v2._internal.execution.controller import TrainController

        TrainController.abort = fake_abort

    def train_fn():
        signal_actor = ray.get_actor("signal_actor", namespace="test_sigint_abort")
        ray.get(signal_actor.send.remote())
        while True:
            pass

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
    )
    trainer.fit()


@pytest.mark.parametrize(
    "spam_sigint",
    [
        False,
        # Disabling this test because it's flaky.
        # True,
    ],
)
def test_sigint_abort(spam_sigint):
    # Use SignalActor to wait for training to start before sending SIGINT.
    SignalActor = create_remote_signal_actor(ray)
    signal_actor = SignalActor.options(
        name="signal_actor", namespace="test_sigint_abort"
    ).remote()

    # Use spawn because of
    # https://docs.ray.io/en/latest/ray-core/patterns/fork-new-processes.html
    multiprocessing.set_start_method("spawn", force=True)
    process = multiprocessing.Process(
        target=run_process_for_sigint_abort, args=(not spam_sigint,)
    )
    process.start()

    # Wait for training to start.
    ray.get(signal_actor.wait.remote())

    # Verify that process exits after sufficient number of SIGINTS.
    os.kill(process.pid, signal.SIGINT)
    if spam_sigint:
        import time

        assert process.exitcode is None
        # This is flaky. Sometimes SIGINTs are ignored and you need to wait.
        while process.exitcode is None:
            time.sleep(1)
            os.kill(process.pid, signal.SIGINT)
    process.join()


SUPPORTED_METRICS = [
    {"loss": 1.0},
    {"loss": 1, "accuracy": 0.95},
    {"loss": None},
    {"loss": "label"},
    {"nested": {"a": 1}},
]
UNSUPPORTED_METRICS = ["torch_tensor", "nested_torch_tensor", "torch_state_dict"]


def test_supported_report_metrics(tmp_path):
    def train_fn():
        for metric in SUPPORTED_METRICS:
            with tempfile.TemporaryDirectory() as temp_dir:
                ray.train.report(
                    metrics=metric,
                    checkpoint=ray.train.Checkpoint.from_directory(temp_dir),
                )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name="test-supported-report-metrics", storage_path=str(tmp_path)
        ),
    )
    result = trainer.fit()
    for (_, actual_metric), expected_metric in zip(
        result.best_checkpoints, SUPPORTED_METRICS, strict=True
    ):
        assert actual_metric == expected_metric

    restored_result = Result.from_path(tmp_path / "test-supported-report-metrics")
    for (_, actual_metric), expected_metric in zip(
        restored_result.best_checkpoints, SUPPORTED_METRICS, strict=True
    ):
        assert actual_metric == expected_metric


@pytest.mark.parametrize("metric_name", UNSUPPORTED_METRICS)
def test_unsupported_report_metrics(metric_name, tmp_path):
    def train_fn():
        if metric_name == "torch_tensor":
            metric = {"loss": torch.tensor(1.0)}
        elif metric_name == "nested_torch_tensor":
            metric = {"nested": {"a": torch.tensor(1.0)}}
        elif metric_name == "torch_state_dict":
            metric = torch.nn.Linear(1, 1).state_dict()
        else:
            raise ValueError()

        with tempfile.TemporaryDirectory() as temp_dir:
            ray.train.report(
                metrics=metric, checkpoint=ray.train.Checkpoint.from_directory(temp_dir)
            )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name="test-unsupported-report-metrics", storage_path=str(tmp_path)
        ),
    )
    with pytest.raises(WorkerGroupError) as exc_info:
        trainer.fit()

    assert len(exc_info.value.worker_failures) == 1
    worker_error = exc_info.value.worker_failures[0]
    assert isinstance(worker_error, ValueError)
    assert worker_error.args[0].startswith(
        "Passing objects containing Torch tensors as metrics is not "
        "supported as it will throw an exception on deserialization."
    )


@pytest.mark.parametrize("metric", SUPPORTED_METRICS)
def test_supported_returned_metrics(metric, tmp_path):
    def train_fn():
        return metric

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name="test-supported-return-metrics", storage_path=str(tmp_path)
        ),
    )
    result = trainer.fit()
    assert result.return_value == metric


@pytest.mark.parametrize("metric_name", UNSUPPORTED_METRICS)
def test_unsupported_returned_metrics(metric_name, tmp_path):
    def train_fn():
        if metric_name == "torch_tensor":
            metric = {"loss": torch.tensor(1.0)}
        elif metric_name == "nested_torch_tensor":
            metric = {"nested": {"a": torch.tensor(1.0)}}
        elif metric_name == "torch_state_dict":
            metric = torch.nn.Linear(1, 1).state_dict()
        else:
            raise ValueError()

        return metric

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name="test-unsupported-report-metrics", storage_path=str(tmp_path)
        ),
    )
    with pytest.raises(WorkerGroupError) as exc_info:
        trainer.fit()

    assert len(exc_info.value.worker_failures) == 1
    worker_error = exc_info.value.worker_failures[0]
    assert isinstance(worker_error, ValueError)
    assert worker_error.args[0].startswith(
        "Returning objects containing Torch tensors from the "
        "training function is not supported as it will throw an "
        "exception on deserialization."
    )


def test_local_in_out_of_band_checkpointing(tmp_path, port=5002, region="us-west-2"):
    tmp_path = tmp_path.resolve()
    experiment_path = tmp_path / "storage-dir"
    out_of_band_path = tmp_path / "oob-dir"

    def write_file(file_path, content: str):
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(content)

    with simulate_s3_bucket(port=port, region=region) as s3_uri:
        s3_bucket_uri = URI(s3_uri)
        bucket_name = s3_bucket_uri.name
        aws_env_vars = {
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_SECURITY_TOKEN": "testing",
            "AWS_SESSION_TOKEN": "testing",
        }

        def train_fn():
            # Save with default storage upload
            write_file(tmp_path / "epoch-1" / "results.txt", "1")
            ray.train.report(
                metrics={"score": 1},
                checkpoint=Checkpoint(tmp_path / "epoch-1"),
                checkpoint_dir_name="epoch-1",
            )
            # Save in-band with NO_UPLOAD
            write_file(experiment_path / "epoch-2" / "results.txt", "2")
            ray.train.report(
                metrics={"score": 2},
                checkpoint=Checkpoint(experiment_path / "epoch-2"),
                checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
            )
            # Save in-band with a custom upload function
            write_file(experiment_path / "epoch-3" / "results.txt", "3")
            ray.train.report(
                metrics={"score": 3},
                checkpoint=Checkpoint(experiment_path / "epoch-3"),
                checkpoint_upload_fn=lambda ckpt, name: ckpt,
            )

            # Save out-of-band with NO_UPLOAD
            write_file(out_of_band_path / "epoch-4" / "results.txt", "4")
            ray.train.report(
                metrics={"score": 4},
                checkpoint=Checkpoint(out_of_band_path / "epoch-4"),
                checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
            )
            # Save out-of-band with a custo upload function
            write_file(out_of_band_path / "epoch-5" / "results.txt", "5")
            ray.train.report(
                metrics={"score": 5},
                checkpoint=Checkpoint(out_of_band_path / "epoch-5"),
                checkpoint_upload_fn=lambda ckpt, name: ckpt,
            )

            s3 = boto3.client(
                "s3", region_name=region, endpoint_url=f"http://localhost:{port}"
            )
            s3.create_bucket(
                Bucket=URI(s3_uri).name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
            # S3 with default upload
            s3.put_object(Bucket=bucket_name, Key="epoch-6/results.txt", Body="6")
            ray.train.report(
                metrics={"score": 6},
                checkpoint=Checkpoint(str(s3_bucket_uri / "epoch-6")),
                checkpoint_dir_name="epoch-6",
            )
            # Save S3 out-of-band with NO_UPLOAD
            s3.put_object(Bucket=bucket_name, Key="epoch-7/results.txt", Body="7")
            ray.train.report(
                metrics={"score": 7},
                checkpoint=Checkpoint(str(s3_bucket_uri / "epoch-7")),
                checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
            )
            # Save S3 out-of-band with custom upload fn
            s3.put_object(Bucket=bucket_name, Key="epoch-8/results.txt", Body="8")
            ray.train.report(
                metrics={"score": 8},
                checkpoint=Checkpoint(str(s3_bucket_uri / "epoch-8")),
                checkpoint_upload_fn=lambda ckpt, name: ckpt,
            )

            reported_checkpoints = ray.train.get_all_reported_checkpoints()
            assert len(reported_checkpoints) == 8

            raise RuntimeError("intentional failure after checkpoint reports")

        # Run train_fn and leave a checkpoint manager snapshot behind.
        trainer = DataParallelTrainer(
            train_fn,
            run_config=RunConfig(
                name="test-local-in-out-of-band",
                storage_path=str(experiment_path),
                worker_runtime_env={"env_vars": aws_env_vars},
            ),
        )
        with pytest.raises(WorkerGroupError, match="intentional failure"):
            trainer.fit()

        def resumption_train_fn():
            checkpoint = ray.train.get_checkpoint()
            assert checkpoint.path.endswith("epoch-8")
            assert isinstance(checkpoint.filesystem, pyarrow.fs.S3FileSystem)

            reported_checkpoints = ray.train.get_all_reported_checkpoints()
            assert len(reported_checkpoints) == 8

        trainer = DataParallelTrainer(
            resumption_train_fn,
            run_config=RunConfig(
                name="test-local-in-out-of-band",
                storage_path=str(experiment_path),
                worker_runtime_env={"env_vars": aws_env_vars},
            ),
        )
        result = trainer.fit()
        assert len(result.best_checkpoints) == 8

        restored_result = Result.from_path(
            experiment_path / "test-local-in-out-of-band"
        )
        assert len(restored_result.best_checkpoints) == 8

        expected_paths = [
            experiment_path / "test-local-in-out-of-band" / "epoch-1",
            experiment_path / "epoch-2",
            experiment_path / "epoch-3",
            out_of_band_path / "epoch-4",
            out_of_band_path / "epoch-5",
            experiment_path / "test-local-in-out-of-band" / "epoch-6",
            f"{bucket_name}/epoch-7",
            f"{bucket_name}/epoch-8",
        ]
        for expected_path, (ckpt, metrics), i in zip(
            expected_paths, restored_result.best_checkpoints, range(1, 9), strict=True
        ):
            assert ckpt.path == str(expected_path)
            if i < 7:
                assert isinstance(ckpt.filesystem, pyarrow.fs.LocalFileSystem)
            else:
                assert isinstance(ckpt.filesystem, pyarrow.fs.S3FileSystem)
            assert metrics == {"score": i}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
