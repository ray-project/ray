import pyarrow
import pytest

import ray
from ray import train
from ray.air._internal.uri_utils import URI
from ray.air.constants import EXPR_RESULT_FILE
from ray.train import CheckpointConfig, Result, RunConfig, ScalingConfig
from ray.train.base_trainer import TrainingFailedError
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.torch import TorchTrainer
from ray.tune import TuneConfig, Tuner

_PARAM_SPACE = {"a": 1, "b": 2}


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def build_dummy_trainer(configs):
    def worker_loop(_config):
        for i in range(configs["NUM_ITERATIONS"]):
            # Do some random reports in between checkpoints.
            train.report({"metric_a": -100, "metric_b": -100})

            if ray.train.get_context().get_world_rank() == 0:
                with create_dict_checkpoint({"iter": i}) as checkpoint:
                    train.report(
                        metrics={"metric_a": i, "metric_b": -i},
                        checkpoint=checkpoint,
                    )
            else:
                train.report(metrics={"metric_a": i, "metric_b": -i})
        raise RuntimeError()

    trainer = TorchTrainer(
        train_loop_per_worker=worker_loop,
        train_loop_config=_PARAM_SPACE,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        run_config=RunConfig(
            name=configs["EXP_NAME"],
            storage_path=configs["STORAGE_PATH"],
            checkpoint_config=CheckpointConfig(
                num_to_keep=configs["NUM_CHECKPOINTS"],
                checkpoint_score_attribute="metric_a",
                checkpoint_score_order="max",
            ),
        ),
    )
    return trainer


def build_dummy_tuner(configs):
    return Tuner(
        build_dummy_trainer(configs),
        param_space={"train_loop_config": _PARAM_SPACE},
        tune_config=TuneConfig(num_samples=1),
    )


@pytest.mark.parametrize("storage", ["local", "remote"])
@pytest.mark.parametrize("mode", ["trainer", "tuner"])
def test_result_restore(ray_start_4_cpus, tmpdir, mock_s3_bucket_uri, storage, mode):
    NUM_ITERATIONS = 5
    NUM_CHECKPOINTS = 3
    if storage == "local":
        storage_path = str(tmpdir)
    elif storage == "remote":
        storage_path = str(URI(mock_s3_bucket_uri))

    exp_name = "test_result_restore"

    configs = {
        "EXP_NAME": exp_name,
        "STORAGE_PATH": storage_path,
        "NUM_ITERATIONS": NUM_ITERATIONS,
        "NUM_CHECKPOINTS": NUM_CHECKPOINTS,
    }

    if mode == "trainer":
        trainer = build_dummy_trainer(configs)
        with pytest.raises(TrainingFailedError):
            trainer.fit()
    elif mode == "tuner":
        tuner = build_dummy_tuner(configs)
        tuner.fit()

    # Find the trial directory to restore
    exp_dir = str(URI(storage_path) / exp_name)
    fs, fs_exp_dir = pyarrow.fs.FileSystem.from_uri(exp_dir)
    for item in fs.get_file_info(pyarrow.fs.FileSelector(fs_exp_dir)):
        if item.type == pyarrow.fs.FileType.Directory and item.base_name.startswith(
            "TorchTrainer"
        ):
            trial_dir = str(URI(exp_dir) / item.base_name)
            break

    # [1] Restore from path
    result = Result.from_path(trial_dir)

    # Check if we restored all checkpoints
    assert result.checkpoint
    assert len(result.best_checkpoints) == NUM_CHECKPOINTS

    """
    Top-3 checkpoints with metrics:

                        | iter   | metric_a    metric_b
    checkpoint_000004        4            4          -4
    checkpoint_000003        3            3          -3
    checkpoint_000002        2            2          -2
    """
    # Check if the checkpoints bounded with correct metrics
    best_ckpt_a = result.get_best_checkpoint(metric="metric_a", mode="max")
    assert load_dict_checkpoint(best_ckpt_a)["iter"] == NUM_ITERATIONS - 1

    best_ckpt_b = result.get_best_checkpoint(metric="metric_b", mode="max")
    assert load_dict_checkpoint(best_ckpt_b)["iter"] == NUM_ITERATIONS - NUM_CHECKPOINTS

    with pytest.raises(RuntimeError, match="Invalid metric name.*"):
        result.get_best_checkpoint(metric="invalid_metric", mode="max")

    # Check if we properly restored errors
    assert isinstance(result.error, RuntimeError)

    # Check that the config is properly formatted in the result metrics
    assert result.metrics.get("config") == {"train_loop_config": _PARAM_SPACE}

    # [2] Restore from path without result.json
    fs.delete_file((URI(trial_dir) / EXPR_RESULT_FILE).path)
    result = Result.from_path(trial_dir)

    # Do the same checks as above
    assert result.checkpoint
    assert len(result.best_checkpoints) == NUM_CHECKPOINTS

    best_ckpt_a = result.get_best_checkpoint(metric="metric_a", mode="max")
    assert load_dict_checkpoint(best_ckpt_a)["iter"] == NUM_ITERATIONS - 1

    best_ckpt_b = result.get_best_checkpoint(metric="metric_b", mode="max")
    assert load_dict_checkpoint(best_ckpt_b)["iter"] == NUM_ITERATIONS - NUM_CHECKPOINTS

    assert isinstance(result.error, RuntimeError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
