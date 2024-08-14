import json
import os
from tempfile import TemporaryDirectory

import pytest

import ray
from ray import train, tune
from ray.train import Checkpoint, FailureConfig, RunConfig, ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.xgboost import XGBoostTrainer
from ray.tune.schedulers.async_hyperband import ASHAScheduler
from ray.tune.schedulers.resource_changing_scheduler import (
    DistributeResources,
    ResourceChangingScheduler,
)
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def train_fn(config):
    start_epoch = 0

    print(train.get_context().get_trial_resources())
    checkpoint = train.get_checkpoint()
    if checkpoint:
        # assume that we have run the train.report() example
        # and successfully save some model weights
        with checkpoint.as_directory() as tmpdir:
            with open(os.path.join(tmpdir, "checkpoint.json"), "r") as fin:
                checkpoint_dict = json.load(fin)

        start_epoch = checkpoint_dict.get("epoch", -1) + 1

    # wrap the model in DDP
    for epoch in range(start_epoch, config["num_epochs"]):
        with TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "checkpoint.json"), "w") as fout:
                json.dump(dict(epoch=epoch), fout)

            train.report(
                {
                    "metric": config["metric"] * epoch,
                    "epoch": epoch,
                    "num_cpus": train.get_context()
                    .get_trial_resources()
                    .required_resources["CPU"],
                },
                checkpoint=Checkpoint.from_directory(tmpdir),
            )


class AssertingDataParallelTrainer(DataParallelTrainer):
    def training_loop(self) -> None:
        scaling_config = self._validate_scaling_config(self.scaling_config)
        pgf = scaling_config.as_placement_group_factory()
        tr = train.get_context().get_trial_resources()
        # Ensure that strategy attribute didn't get dropped.
        assert pgf.strategy == "SPREAD"
        assert pgf == tr, (pgf, tr)
        return super().training_loop()


class AssertingXGBoostTrainer(XGBoostTrainer):
    @property
    def _ray_params(self):
        scaling_config = self._validate_scaling_config(self.scaling_config)
        pgf = scaling_config.as_placement_group_factory()
        tr = train.get_context().get_trial_resources()
        # Ensure that strategy attribute didn't get dropped.
        assert pgf.strategy == "SPREAD"
        assert pgf == tr, (scaling_config, pgf, tr)
        return super()._ray_params


def test_data_parallel_trainer(ray_start_8_cpus):
    num_workers = 2
    trainer = AssertingDataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(
            num_workers=num_workers, placement_strategy="SPREAD"
        ),
    )
    tuner = Tuner(
        trainer,
        param_space={
            "train_loop_config": {
                "num_epochs": 100,
                "metric": tune.grid_search([1, 2, 3, 4, 5]),
            }
        },
        tune_config=TuneConfig(
            mode="max",
            metric="metric",
            scheduler=ResourceChangingScheduler(
                ASHAScheduler(),
                resources_allocation_function=DistributeResources(
                    add_bundles=True, reserve_resources={"CPU": 1}
                ),
            ),
        ),
        run_config=RunConfig(failure_config=FailureConfig(fail_fast=True)),
    )
    result_grid = tuner.fit()
    assert not any(x.error for x in result_grid)
    # + 1 for Trainable
    assert result_grid.get_dataframe()["num_cpus"].max() > num_workers + 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
