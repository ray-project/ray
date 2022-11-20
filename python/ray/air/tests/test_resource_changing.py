from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import FailureConfig, RunConfig, ScalingConfig
from ray.air.constants import TRAIN_DATASET_KEY
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.xgboost import XGBoostTrainer
from sklearn.datasets import load_breast_cancer
import pandas as pd
import pytest
import ray
from ray import tune
from ray.tune.schedulers.resource_changing_scheduler import (
    DistributeResources,
    ResourceChangingScheduler,
)
from ray.tune.schedulers.async_hyperband import ASHAScheduler


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def train_fn(config):
    start_epoch = 0

    print(session.get_trial_resources())
    checkpoint = session.get_checkpoint()
    if checkpoint:
        # assume that we have run the session.report() example
        # and successfully save some model weights
        checkpoint_dict = checkpoint.to_dict()
        start_epoch = checkpoint_dict.get("epoch", -1) + 1

    # wrap the model in DDP
    for epoch in range(start_epoch, config["num_epochs"]):
        checkpoint = Checkpoint.from_dict(dict(epoch=epoch))
        session.report(
            {
                "metric": config["metric"] * epoch,
                "epoch": epoch,
                "num_cpus": session.get_trial_resources().required_resources["CPU"],
            },
            checkpoint=checkpoint,
        )


class AssertingDataParallelTrainer(DataParallelTrainer):
    def training_loop(self) -> None:
        scaling_config = self._validate_scaling_config(self.scaling_config)
        pgf = scaling_config.as_placement_group_factory()
        tr = session.get_trial_resources()
        # Ensure that strategy attribute didn't get dropped.
        assert pgf.strategy == "SPREAD"
        assert pgf == tr, (pgf, tr)
        return super().training_loop()


class AssertingXGBoostTrainer(XGBoostTrainer):
    @property
    def _ray_params(self):
        scaling_config = self._validate_scaling_config(self.scaling_config)
        pgf = scaling_config.as_placement_group_factory()
        tr = session.get_trial_resources()
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


def test_gbdt_trainer(ray_start_8_cpus):
    data_raw = load_breast_cancer()
    dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
    dataset_df["target"] = data_raw["target"]
    train_ds = ray.data.from_pandas(dataset_df).repartition(16)
    trainer = AssertingXGBoostTrainer(
        datasets={TRAIN_DATASET_KEY: train_ds},
        label_column="target",
        scaling_config=ScalingConfig(num_workers=2),
        params={
            "objective": "binary:logistic",
            "eval_metric": ["logloss"],
        },
    )
    tuner = Tuner(
        trainer,
        param_space={
            "num_boost_round": 100,
            "params": {
                "eta": tune.grid_search([0.28, 0.29, 0.3, 0.31, 0.32]),
            },
        },
        tune_config=TuneConfig(
            mode="min",
            metric="train-logloss",
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
