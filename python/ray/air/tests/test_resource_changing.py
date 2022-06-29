from typing import Callable, Dict, Optional, Union
from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfigDataClass
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.base_trainer import BaseTrainer
from ray.train.gbdt_trainer import GBDTTrainer
import pytest
import ray
from ray import tune
from ray.tune.schedulers.resource_changing_scheduler import DistributeResources, ResourceChangingScheduler
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
        session.report({"metric": config["metric"] * epoch, "epoch": epoch}, checkpoint=checkpoint)


class DummyDataParallelTrainer(DataParallelTrainer):
    def training_loop(self) -> None:
        scaling_config_dataclass = self._validate_and_get_scaling_config_data_class(
            self.scaling_config
        )
        assert scaling_config_dataclass.as_placement_group_factory() == session.get_trial_resources()
        return super().training_loop()


def test_data_parallel_trainer(ray_start_8_cpus):
    trainer = DummyDataParallelTrainer(train_fn, scaling_config=dict(num_workers=2))
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
            scheduler=ResourceChangingScheduler(ASHAScheduler(), resources_allocation_function=DistributeResources(add_bundles=True)),
        ),
    )
    result_grid = tuner.fit()
    assert not any(x.error for x in result_grid)
