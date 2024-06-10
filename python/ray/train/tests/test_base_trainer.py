import logging
import tempfile

import numpy as np
import pytest

import ray
from ray import train, tune
from ray.air.constants import MAX_REPR_LENGTH
from ray.data.context import DataContext
from ray.train import Checkpoint, ScalingConfig
from ray.train.trainer import BaseTrainer
from ray.util.placement_group import get_current_placement_group

logger = logging.getLogger(__name__)


class DummyTrainer(BaseTrainer):
    _scaling_config_allowed_keys = BaseTrainer._scaling_config_allowed_keys + [
        "num_workers",
        "use_gpu",
        "resources_per_worker",
        "placement_strategy",
    ]

    def __init__(self, train_loop, custom_arg=None, **kwargs):
        self.custom_arg = custom_arg
        self.train_loop = train_loop
        super().__init__(**kwargs)

    def training_loop(self) -> None:
        self.train_loop(self)


def test_trainer_fit(ray_start_4_cpus):
    def training_loop(self):
        train.report(dict(my_metric=1))

    trainer = DummyTrainer(train_loop=training_loop)
    result = trainer.fit()
    assert result.metrics["my_metric"] == 1


def test_validate_datasets(ray_start_4_cpus):
    with pytest.raises(ValueError) as e:
        DummyTrainer(train_loop=None, datasets=1)
    assert "`datasets` should be a dict mapping" in str(e.value)

    with pytest.raises(ValueError) as e:
        DummyTrainer(train_loop=None, datasets={"train": 1})
    assert "The Dataset under train key is not a `ray.data.Dataset`"


def test_resources(ray_start_4_cpus):
    def check_cpus(self):
        assert ray.available_resources()["CPU"] == 2

    assert ray.available_resources()["CPU"] == 4
    trainer = DummyTrainer(
        check_cpus,
        scaling_config=ScalingConfig(
            trainer_resources={"CPU": 2}, resources_per_worker={}
        ),
    )
    trainer.fit()


def test_arg_override(ray_start_4_cpus):
    def check_override(self):
        assert self.scaling_config.num_workers == 1
        # Should do deep update.
        assert not self.custom_arg["outer"]["inner"]
        assert self.custom_arg["outer"]["fixed"] == 1

        pg = get_current_placement_group()
        assert len(pg.bundle_specs) == 1  # Merged trainer and worker bundle

    scale_config = ScalingConfig(num_workers=4)
    trainer = DummyTrainer(
        check_override,
        custom_arg={"outer": {"inner": True, "fixed": 1}},
        scaling_config=scale_config,
    )

    new_config = {
        "custom_arg": {"outer": {"inner": False}},
        "scaling_config": ScalingConfig(num_workers=1),
    }

    tune.run(trainer.as_trainable(), config=new_config)


def test_reserved_cpu_warnings_no_cpu_usage(ray_start_1_cpu_1_gpu):
    """Ensure there is no divide by zero error if trial requires no CPUs."""

    def train_loop(config):
        pass

    trainer = DummyTrainer(
        train_loop,
        scaling_config=ScalingConfig(
            num_workers=1, use_gpu=True, trainer_resources={"CPU": 0}
        ),
        datasets={"train": ray.data.range(10)},
    )
    trainer.fit()


def test_setup(ray_start_4_cpus):
    def check_setup(self):
        assert self._has_setup

    class DummyTrainerWithSetup(DummyTrainer):
        def setup(self):
            self._has_setup = True

    trainer = DummyTrainerWithSetup(check_setup)
    trainer.fit()


def test_repr(ray_start_4_cpus):
    def training_loop(self):
        pass

    trainer = DummyTrainer(
        training_loop,
        datasets={
            "train": ray.data.from_items([1, 2, 3]),
        },
    )

    representation = repr(trainer)

    assert "DummyTrainer" in representation
    assert len(representation) < MAX_REPR_LENGTH


def test_metadata_propagation_base(ray_start_4_cpus):
    class MyTrainer(BaseTrainer):
        def training_loop(self):
            assert train.get_context().get_metadata() == {"a": 1, "b": 1}
            with tempfile.TemporaryDirectory() as path:
                checkpoint = Checkpoint.from_directory(path)
                checkpoint.set_metadata({"b": 2, "c": 3})
                train.report(dict(my_metric=1), checkpoint=checkpoint)

    trainer = MyTrainer(metadata={"a": 1, "b": 1})
    result = trainer.fit()
    meta_out = result.checkpoint.get_metadata()
    assert meta_out == {"a": 1, "b": 2, "c": 3}, meta_out


def test_metadata_propagation_data_parallel(ray_start_4_cpus):
    def training_loop(self):
        assert train.get_context().get_metadata() == {"a": 1, "b": 1}
        with tempfile.TemporaryDirectory() as path:
            checkpoint = Checkpoint.from_directory(path)
            checkpoint.set_metadata({"b": 2, "c": 3})
            train.report(dict(my_metric=1), checkpoint=checkpoint)

    trainer = DummyTrainer(training_loop, metadata={"a": 1, "b": 1})
    result = trainer.fit()
    meta_out = result.checkpoint.get_metadata()
    assert meta_out == {"a": 1, "b": 2, "c": 3}, meta_out


def test_data_context_propagation(ray_start_4_cpus):
    ctx = DataContext.get_current()
    # Fake DataContext attribute to propagate to worker.
    ctx.foo = "bar"

    def training_loop(self):
        # Dummy train loop that checks that changes in the driver's
        # DataContext are propagated to the worker.
        ctx_worker = DataContext.get_current()
        assert ctx_worker.foo == "bar"

    trainer = DummyTrainer(
        train_loop=training_loop,
        datasets={"train": ray.data.range(10)},
    )
    trainer.fit()


def test_large_params(ray_start_4_cpus):
    """Tests that large params are not serialized with the trainer actor
    and are instead put into the object store separately."""
    huge_array = np.zeros(shape=int(1e8))

    def training_loop(self):
        huge_array

    trainer = DummyTrainer(training_loop)
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(sys.argv[1:] + ["-v", "-x", __file__]))
