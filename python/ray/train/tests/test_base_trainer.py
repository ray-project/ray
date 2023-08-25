import logging
import tempfile
from unittest.mock import patch

import pytest

import ray
from ray import train, tune
from ray.train import ScalingConfig
from ray.train._checkpoint import Checkpoint
from ray.air.constants import MAX_REPR_LENGTH
from ray.tune.impl import tuner_internal
from ray.train.gbdt_trainer import GBDTTrainer
from ray.train.trainer import BaseTrainer
from ray.util.placement_group import get_current_placement_group

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_tuner_internal_logger():
    class MockLogger:
        def __init__(self):
            self.warnings = []

        def warning(self, msg):
            self.warnings.append(msg)

        def warn(self, msg, **kwargs):
            self.warnings.append(msg)

        def info(self, msg):
            print(msg)

        def clear(self):
            self.warnings = []

    old = tuner_internal.warnings
    tuner_internal.warnings = MockLogger()
    yield tuner_internal.warnings
    # The code after the yield will run as teardown code.
    tuner_internal.warnings = old


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


class DummyGBDTTrainer(GBDTTrainer):
    _dmatrix_cls: type = None
    _ray_params_cls: type = None
    _tune_callback_report_cls: type = None
    _tune_callback_checkpoint_cls: type = None
    _init_model_arg_name: str = None


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

    with pytest.raises(ValueError) as e:
        DummyTrainer(
            train_loop=None, datasets={"train": ray.data.from_items([1]).repeat()}
        )
    assert "The Dataset under train key is a `ray.data.DatasetPipeline`."


def test_resources(ray_start_4_cpus):
    def check_cpus(self):
        assert ray.available_resources()["CPU"] == 2

    assert ray.available_resources()["CPU"] == 4
    trainer = DummyTrainer(
        check_cpus, scaling_config=ScalingConfig(trainer_resources={"CPU": 2})
    )
    trainer.fit()


def test_arg_override(ray_start_4_cpus):
    def check_override(self):
        assert self.scaling_config.num_workers == 1
        # Should do deep update.
        assert not self.custom_arg["outer"]["inner"]
        assert self.custom_arg["outer"]["fixed"] == 1

        pg = get_current_placement_group()
        assert len(pg.bundle_specs) == 2  # 1 trainer, 1 worker

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


def test_reserved_cpus(ray_start_4_cpus):
    def train_loop(self):
        ray.data.range(10).show()

    # Will deadlock without reserved CPU fraction.
    scale_config = ScalingConfig(num_workers=1, _max_cpu_fraction_per_node=0.9)
    trainer = DummyTrainer(
        train_loop,
        scaling_config=scale_config,
    )
    tune.run(trainer.as_trainable(), num_samples=4)

    # Needs to request 0 CPU for the trainer otherwise the pg
    # will require {CPU: 1} * 2 resources, which means
    # _max_cpu_fraction_per_node == 0.01 cannot schedule it
    # (because this only allows to have 1 CPU for pg per node).
    scale_config = ScalingConfig(
        num_workers=1, _max_cpu_fraction_per_node=0.01, trainer_resources={"CPU": 0}
    )
    trainer = DummyTrainer(
        train_loop,
        scaling_config=scale_config,
    )
    tune.run(trainer.as_trainable(), num_samples=4)


@patch("ray.available_resources", ray.cluster_resources)
def test_reserved_cpu_warnings(ray_start_4_cpus, mock_tuner_internal_logger):
    # ray.available_resources() is used in the warning logic.
    # We mock it as it can be stochastic due to garbage collection etc.
    # The aim of this test is not to check if ray.available_resources()
    # works correctly, but to test the warning logic.

    def train_loop(config):
        pass

    # Fraction correctly specified.
    trainer = DummyTrainer(
        train_loop,
        scaling_config=ScalingConfig(num_workers=1, _max_cpu_fraction_per_node=0.9),
        datasets={"train": ray.data.range(10)},
    )
    trainer.fit()
    assert not mock_tuner_internal_logger.warnings

    # No datasets, no fraction.
    trainer = DummyTrainer(
        train_loop,
        scaling_config=ScalingConfig(num_workers=1),
    )
    trainer.fit()
    assert not mock_tuner_internal_logger.warnings

    # Should warn.
    trainer = DummyTrainer(
        train_loop,
        scaling_config=ScalingConfig(num_workers=3),
        datasets={"train": ray.data.range(10)},
    )
    trainer.fit()
    assert (
        len(mock_tuner_internal_logger.warnings) == 1
    ), mock_tuner_internal_logger.warnings
    assert "_max_cpu_fraction_per_node" in mock_tuner_internal_logger.warnings[0]
    mock_tuner_internal_logger.clear()

    # Warn if num_samples is configured
    trainer = DummyTrainer(
        train_loop,
        scaling_config=ScalingConfig(num_workers=1),
        datasets={"train": ray.data.range(10)},
    )
    tuner = tune.Tuner(trainer, tune_config=tune.TuneConfig(num_samples=3))
    tuner.fit()
    assert (
        len(mock_tuner_internal_logger.warnings) == 1
    ), mock_tuner_internal_logger.warnings
    assert "_max_cpu_fraction_per_node" in mock_tuner_internal_logger.warnings[0]
    mock_tuner_internal_logger.clear()

    # Don't warn if resources * samples < 0.8
    trainer = DummyTrainer(
        train_loop,
        scaling_config=ScalingConfig(num_workers=1, trainer_resources={"CPU": 0}),
        datasets={"train": ray.data.range(10)},
    )
    tuner = tune.Tuner(trainer, tune_config=tune.TuneConfig(num_samples=3))
    tuner.fit()
    assert not mock_tuner_internal_logger.warnings

    # Don't warn if Trainer is not used
    tuner = tune.Tuner(train_loop, tune_config=tune.TuneConfig(num_samples=3))
    tuner.fit()
    assert not mock_tuner_internal_logger.warnings


def test_reserved_cpu_warnings_no_cpu_usage(
    ray_start_1_cpu_1_gpu, mock_tuner_internal_logger
):
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
    assert not mock_tuner_internal_logger.warnings


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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(sys.argv[1:] + ["-v", "-x", __file__]))
