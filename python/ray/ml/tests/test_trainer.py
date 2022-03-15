import os

import pytest

import ray
from ray import tune

from ray.ml.preprocessor import Preprocessor
from ray.ml.trainer import Trainer
from ray.ml.constants import PREPROCESSOR_KEY


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class DummyPreprocessor(Preprocessor):
    def __init__(self):
        self.fit_counter = 0

    def fit(self, ds):
        self.fit_counter += 1

    def transform(self, ds):
        return ds.map(lambda x: x + 1)


class DummyTrainer(Trainer):
    def __init__(self, train_loop, custom_arg=None, **kwargs):
        self.custom_arg = custom_arg
        self.train_loop = train_loop
        super().__init__(**kwargs)

    def training_loop(self) -> None:
        self.train_loop(self)


def test_trainer_fit(ray_start_4_cpus):
    def training_loop(self):
        tune.report(my_metric=1)

    trainer = DummyTrainer(train_loop=training_loop)
    result = trainer.fit()
    assert result.metrics["my_metric"] == 1


def test_preprocess_datasets(ray_start_4_cpus):
    def training_loop(self):
        assert self.datasets["my_dataset"].take() == [2, 3, 4]

    datasets = {"my_dataset": ray.data.from_items([1, 2, 3])}
    trainer = DummyTrainer(
        training_loop, datasets=datasets, preprocessor=DummyPreprocessor()
    )
    trainer.fit()


@pytest.mark.parametrize("gen_dataset", [True, False])
def test_preprocess_fit_on_train(ray_start_4_cpus, gen_dataset):
    def training_loop(self):
        # Fit was only called once.
        assert self.preprocessor.fit_counter == 1
        # Datasets should all be transformed.
        assert self.datasets["train"].take() == [2, 3, 4]
        assert self.datasets["my_dataset"].take() == [2, 3, 4]

    if gen_dataset:
        datasets = {
            "train": lambda: ray.data.from_items([1, 2, 3]),
            "my_dataset": lambda: ray.data.from_items([1, 2, 3]),
        }
    else:
        datasets = {
            "train": ray.data.from_items([1, 2, 3]),
            "my_dataset": ray.data.from_items([1, 2, 3]),
        }
    trainer = DummyTrainer(
        training_loop, datasets=datasets, preprocessor=DummyPreprocessor()
    )
    trainer.fit()


def test_preprocessor_already_fitted(ray_start_4_cpus):
    def training_loop(self):
        # Make sure fit is not called if preprocessor is already fit.
        assert self.preprocessor.fit_counter == 1
        # Datasets should all be transformed.
        assert self.datasets["train"].take() == [2, 3, 4]
        assert self.datasets["my_dataset"].take() == [2, 3, 4]

    datasets = {
        "train": ray.data.from_items([1, 2, 3]),
        "my_dataset": ray.data.from_items([1, 2, 3]),
    }
    preprocessor = DummyPreprocessor()
    preprocessor.fit(ray.data.from_items([1]))
    trainer = DummyTrainer(
        training_loop, datasets=datasets, preprocessor=DummyPreprocessor()
    )
    trainer.fit()


@pytest.mark.skip("Fix postprocess_checkpoint")
def test_preprocessor_in_checkpoint(ray_start_4_cpus):
    """Checks if preprocessor is automatically saved in checkpoint."""
    preprocessor = DummyPreprocessor()
    assert preprocessor.fit_counter == 0

    def training_loop(self):
        with tune.checkpoint_dir(step=0) as dir:
            data = {"x": 1}
            checkpoint_path = os.path.join(dir, "checkpoint")
            with open(checkpoint_path, "wb+") as f:
                import cloudpickle

                cloudpickle.dump(data, f)

    datasets = {"train": ray.data.from_items([1, 2, 3])}
    trainer = DummyTrainer(
        training_loop, datasets=datasets, preprocessor=DummyPreprocessor()
    )

    result = trainer.fit()
    assert result.checkpoint
    checkpoint_dict = result.checkpoint.to_dict()
    assert PREPROCESSOR_KEY in checkpoint_dict
    assert "x" in checkpoint_dict
    loaded_preprocessor = checkpoint_dict[PREPROCESSOR_KEY]

    # The saved preprocessor should be fitted.
    assert loaded_preprocessor.fit_counter == 1

    # User defined checkpoint should still exist.
    assert checkpoint_dict["x"] == 1


def test_arg_override():
    def check_override(self):
        assert self.scaling_config["num_workers"] == 1
        # Should do deep update.
        assert not self.custom_arg["outer"]["inner"]
        assert self.custom_arg["outer"]["fixed"] == 1
        # Should merge with base config.
        assert self.preprocessor.original

    preprocessor = DummyPreprocessor()
    preprocessor.original = True
    scale_config = {"num_workers": 2}
    trainer = DummyTrainer(
        check_override,
        custom_arg={"outer": {"inner": True, "fixed": 1}},
        preprocessor=preprocessor,
        scaling_config=scale_config,
    )

    new_config = {
        "custom_arg": {"outer": {"inner": False}},
        "scaling_config": {"num_workers": 1},
    }

    tune.run(trainer.as_trainable(), config=new_config)


def test_setup():
    def check_setup(self):
        assert self._has_setup

    class DummyTrainerWithSetup(DummyTrainer):
        def setup(self):
            self._has_setup = True

    trainer = DummyTrainerWithSetup(check_setup)
    trainer.fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
