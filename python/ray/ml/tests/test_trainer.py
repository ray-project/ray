import pytest

import ray
from ray import tune

from ray.ml.preprocessor import Preprocessor
from ray.ml.trainer import Trainer


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


def test_resources(ray_start_4_cpus):
    def check_cpus(self):
        assert ray.available_resources()["CPU"] == 2

    assert ray.available_resources()["CPU"] == 4
    trainer = DummyTrainer(check_cpus, scaling_config={"trainer_resources": {"CPU": 2}})
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


def test_arg_override(ray_start_4_cpus):
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


def test_setup(ray_start_4_cpus):
    def check_setup(self):
        assert self._has_setup

    class DummyTrainerWithSetup(DummyTrainer):
        def setup(self):
            self._has_setup = True

    trainer = DummyTrainerWithSetup(check_setup)
    trainer.fit()


def test_fail(ray_start_4_cpus):
    def fail(self):
        raise ValueError

    trainer = DummyTrainer(fail)
    with pytest.raises(ValueError):
        trainer.fit()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
