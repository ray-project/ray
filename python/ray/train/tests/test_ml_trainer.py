import pytest

import ray
from ray import tune

from ray.ml.preprocessor import Preprocessor
from ray.train.ml_trainer import Trainer
from ray.tune.function_runner import wrap_function


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class DummyPreprocessor(Preprocessor):
    def fit_transform(self, ds):
        return ds.map(lambda x: x * 2)

    def transform(self, ds):
        return ds.map(lambda x: x + 2)


class DummyTrainer(Trainer):
    def as_trainable(self):
        def train_func(config):
            tune.report(my_metric=1)

        return wrap_function(train_func)


class TestTrainer:
    def test_trainer_fit(self, ray_start_4_cpus):
        trainer = DummyTrainer()
        result = trainer.fit()
        assert result.metrics["my_metric"] == 1

    def test_override(self):
        preprocessor = DummyPreprocessor()
        scale_config = {"num_workers": 2, "use_gpu": False}
        trainer = DummyTrainer(
            run_config={"outer": {"inner": 1}},
            preprocessor=preprocessor,
            scaling_config=scale_config,
        )
        new_config = {
            "run_config": {"outer": {"inner": 2}},
            "preprocessor": DummyPreprocessor(),
            "scaling_config": {"use_gpu": True},
        }

        trainer._override_attributes_with_config(new_config)
        assert trainer.preprocessor is not preprocessor

        assert trainer.scaling_config == {"num_workers": 2, "use_gpu": True}

        # Dicts should do deep update.
        assert trainer.run_config == {"outer": {"inner": 2}}


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
