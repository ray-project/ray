import pytest

import ray
from ray import tune
from ray import train
from ray.ml.constants import PREPROCESSOR_KEY, MODEL_KEY
from ray.ml.config import DataParallelScalingConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.predictors.torch import TorchPredictor
from ray.train.v2.trainer import Trainer, DataParallelFunctionTrainer
from ray.train.v2.trainers.torch import TorchTrainer
from ray.tune.function_runner import wrap_function

from ray.train.examples.train_linear_example import train_func as linear_train_func


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


scale_config = DataParallelScalingConfig(num_workers=2)


class TestTrainer:
    def test_trainer_fit(self, ray_start_4_cpus):
        trainer = DummyTrainer()
        result = trainer.fit()
        assert result.metrics["my_metric"] == 1

    def test_override(self):
        preprocessor = DummyPreprocessor()
        scale_config = DataParallelScalingConfig(num_workers=2)
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

        # Dicts should do deep update.
        assert trainer.run_config == {"outer": {"inner": 2}}

        # Dataclasses should do deep update.
        assert trainer.scaling_config == scale_config
        assert trainer.scaling_config.use_gpu


class TestDataParallelTrainer:
    def test_fit_train_func(self, ray_start_4_cpus):
        def train_func():
            train.report(loss=1)

        trainer = DataParallelFunctionTrainer(
            train_func=train_func, scaling_config=scale_config
        )
        assert trainer.fit().metrics["loss"] == 1

    def test_scale(self, ray_start_4_cpus):
        def train_func():
            assert ray.available_resources()["CPU"] == 1
            train.report(loss=1)

        assert ray.available_resources()["CPU"] == 4
        trainer = DataParallelFunctionTrainer(
            train_func=train_func, scaling_config=scale_config
        )
        trainer.fit()

    def test_fit_train_func_config(self, ray_start_4_cpus):
        def train_func(config):
            train.report(loss=config["x"])

        trainer = DataParallelFunctionTrainer(
            train_func=train_func,
            train_func_config={"x": 100},
            scaling_config=scale_config,
        )
        assert trainer.fit().metrics["loss"] == 100

    def test_datasets(self, ray_start_4_cpus):
        """Checks that Dataset is correctly sharded even with multiple epochs."""
        num_data = 10
        num_epochs = 2

        dataset = ray.data.range(num_data)

        def get_dataset():
            data_all_epochs = []
            for _ in range(num_epochs):
                data_this_epoch = []
                dataset = train.get_train_dataset_shard()
                for batch in dataset.iter_batches():
                    data_this_epoch.extend(batch)
                data_all_epochs.append(data_this_epoch)
            train.report(data=data_all_epochs)

        trainer = DataParallelFunctionTrainer(
            train_func=get_dataset, scaling_config=scale_config, train_dataset=dataset
        )
        result = trainer.fit()
        rank_zero_shards = result.metrics["data"]
        for epoch_shard in rank_zero_shards:
            assert len(epoch_shard) == num_data / scale_config.num_workers

    def test_multiple_datasets(self, ray_start_4_cpus):
        num_train_data = 10
        num_val_data = 6

        train_dataset = ray.data.range(num_train_data)
        val_dataset = ray.data.range(num_val_data)

        def get_dataset():
            train_dataset = train.get_train_dataset_shard()
            assert train_dataset.count() == num_train_data / scale_config.num_workers
            val_dataset = train.get_dataset_shard("val")
            assert val_dataset.count() == num_val_data / scale_config.num_workers

        trainer = DataParallelFunctionTrainer(
            train_func=get_dataset,
            scaling_config=scale_config,
            train_dataset=train_dataset,
            additional_datasets={"val": val_dataset},
        )
        trainer.fit()

    def test_preprocessor(self, ray_start_4_cpus):
        train_dataset = ray.data.from_items([1, 2, 3])
        val_dataset = ray.data.from_items([1, 2, 3])

        # Use only 1 worker.
        scale_config = DataParallelScalingConfig(num_workers=1)

        preprocessor = DummyPreprocessor()

        def get_dataset():
            train_dataset = train.get_train_dataset_shard()
            assert train_dataset.count() == 3
            assert set(train_dataset.take(3)) == {2, 4, 6}
            val_dataset = train.get_dataset_shard("val")
            assert val_dataset.count() == 3
            assert set(val_dataset.take(3)) == {3, 4, 5}

        trainer = DataParallelFunctionTrainer(
            train_func=get_dataset,
            scaling_config=scale_config,
            train_dataset=train_dataset,
            additional_datasets={"val": val_dataset},
            preprocessor=preprocessor,
        )
        trainer.fit()

    def test_checkpoint(self, ray_start_4_cpus):
        def train_func():
            for i in range(3):
                train.save_checkpoint(model=i)

        trainer = DataParallelFunctionTrainer(
            train_func=train_func, scaling_config=scale_config
        )
        result = trainer.fit()
        assert result.checkpoint.to_dict()["model"] == 2

    def test_preprocessor_checkpointed(self, ray_start_4_cpus):
        def train_func():
            for i in range(3):
                train.save_checkpoint(model=i)

        preprocessor = DummyPreprocessor()

        trainer = DataParallelFunctionTrainer(
            train_func=train_func,
            scaling_config=scale_config,
            preprocessor=preprocessor,
        )
        result = trainer.fit()
        assert result.checkpoint.to_dict()["model"] == 2
        assert type(result.checkpoint.to_dict()[PREPROCESSOR_KEY]) == DummyPreprocessor

    def test_resume_from_checkpoint(self, ray_start_4_cpus):
        def train_func():
            checkpoint = train.load_checkpoint()
            if checkpoint:
                epoch = checkpoint["epoch"]
            else:
                epoch = 0
            for i in range(epoch, epoch + 2):
                train.save_checkpoint(epoch=i)

        trainer = DataParallelFunctionTrainer(
            train_func=train_func, scaling_config=scale_config
        )
        result = trainer.fit()
        assert result.checkpoint.to_dict()["epoch"] == 1

        trainer = DataParallelFunctionTrainer(
            train_func=train_func,
            scaling_config=scale_config,
            resume_from_checkpoint=result.checkpoint,
        )
        result = trainer.fit()
        assert result.checkpoint.to_dict()["epoch"] == 2

    def test_tune(self, ray_start_4_cpus):
        def train_func(config):
            train.report(loss=config["x"])

        trainer = DataParallelFunctionTrainer(
            train_func=train_func,
            train_func_config={"x": 100},
            scaling_config=scale_config,
        )
        analysis = tune.run(
            trainer.as_trainable(), config={"x": tune.choice([200, 300])}, num_samples=2
        )
        assert analysis.trials[0].last_result["loss"] in [200, 300]

        # Make sure original Trainer is not affected.
        assert trainer.train_func_config["x"] == 100


class TestTorchTrainer:
    @pytest.mark.parametrize("num_workers", [1, 2])
    def test_torch_linear(self, ray_start_4_cpus, num_workers):
        def train_func(config):
            result = linear_train_func(config)
            assert len(result) == epochs
            assert result[-1]["loss"] < result[0]["loss"]

        num_workers = num_workers
        epochs = 3
        scaling_config = DataParallelScalingConfig(num_workers=num_workers)
        config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
        trainer = TorchTrainer(
            train_func=train_func,
            train_func_config=config,
            scaling_config=scaling_config,
        )
        trainer.fit()

    def test_torch_predict(self, ray_start_4_cpus):
        import torch

        def train_func():
            model = torch.nn.Linear(1, 1)
            train.save_checkpoint({MODEL_KEY: model})

        scaling_config = DataParallelScalingConfig(num_workers=2)
        trainer = TorchTrainer(train_func=train_func, scaling_config=scaling_config)
        result = trainer.fit()

        predictor = TorchPredictor.from_checkpoint(result.checkpoint)
        predictions = predictor.predict(
            ray.data.range(3), feature_column_dtypes=torch.float
        )
        assert len(predictions) == 3

    def test_torch_predict_state_dict(self, ray_start_4_cpus):
        import torch

        def train_func():
            model = torch.nn.Linear(1, 1).state_dict()
            train.save_checkpoint({MODEL_KEY: model})

        scaling_config = DataParallelScalingConfig(num_workers=2)
        trainer = TorchTrainer(train_func=train_func, scaling_config=scaling_config)
        result = trainer.fit()

        # If loading from a state dict, a model definition must be passed in.
        with pytest.raises(RuntimeError):
            TorchPredictor.from_checkpoint(result.checkpoint)

        predictor = TorchPredictor.from_checkpoint(
            result.checkpoint, model_definition=torch.nn.Linear(1, 1)
        )

        predict_dataset = ray.data.range(3)
        predictions = predict_dataset.map_batches(
            lambda batch: predictor.predict(batch, dtype=torch.float),
            batch_format="pandas",
        )
        assert predictions.count() == 3


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
