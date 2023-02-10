from typing import Dict, Tuple
from unittest.mock import patch

import pytest
import numpy as np
import tensorflow as tf

import ray
from ray.air import session
from ray.air.integrations.keras import Callback, ReportCheckpointCallback
from ray.train.constants import TRAIN_DATASET_KEY
from ray.air.config import ScalingConfig
from ray.train.tensorflow import (
    TensorflowCheckpoint,
    TensorflowTrainer,
    TensorflowPredictor,
)


class TestReportCheckpointCallback:
    @pytest.fixture(name="model")
    def model_fixture(self):
        model = tf.keras.Sequential(
            [tf.keras.layers.InputLayer(input_shape=(1,)), tf.keras.layers.Dense(1)]
        )
        model.compile(
            optimizer="sgd",
            loss="mean_squared_error",
            metrics=["accuracy"],
        )
        return model

    @patch("ray.air.session.report")
    @pytest.mark.parametrize(
        "metrics, expected_metrics_keys",
        [
            (None, {"loss", "accuracy", "val_loss", "val_accuracy"}),
            ("loss", {"loss"}),
            (["loss", "accuracy"], {"loss", "accuracy"}),
            ({"spam": "loss"}, {"spam"}),
        ],
    )
    def test_reported_metrics_contain_expected_keys(
        self, mock_report, metrics, expected_metrics_keys, model
    ):
        # Reported metrics contain different keys depending on the value passed to the
        # `metrics` parameter. This test varies the value of `metrics` and asserts that
        # the reported keys are correct.
        model.fit(
            x=np.zeros((1, 1)),
            y=np.zeros((1, 1)),
            validation_data=(np.zeros((1, 1)), np.zeros((1, 1))),
            callbacks=[ReportCheckpointCallback(metrics=metrics)],
        )

        for (metrics,), _ in ray.air.session.report.call_args_list:
            assert metrics.keys() == expected_metrics_keys

    @patch("ray.air.session.report")
    def test_report_with_default_arguments(self, mock_report, model):
        # This tests `ReportCheckpointCallback` with default arguments. The test
        # simulates the end of an epoch, and asserts that a metric and checkpoint are
        # reported.
        callback = ReportCheckpointCallback()
        callback.model = model

        callback.on_epoch_end(0, {"loss": 0})

        assert len(ray.air.session.report.call_args_list) == 1
        metrics, checkpoint = self.parse_call(ray.air.session.report.call_args_list[0])
        assert metrics == {"loss": 0}
        assert checkpoint is not None

    @patch("ray.air.session.report")
    def test_checkpoint_on_list(self, mock_report, model):
        # This tests `ReportCheckpointCallback` when `checkpoint_on` is a `list`. The
        # test simulates each event in `checkpoint_on`, and asserts that a checkpoint
        # is reported for each event.
        callback = ReportCheckpointCallback(
            checkpoint_on=["epoch_end", "train_batch_end"]
        )
        callback.model = model

        callback.on_train_batch_end(0, {"loss": 0})
        callback.on_epoch_end(0, {"loss": 0})

        assert len(ray.air.session.report.call_args_list) == 2
        _, first_checkpoint = self.parse_call(ray.air.session.report.call_args_list[0])
        assert first_checkpoint is not None
        _, second_checkpoint = self.parse_call(ray.air.session.report.call_args_list[0])
        assert second_checkpoint is not None

    @patch("ray.air.session.report")
    def test_report_metrics_on_list(self, mock_report, model):
        # This tests `ReportCheckpointCallback` when `report_metrics_on` is a `list`.
        # The test simulates each event in `report_metrics_on`, and asserts that metrics
        # are reported for each event.
        callback = ReportCheckpointCallback(
            report_metrics_on=["epoch_end", "train_batch_end"]
        )
        callback.model = model

        callback.on_train_batch_end(0, {"loss": 0})
        callback.on_epoch_end(0, {"loss": 1})

        assert len(ray.air.session.report.call_args_list) == 2
        first_metric, _ = self.parse_call(ray.air.session.report.call_args_list[0])
        assert first_metric == {"loss": 0}
        second_metric, _ = self.parse_call(ray.air.session.report.call_args_list[1])
        assert second_metric == {"loss": 1}

    @patch("ray.air.session.report")
    def test_report_and_checkpoint_on_different_events(self, mock_report, model):
        # This tests `ReportCheckpointCallback` when `report_metrics_on` and
        # `checkpoint_on` are different. The test asserts that:
        # 1. Checkpoints are reported on `checkpoint_on`
        # 2. Metrics are reported on `report_metrics_on`
        # 3. Metrics are reported with checkpoints
        callback = ReportCheckpointCallback(
            report_metrics_on="train_batch_end", checkpoint_on="epoch_end"
        )
        callback.model = model

        callback.on_train_batch_end(0, {"loss": 0})
        callback.on_epoch_end(0, {"loss": 1})

        assert len(ray.air.session.report.call_args_list) == 2
        first_metric, first_checkpoint = self.parse_call(
            ray.air.session.report.call_args_list[0]
        )
        assert first_metric == {"loss": 0}
        assert first_checkpoint is None
        second_metric, second_checkpoint = self.parse_call(
            ray.air.session.report.call_args_list[1]
        )
        # We should always include metrics, even if it isn't during one of the events
        # specified in `report_metrics_on`.
        assert second_metric == {"loss": 1}
        assert second_checkpoint is not None

    def parse_call(self, call) -> Tuple[Dict, ray.air.Checkpoint]:
        (metrics,), kwargs = call
        checkpoint = kwargs["checkpoint"]
        return metrics, checkpoint


def get_dataset(a=5, b=10, size=1000):
    items = [i / size for i in range(size)]
    dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
    return dataset


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            # Add feature dimension, expanding (batch_size,) to (batch_size, 1).
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(10),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


def train_func(config: dict):
    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_model()
        multi_worker_model.compile(
            optimizer=tf.keras.optimizers.SGD(learning_rate=config.get("lr", 1e-3)),
            loss=tf.keras.losses.mean_squared_error,
            metrics=[tf.keras.metrics.mean_squared_error],
        )

    dataset = session.get_dataset_shard("train")

    for _ in range(config.get("epoch", 3)):
        tf_dataset = dataset.to_tf("x", "y", batch_size=32)
        multi_worker_model.fit(tf_dataset, callbacks=[Callback()])


def test_keras_callback_e2e():
    epochs = 3
    config = {
        "epochs": epochs,
    }
    trainer = TensorflowTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={TRAIN_DATASET_KEY: get_dataset()},
    )
    checkpoint = trainer.fit().checkpoint
    assert isinstance(checkpoint, TensorflowCheckpoint)
    assert checkpoint._flavor == TensorflowCheckpoint.Flavor.MODEL_WEIGHTS

    predictor = TensorflowPredictor.from_checkpoint(
        checkpoint, model_definition=build_model
    )

    items = np.random.uniform(0, 1, size=(10, 1))
    predictor.predict(data=items)


def test_keras_callback_is_deprecated():
    with pytest.warns(DeprecationWarning):
        Callback()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
