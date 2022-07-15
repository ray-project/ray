# isort: skip_file

# __use_predictor_start__
import numpy as np
import tensorflow as tf

import ray
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    to_air_checkpoint,
    TensorflowPredictor,
)

# This can be a trained model.
def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


model = build_model()
checkpoint = to_air_checkpoint(model)
predictor = TensorflowPredictor.from_checkpoint(
    checkpoint, model_definition=build_model
)

data = np.array([[1, 2], [3, 4]])
predictions = predictor.predict(data)
# __use_predictor_end__

# __batch_prediction_start__
from ray.train.batch_predictor import BatchPredictor

batch_predictor = BatchPredictor(
    checkpoint, TensorflowPredictor, model_definition=build_model
)
predict_dataset = ray.data.range(3)
predictions = batch_predictor.predict(predict_dataset)
# __batch_prediction_end__

# __pipelined_prediction_start__
import pandas as pd
import ray
from ray.air import Checkpoint
from ray.train.predictor import Predictor
from ray.train.batch_predictor import BatchPredictor

# Create a dummy predictor that always returns `42` for each input.
class DummyPredictor(Predictor):
    @classmethod
    def from_checkpoint(cls, checkpoint, **kwargs):
        return DummyPredictor()

    def predict(self, data, **kwargs):
        return pd.DataFrame({"a": [42] * len(data)})


# Create a batch predictor for this dummy predictor.
batch_pred = BatchPredictor(Checkpoint.from_dict({"x": 0}), DummyPredictor)
# Create a dummy dataset.
ds = ray.data.range_tensor(1000, parallelism=4)
# Setup a prediction pipeline.
print(batch_pred.predict_pipelined(ds, blocks_per_window=1))
# > DatasetPipeline(num_windows=4, num_stages=3)
# __pipelined_prediction_end__


# __use_pretrained_model_start__
import ray
import tensorflow as tf
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    to_air_checkpoint,
    TensorflowPredictor,
)


# This can be a trained model
def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=(1,)),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


model = build_model()
checkpoint = to_air_checkpoint(model)
batch_predictor = BatchPredictor(
    checkpoint, TensorflowPredictor, model_definition=build_model
)
predict_dataset = ray.data.range(3)
predictions = batch_predictor.predict(predict_dataset)

# __use_pretrained_model_end__
