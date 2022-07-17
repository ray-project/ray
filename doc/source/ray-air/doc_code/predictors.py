# flake8: noqa
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

data = np.array([1, 2, 3, 4])
predictions = predictor.predict(data)
print(predictions)
# [[-1.6930283]
#  [-3.3860567]
#  [-5.079085 ]
#  [-6.7721133]]
# __use_predictor_end__

# __batch_prediction_start__
from ray.train.batch_predictor import BatchPredictor

batch_predictor = BatchPredictor(
    checkpoint, TensorflowPredictor, model_definition=build_model
)
predict_dataset = ray.data.range(3)
predictions = batch_predictor.predict(predict_dataset)
print(predictions.show())
# {'predictions': array([0.], dtype=float32)}
# {'predictions': array([-0.6512989], dtype=float32)}
# {'predictions': array([-1.3025978], dtype=float32)}
# __batch_prediction_end__

# __scoring_start__
import pandas as pd
import ray
from ray.air import Checkpoint
from ray.train.predictor import Predictor
from ray.train.batch_predictor import BatchPredictor

# Create a dummy predictor that returns identity as the predictions.
class DummyPredictor(Predictor):
    @classmethod
    def from_checkpoint(cls, checkpoint, **kwargs):
        return cls(**kwargs)

    def _predict_pandas(self, data_df, **kwargs):
        return pd.DataFrame({"predictions": data_df["feature_1"]})


# Create a batch predictor for this dummy predictor.
batch_pred = BatchPredictor(Checkpoint.from_dict({"x": 0}), DummyPredictor)

# Create a dummy dataset.
ds = ray.data.from_pandas(pd.DataFrame({"feature_1": [1, 2, 3], "label": [1, 2, 3]}))

predictions = batch_pred.predict(
    ds, feature_columns=["feature_1"], keep_columns=["label"]
)
print(predictions.show())
# {'predictions': 1, 'label': 1}
# {'predictions': 2, 'label': 2}
# {'predictions': 3, 'label': 3}

# Calculate final accuracy.
def calculate_accuracy(df):
    return pd.DataFrame({"correct": df["predictions"] == df["label"]})


correct = predictions.map_batches(calculate_accuracy)
print("Final accuracy: ", correct.mean(on="correct"))
# Final accuracy:  1.0
# __scoring_end__

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
ds = ray.data.range_tensor(200, parallelism=4)
# Setup a prediction pipeline.
pipeline = batch_pred.predict_pipelined(ds, blocks_per_window=1)
for batch in pipeline.iter_batches():
    print("Pipeline result", batch)
    # 0    42
    # 1    42
    # ...
# __pipelined_prediction_end__
