# flake8: noqa
# isort: skip_file

# __use_predictor_start__
import numpy as np
import tensorflow as tf

import ray
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import (
    TensorflowCheckpoint,
    TensorflowPredictor,
)


def build_model() -> tf.keras.Model:
    model = tf.keras.Sequential(
        [
            tf.keras.layers.InputLayer(input_shape=()),
            # Add feature dimension, expanding (batch_size,) to (batch_size, 1).
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(1),
        ]
    )
    return model


model = build_model()
checkpoint = TensorflowCheckpoint.from_model(model)
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
import pandas as pd
from ray.train.batch_predictor import BatchPredictor

batch_predictor = BatchPredictor(
    checkpoint, TensorflowPredictor, model_definition=build_model
)
# Create a dummy dataset.
ds = ray.data.from_pandas(pd.DataFrame({"feature_1": [1, 2, 3], "label": [1, 2, 3]}))

# Use `feature_columns` to specify the input columns to your model.
predictions = batch_predictor.predict(ds, feature_columns=["feature_1"])
print(predictions.show())
# {'predictions': array([-1.2789773], dtype=float32)}
# {'predictions': array([-2.5579545], dtype=float32)}
# {'predictions': array([-3.8369317], dtype=float32)}
# __batch_prediction_end__

# __compute_accuracy_start__
def calculate_accuracy(df):
    return pd.DataFrame({"correct": int(df["predictions"][0]) == df["label"]})


predictions = batch_predictor.predict(
    ds, feature_columns=["feature_1"], keep_columns=["label"]
)
print(predictions.show())
# {'predictions': array([-1.2789773], dtype=float32), 'label': 0}
# {'predictions': array([-2.5579545], dtype=float32), 'label': 1}
# {'predictions': array([-3.8369317], dtype=float32), 'label': 0}

correct = predictions.map_batches(calculate_accuracy)
print("Final accuracy: ", correct.mean(on="correct"))
# Final accuracy:  0.5
# __compute_accuracy_end__

# __pipelined_prediction_start__
import pandas as pd
import ray
from ray.air import Checkpoint
from ray.train.predictor import Predictor
from ray.train.batch_predictor import BatchPredictor

# Create a BatchPredictor that always returns `42` for each input.
batch_pred = BatchPredictor.from_pandas_udf(
    lambda data: pd.DataFrame({"a": [42] * len(data)})
)

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
