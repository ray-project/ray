import numpy as np
import pandas as pd

import ray


dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
dataset = dataset.drop_columns("target")

# All columns are features.
num_features = len(dataset.schema().names)


# Concatenate the features to a single Numpy array.
def concatenate(batch: pd.DataFrame):
    concatenated_features = batch.to_numpy()
    return concatenated_features


dataset = dataset.map_batches(concatenate, batch_format="pandas")


# Define the model class for prediction.
class TFModel:
    def __init__(self):
        from tensorflow import keras  # this is needed for tf<2.9
        from keras import layers

        # Use a standard 3 layer feed-forward neural network.
        self.model = keras.Sequential(
            [
                keras.Input(shape=(num_features,)),
                layers.Dense(16, activation="relu"),
                layers.Dense(16, activation="relu"),
                layers.Dense(1, activation="sigmoid"),
            ]
        )

    def __call__(self, batch: np.ndarray):
        return self.model(batch).numpy()


# Predict on the features.
predicted_probabilities = dataset.map_batches(
    TFModel, compute=ray.data.ActorPoolStrategy(size=2), batch_format="numpy"
)

# Call show on the output probabilities to trigger execution.
predicted_probabilities.show(limit=4)
# [1.]
# [1.]
# [1.]
# [1.]
