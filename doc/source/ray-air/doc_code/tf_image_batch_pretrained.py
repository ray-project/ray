import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_datasets as tfds

import ray
from ray.air.batch_predictor import BatchPredictor
from ray.data.datasource import SimpleTensorFlowDatasource
from ray.data.extensions import TensorArray
from ray.train.tensorflow import TensorflowPredictor, to_air_checkpoint


def get_dataset(split_type="train"):
    def dataset_factory():
        return tfds.load("mnist", split=[split_type], as_supervised=True)[0].take(128)

    dataset = ray.data.read_datasource(
        SimpleTensorFlowDatasource(), dataset_factory=dataset_factory
    )

    def normalize_images(x):
        x = np.float32(x.numpy()) / 255.0
        x = np.reshape(x, (-1,))
        return x

    def preprocess_dataset(batch):
        return [
            (normalize_images(image), normalize_images(image)) for image, _ in batch
        ]

    dataset = dataset.map_batches(preprocess_dataset)

    def convert_batch_to_pandas(batch):

        images = [TensorArray(image) for image, _ in batch]
        labels = [label for _, label in batch]
        # because we did autoencoder here
        df = pd.DataFrame({"image": images, "label": labels})
        return df

    dataset = dataset.map_batches(convert_batch_to_pandas)
    return dataset


# __batch_prediction_start__
model = tf.keras.models.Sequential([
    tf.keras.layers.Flatten(input_shape=(28, 28)),
    tf.keras.layers.Dense(128, activation="relu"),
    tf.keras.layers.Dense(10)
])

test_dataset = get_dataset(split_type="test")
checkpoint = to_air_checkpoint(model)
batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint, TensorflowPredictor
)
predictions = batch_predictor.predict(
    test_dataset, feature_columns=["image"], dtype=tf.float32
)
pandas_predictions = predictions.to_pandas(float("inf"))
print(f"PREDICTIONS\n{pandas_predictions}")
# __batch_prediction_end__

