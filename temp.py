import ray
from ray.data.datasource import SimpleTensorFlowDatasource
import tensorflow as tf

from tensorflow.keras import datasets, layers, models
import tensorflow_datasets as tfds
import matplotlib.pyplot as plt

def train_dataset_factory():
    return tfds.load("cifar10", split=["train"], as_supervised=True)[0]

def test_dataset_factory():
    return tfds.load("cifar10", split=["test"], as_supervised=True)[0]

train_dataset = ray.data.read_datasource(
    SimpleTensorFlowDatasource(), dataset_factory=train_dataset_factory
)
test_dataset = ray.data.read_datasource(SimpleTensorFlowDatasource(), dataset_factory=test_dataset_factory)


def normalize_images(batch):
    return [(tf.cast(image, tf.float32) / 255.0, label) for image, label in batch]

train_dataset = train_dataset.map_batches(normalize_images)
test_dataset = test_dataset.map_batches(normalize_images)


import pandas as pd


def convert_batch_to_pandas(batch):
    images = [image for image, _ in batch]
    labels = [label for _, label in batch]

    df = pd.DataFrame({"image": images, "label": labels})

    return df


train_dataset = train_dataset.map_batches(convert_batch_to_pandas)
test_dataset = test_dataset.map_batches(convert_batch_to_pandas)

test_dataset


def build_model():
    model = models.Sequential()
    model.add(layers.Conv2D(32, (3, 3), activation='relu', input_shape=(32, 32, 3)))
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Conv2D(64, (3, 3), activation='relu'))
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Conv2D(64, (3, 3), activation='relu'))
    model.add(layers.Flatten())
    model.add(layers.Dense(64, activation='relu'))
    model.add(layers.Dense(10))
    return model


from ray import train
from ray.train.tensorflow import prepare_dataset_shard


def train_loop_per_worker(config):
    dataset_shard = train.get_dataset_shard("train")
    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
    with strategy.scope():
        model = build_model()
        model.compile(optimizer='adam',
                    loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                    metrics=['accuracy'])

    for epoch in range(2):
        tf_dataset = prepare_dataset_shard(
            dataset_shard.to_tf(
                feature_columns=["image"],
                label_column="label",
                output_signature=(
                    tf.TensorSpec(shape=(None, 32, 32, 3), dtype=tf.float32),
                    tf.TensorSpec(shape=(None, 1), dtype=tf.uint8),
                ),
                batch_size=config["batch_size"],
                unsqueeze_label_tensor=True
            )
        )
        model.fit(tf_dataset)
        train.save_checkpoint(epoch=epoch, model_weights=model.get_weights())


from ray.ml.train.integrations.tensorflow import TensorflowTrainer

trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"batch_size": 2},
    datasets={"train": train_dataset},
    scaling_config={"num_workers": 2}
)
result = trainer.fit()
latest_checkpoint = result.checkpoint