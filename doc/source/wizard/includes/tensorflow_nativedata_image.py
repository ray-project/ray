import tensorflow as tf

import boto3
from pathlib import Path
import os

import tensorflow as tf
import tensorflow_datasets as tfds


def download_dataset_from_s3(destination_dir: str):
    destination_path = Path(destination_dir).resolve()
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket("air-example-data")
    for obj in bucket.objects.filter(Prefix="food-101-tiny"):
        os.makedirs(os.path.dirname(obj.key), exist_ok=True)
        bucket.download_file(obj.key, str(destination_path / obj.key))


def get_train_test_datasets(img_size, global_batch_size):
    if not os.path.exists("./food-101-tiny"):
        download_dataset_from_s3(destination_dir=".")

    builder = tfds.ImageFolder("./food-101-tiny")
    train_ds = builder.as_dataset(
        split="train", shuffle_files=False, batch_size=global_batch_size
    )
    train_ds = train_ds.map(
        lambda data: (
            tf.image.resize(data["image"], (img_size, img_size)),
            data["label"],
        )
    )
    valid_ds = builder.as_dataset(
        split="valid", shuffle_files=True, batch_size=global_batch_size
    )
    valid_ds = valid_ds.map(
        lambda data: (
            tf.image.resize(data["image"], (img_size, img_size)),
            data["label"],
        )
    )
    return train_ds, valid_ds


import ray

ray.init(runtime_env={"pip": ["tfds-nightly"]})

IMG_SIZE = 224
NUM_CLASSES = 10

train_ds, valid_ds = get_train_test_datasets(IMG_SIZE, 64)

import ipdb

ipdb.set_trace()


from tensorflow.keras import layers
from tensorflow.keras.applications import EfficientNetB0


def build_model():
    inputs = layers.Input(shape=(IMG_SIZE, IMG_SIZE, 3))
    # x = img_augmentation(inputs)
    x = inputs
    model = EfficientNetB0(include_top=False, input_tensor=x, weights="imagenet")

    # Freeze the pretrained weights
    model.trainable = True

    # Rebuild top
    x = layers.GlobalAveragePooling2D(name="avg_pool")(model.output)
    x = layers.BatchNormalization()(x)

    top_dropout_rate = 0.2
    x = layers.Dropout(top_dropout_rate, name="top_dropout")(x)
    outputs = layers.Dense(NUM_CLASSES, activation="linear", name="pred")(x)

    # Compile
    model = tf.keras.Model(inputs, outputs, name="EfficientNet")
    return model


from ray.air import session
from ray.air.integrations.keras import ReportCheckpointCallback

# 1. Pass in the hyperparameter config
def train_func(config: dict):
    epochs = config.get("epochs", 5)
    batch_size_per_worker = config.get("batch_size", 64)

    # 2. Synchronized model setup
    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        loss_object = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
        optimizer = tf.keras.optimizers.Adam()
        model = build_model()
        model.compile(optimizer=optimizer, loss=loss_object, metrics=["accuracy"])

    # 3. Shard the dataset across `session.get_world_size()` workers
    global_batch_size = batch_size_per_worker * session.get_world_size()
    train_ds, test_ds = get_train_test_datasets(IMG_SIZE, global_batch_size)

    if session.get_world_rank() == 0:
        print(f"\nDataset is sharded across {session.get_world_size()} workers:")
        # The number of samples is approximate, because is not always
        # a multiple of batch_size, so some batches could contain fewer than
        # `batch_size_per_worker` samples.
        print(
            f"# training batches per worker = {len(train_ds)} "
            f"(~{len(train_ds) * batch_size_per_worker} samples)"
        )
        print(
            f"# test batches per worker = {len(test_ds)} "
            f"(~{len(test_ds) * batch_size_per_worker} samples)"
        )

    # 4. Report metrics and checkpoint the model
    report_metrics_and_checkpoint_callback = ReportCheckpointCallback(
        report_metrics_on="epoch_end", checkpoint_on="epoch_end"
    )
    model.fit(
        train_ds,
        epochs=epochs,
        callbacks=[report_metrics_and_checkpoint_callback],
        verbose=(0 if session.get_world_rank() != 0 else 2),
    )

    eval_result = model.evaluate(test_ds, return_dict=True, verbose=0)
    test_loss = eval_result["loss"]
    test_accuracy = eval_result["accuracy"]
    if session.get_world_rank() == 0:
        print(
            f"Final Test Loss: {test_loss:.4f}, "
            f"Final Test Accuracy: {test_accuracy:.4f}"
        )


from ray import air
from ray.train.tensorflow import TensorflowTrainer

num_workers = 2
use_gpu = True

trainer = TensorflowTrainer(
    train_loop_per_worker=train_func,
    train_loop_config={
        "batch_size": 32,
        "epochs": 4,
    },
    scaling_config=air.ScalingConfig(
        num_workers=num_workers,
        use_gpu=use_gpu,
        trainer_resources={"CPU": 0},
        resources_per_worker={"CPU": 5.0, "GPU": 1.0},
    ),
)
result = trainer.fit()

metrics = result.metrics or {}
for name, val in metrics.items():
    print(name, ":", val)
