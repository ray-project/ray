import ray
import tensorflow as tf
from typing import Dict

IMG_SIZE = 224
NUM_CLASSES = 10

import numpy as np
from tensorflow.keras import layers
from tensorflow.keras.applications import EfficientNetB0

from ray.data.datasource.partitioning import Partitioning

ray.data.context.DatasetContext.get_current().use_streaming_executor = True

def get_dataset_for_split(split: str):
    data_folder = f"s3://anonymous@air-example-data/food-101-tiny/{split}"
    partitioning = Partitioning(
        "dir", field_names=["class"], base_dir=data_folder
    )

    def resize(batch: Dict[str, np.ndarray]):
        batch["image"] = tf.convert_to_tensor(batch["image"], dtype=tf.uint8)
        batch["image"] = tf.image.resize(batch["image"], (IMG_SIZE, IMG_SIZE)).numpy()
        return batch

    return ray.data.read_images(
        data_folder, size=(512, 512), partitioning=partitioning, mode="RGB"
    ).map_batches(resize, batch_format="numpy").random_shuffle()


train_ds, valid_ds = [get_dataset_for_split(split) for split in ("train", "valid")]

labels = valid_ds.groupby("class").count().to_pandas()
class_to_idx = {
    class_name: i
    for i, class_name in enumerate(labels["class"])
}

TRAIN_DS_LENGTH = int(train_ds.count())
VALID_DS_LENGTH = int(valid_ds.count())
NUM_WORKERS = 1

from ray.data.preprocessors import BatchMapper

def build_preprocessor():
    # 1. Map the image folder names to label ids
    def map_labels(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["label"] = np.vectorize(class_to_idx.get)(batch["class"])
        return {"image": batch["image"], "label": batch["label"]}

    label_preprocessor = BatchMapper(map_labels, batch_format="numpy")
    return label_preprocessor


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
        model = build_model()
        optimizer = tf.keras.optimizers.Adam()
        loss_object = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
        model.compile(
            optimizer=optimizer,
            loss=loss_object,
            metrics=["accuracy"],
        )

    # 3. Shard the dataset across `session.get_world_size()` workers
    train_ray_ds = session.get_dataset_shard("train")
    train_ds = train_ray_ds.to_tf(
        feature_columns=["image"],
        label_columns=["label"],
        # batch_size=batch_size_per_worker,
        # local_shuffle_buffer_size=256,
    )
    # train_ds_length = TRAIN_DS_LENGTH // NUM_WORKERS // batch_size_per_worker
    train_ds = train_ds.apply(tf.data.experimental.assert_cardinality(train_ray_ds.count()))
    train_ds = train_ds.map(lambda image, label: (tf.image.resize(image["image"], (IMG_SIZE, IMG_SIZE)), label["label"]))

    valid_ray_ds = session.get_dataset_shard("valid")
    valid_ds = valid_ray_ds.to_tf(feature_columns=["image"], label_columns=["label"])
    valid_ds = valid_ds.apply(tf.data.experimental.assert_cardinality(valid_ray_ds.count()))
    valid_ds = valid_ds.map(lambda image, label: (tf.image.resize(image["image"], (IMG_SIZE, IMG_SIZE)), label["label"]))

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
            f"# test batches per worker = {len(valid_ds)} "
            f"(~{len(valid_ds) * batch_size_per_worker} samples)"
        )
  
    # 4. Report metrics and checkpoint the model
    report_metrics_and_checkpoint_callback = ReportCheckpointCallback(
        report_metrics_on="epoch_end",
        checkpoint_on="epoch_end"
    )
    model.fit(
        train_ds,
        epochs=epochs,
        callbacks=[report_metrics_and_checkpoint_callback],
        verbose=(0 if session.get_world_rank() != 0 else 2),
    )

    eval_result = model.evaluate(valid_ds, return_dict=True, verbose=0)
    test_loss = eval_result["loss"]
    test_accuracy = eval_result["accuracy"]
    if session.get_world_rank() == 0:
        print(
            f"Final Test Loss: {test_loss:.4f}, "
            f"Final Test Accuracy: {test_accuracy:.4f}"
        )



from ray import air
from ray.train.tensorflow import TensorflowTrainer

use_gpu = True

trainer = TensorflowTrainer(
    train_loop_per_worker=train_func,
    train_loop_config={
        "batch_size": 125,
        "epochs": 10,
    },
    datasets={"train": train_ds, "valid": valid_ds},
    preprocessor=build_preprocessor(),
    scaling_config=air.ScalingConfig(
        num_workers=NUM_WORKERS,
        use_gpu=use_gpu,
        trainer_resources={"CPU": 0},
        resources_per_worker={"CPU": 5.0, "GPU": 1.0},
    ),
)
result = trainer.fit()

metrics = result.metrics or {}
for name, val in metrics.items():
    print(name, ":", val)
