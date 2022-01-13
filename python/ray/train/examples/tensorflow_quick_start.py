# flake8: noqa
# yapf: disable

# __tf_setup_begin__

import numpy as np
import tensorflow as tf


def mnist_dataset(batch_size):
    (x_train, y_train), _ = tf.keras.datasets.mnist.load_data()
    # The `x` arrays are in uint8 and have values in the [0, 255] range.
    # You need to convert them to float32 with values in the [0, 1] range.
    x_train = x_train / np.float32(255)
    y_train = y_train.astype(np.int64)
    train_dataset = tf.data.Dataset.from_tensor_slices(
        (x_train, y_train)).shuffle(60000).repeat().batch(batch_size)
    return train_dataset


def build_and_compile_cnn_model():
    model = tf.keras.Sequential([
        tf.keras.layers.InputLayer(input_shape=(28, 28)),
        tf.keras.layers.Reshape(target_shape=(28, 28, 1)),
        tf.keras.layers.Conv2D(32, 3, activation='relu'),
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dense(10)
    ])
    model.compile(
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        optimizer=tf.keras.optimizers.SGD(learning_rate=0.001),
        metrics=['accuracy'])
    return model

# __tf_setup_end__

# __tf_single_begin__

def train_func():
    batch_size = 64
    single_worker_dataset = mnist_dataset(batch_size)
    single_worker_model = build_and_compile_cnn_model()
    single_worker_model.fit(single_worker_dataset, epochs=3, steps_per_epoch=70)

# __tf_single_end__

# __tf_distributed_begin__

import json
import os

def train_func_distributed():
    per_worker_batch_size = 64
    # This environment variable will be set by Ray Train.
    tf_config = json.loads(os.environ['TF_CONFIG'])
    num_workers = len(tf_config['cluster']['worker'])

    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    global_batch_size = per_worker_batch_size * num_workers
    multi_worker_dataset = mnist_dataset(global_batch_size)

    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = build_and_compile_cnn_model()

    multi_worker_model.fit(multi_worker_dataset, epochs=3, steps_per_epoch=70)

# __tf_distributed_end__

if __name__ == "__main__":
    # __tf_single_run_begin__

    train_func()

    # __tf_single_run_end__

    # __tf_trainer_begin__

    from ray.train import Trainer

    trainer = Trainer(backend="tensorflow", num_workers=4)

    # For GPU Training, set `use_gpu` to True.
    # trainer = Trainer(backend="tensorflow", num_workers=4, use_gpu=True)

    trainer.start()
    results = trainer.run(train_func_distributed)
    trainer.shutdown()

    # __tf_trainer_end__
