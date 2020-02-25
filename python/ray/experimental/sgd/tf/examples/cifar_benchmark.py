"""
Train a simple deep CNN on the CIFAR10 small images dataset.
And compare the performance of training on a few workers vs locally.
"""
import argparse
import time

import os
from filelock import FileLock

import ray
from ray.experimental.sgd.tf.tf_trainer import TFTrainer


num_classes = 10

def fetch_keras_data():
    from tensorflow.karas.utils import to_categorical
    from tensorflow.keras.datasets import cifar10

    # The data, split between train and test sets:
    with FileLock(os.path.expanduser("~/.cifar.lock")):
        (x_train, y_train), (x_test, y_test) = cifar10.load_data()

    # Convert class vectors to binary class matrices.
    y_train = to_categorical(y_train, num_classes)
    y_test = to_categorical(y_test, num_classes)

    x_train = x_train.astype("float32")
    x_test = x_test.astype("float32")
    x_train /= 255
    x_test /= 255
    return (x_train, y_train), (x_test, y_test)


def create_model(config):
    import tensorflow as tf

    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import Dense, Dropout, Activation, Flatten
    from tensorflow.keras.layers import Conv2D, MaxPooling2D

    model = Sequential()
    model.add(
        Conv2D(32, (3, 3), padding="same", input_shape=config["input_shape"]))
    model.add(Activation("relu"))
    model.add(Conv2D(32, (3, 3)))
    model.add(Activation("relu"))
    model.add(MaxPooling2D(pool_size=(2, 2)))
    model.add(Dropout(0.25))

    model.add(Conv2D(64, (3, 3), padding="same"))
    model.add(Activation("relu"))
    model.add(Conv2D(64, (3, 3)))
    model.add(Activation("relu"))
    model.add(MaxPooling2D(pool_size=(2, 2)))
    model.add(Dropout(0.25))

    model.add(Flatten())
    model.add(Dense(64))
    model.add(Activation("relu"))
    model.add(Dropout(0.5))
    model.add(Dense(num_classes))
    model.add(Activation("softmax"))

    # initiate RMSprop optimizer
    opt = tf.keras.optimizers.RMSprop(lr=0.001, decay=1e-6)

    # Let's train the model using RMSprop
    model.compile(
        loss="categorical_crossentropy", optimizer=opt, metrics=["accuracy"])
    return model


def data_creator(config):
    from tensorflow.data import Dataset

    (x_train, y_train), (x_test, y_test) = fetch_keras_data()

    train_dataset = Dataset.from_tensor_slices((x_train, y_train))
    test_dataset  = Dataset.from_tensor_slices((x_test, y_test))


    batch_size = config["batch_size"]

    train_dataset = train_dataset.repeat().shuffle(
        len(x_train)).batch(batch_size)
    test_dataset = test_dataset.repeat().batch(batch_size)


    return train_dataset, test_dataset


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address",
        default="auto",
        type=str,
        help="The address to use for Ray.")
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Number of workers used for training.")
    parser.add_argument(
        "--batch-size",
        "-bs"
        type=int, default=128,
        help=(
            "Total CPU batch size.\n"+
            "worker-batch-size = batch-size/num-replicas"
            ))
    parser.add_argument(
        "--batch-size-gpu-scaler",
        "-bs_gpu",
        type=int, default=16,
        help=(
            "Multiplier to use when computing total GPU batch size.\n\n"+
            "GPU batch size needs to be very large to avoid slowdown "+
            "relative to local training due to communication overhead as "+
            "GPUs are very fast.\n\n"+
            "gpu-batch-size = batch-size * batch-size-gpu-scaler"
            ))
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Use GPU training.")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help=(
            "Finish quickly for testing."+
            "Not intended to produce meaningful results.")
        )

    args, _ = parser.parse_known_args()

    ray.init(address=args.address)
    data_size = 60000
    test_size = 10000

    batch_size = args.batch_size
    if args.use_gpu:
        batch_size *= args.batch_size_gpu_scaler

    num_train_steps = (10 if args.smoke_test else data_size // batch_size)
    num_eval_steps = (10 if args.smoke_test else test_size // batch_size)

    def init_hook():
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
        os.environ["TF_CPP_MIN_VLOG_LEVEL"] = "3"

        import tensorflow as tf

        gpus = tf.config.experimental.list_physical_devices("GPU")
        if len(gpus) == 0:
            if args.use_gpu:
                raise "No GPUs found!"

        import logging
        tf.get_logger().setLevel(logging.ERROR)

        if not args.use_gpu:
            tf.keras.backend.set_image_data_format("channels_last")
        else:
            tf.keras.backend.set_image_data_format("channels_first")

    init_hook()
    (x_train, y_train), (x_test, y_test) = fetch_keras_data()

    config = {
        "batch_size": batch_size,
        "fit_config": {
            "steps_per_epoch": num_train_steps,
        },
        "evaluate_config": {
            "steps": num_eval_steps,
        },
        "input_shape": x_train.shape[1:]
    }

    trainer = TFTrainer(
        model_creator=create_model,
        data_creator=data_creator,
        init_hook=init_hook,
        num_replicas=args.num_replicas,
        use_gpu=args.use_gpu,
        verbose=True,
        config=config)

    training_start = time.time()

    progress_report_interval = 100 if args.use_gpu else 10

    remote_train_stats = trainer.train(progress_report_interval)
    remote_training_time = time.time() - training_start

    remote_train_stats.update(trainer.validate(progress_report_interval))
    trainer.shutdown()

    import tensorflow as tf
    # make sure we only use CPU so it's fair to the workers
    if not args.use_gpu:
        tf.config.experimental.set_visible_devices([], "GPU")

    # if you want to verify that only the CPU is used,
    # enable this:
    # tf.debugging.set_log_device_placement(True)

    # patch batch_size so that the local model consumes as much data
    # as all the workers together
    config["batch_size"] *= args.num_replicas
    train_data, test_data = data_creator(config)
    model = create_model(config)

    training_start = time.time()

    # workers do the same # of updates
    # but see num_replicas as much data
    local_train_stats = model.fit(
        train_data,
        steps_per_epoch=config["fit_config"]["steps_per_epoch"],
        verbose=True)

    local_training_time = time.time() - training_start

    local_eval_stats = model.evaluate(
        test_data, steps=config["evaluate_config"]["steps"], verbose=True)

    print(f"Remote results: {remote_train_stats}:")
    print(f"Local results. Train: {local_train_stats.history}" +
          f" Eval: {local_eval_stats}")

    print(f"Training on {args.num_replicas} " +
          f"workers takes: {remote_training_time:.3f} seconds/epoch")
    print(
        f"Training locally takes: {local_training_time:.3f} seconds/epoch")
