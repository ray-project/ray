from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D
from keras import backend as K

import ray
from ray import tune
from ray.tune.examples.tune_mnist_keras import TuneCallback, get_mnist_data, create_parser


def train(args, cfg, reporter):
    # We set threads here to avoid contention, as Keras
    # is heavily parallelized across multiple cores.
    K.set_session(
        K.tf.Session(
            config=K.tf.ConfigProto(
                intra_op_parallelism_threads=args.threads,
                inter_op_parallelism_threads=args.threads)))
    vars(args).update(cfg)
    batch_size = 128
    num_classes = 10
    epochs = 12

    # input image dimensions
    img_rows, img_cols = 28, 28

    x_train, y_train, x_test, y_test, input_shape = get_mnist_data(img_rows, img_cols)

    model = Sequential()
    model.add(
        Conv2D(
            32,
            kernel_size=(args.kernel1, args.kernel1),
            activation="relu",
            input_shape=input_shape))
    model.add(Conv2D(64, (args.kernel2, args.kernel2), activation="relu"))
    model.add(MaxPooling2D(pool_size=(args.poolsize, args.poolsize)))
    model.add(Dropout(args.dropout1))
    model.add(Flatten())
    model.add(Dense(args.hidden, activation="relu"))
    model.add(Dropout(args.dropout2))
    model.add(Dense(num_classes, activation="softmax"))

    model.compile(
        loss=keras.losses.categorical_crossentropy,
        optimizer=keras.optimizers.SGD(lr=args.lr, momentum=args.momentum),
        metrics=["accuracy"])

    model.fit(
        x_train,
        y_train,
        batch_size=batch_size,
        epochs=epochs,
        verbose=0,
        validation_data=(x_test, y_test),
        callbacks=[TuneCallback(reporter)])


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    mnist.load_data()  # we do this because it's not threadsafe

    ray.init()

    tune.register_trainable(
        "TRAIN_FN",
        lambda config, reporter: train(args, config, reporter))
    tune.run(
        "TRAIN_FN",
        name="test_simple",
        **{
            "stop": {
                "mean_accuracy": 0.99,
                "timesteps_total": 50
            },
            "num_samples": 5,
            "resources_per_trial": {
                "cpu": args.threads,
                "gpu": 0.5 if args.use_gpu else 0
            },
            "config": {
                "lr": tune.sample_from(
                    lambda spec: np.random.uniform(0.001, 0.1)),
                "momentum": tune.sample_from(
                    lambda spec: np.random.uniform(0.1, 0.9)),
                "hidden": tune.sample_from(
                    lambda spec: np.random.randint(32, 512)),
                "dropout1": tune.sample_from(
                    lambda spec: np.random.uniform(0.2, 0.8)),
            }
        })
