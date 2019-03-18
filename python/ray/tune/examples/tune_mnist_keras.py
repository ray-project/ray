from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import argparse
import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D
from keras import backend as K

import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler


class TuneCallback(keras.callbacks.Callback):
    def __init__(self, reporter, logs={}):
        self.reporter = reporter
        self.iteration = 0

    def on_train_end(self, epoch, logs={}):
        self.reporter(
            timesteps_total=self.iteration, done=1, mean_accuracy=logs["acc"])

    def on_batch_end(self, batch, logs={}):
        self.iteration += 1
        self.reporter(
            timesteps_total=self.iteration, mean_accuracy=logs["acc"])


def train_mnist(args, cfg, reporter):
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

    # the data, split between train and test sets
    (x_train, y_train), (x_test, y_test) = mnist.load_data()

    if K.image_data_format() == 'channels_first':
        x_train = x_train.reshape(x_train.shape[0], 1, img_rows, img_cols)
        x_test = x_test.reshape(x_test.shape[0], 1, img_rows, img_cols)
        input_shape = (1, img_rows, img_cols)
    else:
        x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
        x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)
        input_shape = (img_rows, img_cols, 1)

    x_train = x_train.astype('float32')
    x_test = x_test.astype('float32')
    x_train /= 255
    x_test /= 255
    print('x_train shape:', x_train.shape)
    print(x_train.shape[0], 'train samples')
    print(x_test.shape[0], 'test samples')

    # convert class vectors to binary class matrices
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    model = Sequential()
    model.add(
        Conv2D(
            32,
            kernel_size=(args.kernel1, args.kernel1),
            activation='relu',
            input_shape=input_shape))
    model.add(Conv2D(64, (args.kernel2, args.kernel2), activation='relu'))
    model.add(MaxPooling2D(pool_size=(args.poolsize, args.poolsize)))
    model.add(Dropout(args.dropout1))
    model.add(Flatten())
    model.add(Dense(args.hidden, activation='relu'))
    model.add(Dropout(args.dropout2))
    model.add(Dense(num_classes, activation='softmax'))

    model.compile(
        loss=keras.losses.categorical_crossentropy,
        optimizer=keras.optimizers.SGD(lr=args.lr, momentum=args.momentum),
        metrics=['accuracy'])

    model.fit(
        x_train,
        y_train,
        batch_size=batch_size,
        epochs=epochs,
        verbose=0,
        validation_data=(x_test, y_test),
        callbacks=[TuneCallback(reporter)])


def create_parser():
    parser = argparse.ArgumentParser(description='Keras MNIST Example')
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--use-gpu", action="store_true", help="Use GPU in training.")
    parser.add_argument(
        '--jobs',
        type=int,
        default=1,
        help='number of jobs to run concurrently (default: 1)')
    parser.add_argument(
        '--threads',
        type=int,
        default=2,
        help='threads used in operations (default: 2)')
    parser.add_argument(
        '--steps',
        type=float,
        default=0.01,
        metavar='LR',
        help='learning rate (default: 0.01)')
    parser.add_argument(
        '--lr',
        type=float,
        default=0.01,
        metavar='LR',
        help='learning rate (default: 0.01)')
    parser.add_argument(
        '--momentum',
        type=float,
        default=0.5,
        metavar='M',
        help='SGD momentum (default: 0.5)')
    parser.add_argument(
        '--kernel1',
        type=int,
        default=3,
        help='Size of first kernel (default: 3)')
    parser.add_argument(
        '--kernel2',
        type=int,
        default=3,
        help='Size of second kernel (default: 3)')
    parser.add_argument(
        '--poolsize', type=int, default=2, help='Size of Pooling (default: 2)')
    parser.add_argument(
        '--dropout1',
        type=float,
        default=0.25,
        help='Size of first kernel (default: 0.25)')
    parser.add_argument(
        '--hidden',
        type=int,
        default=128,
        help='Size of Hidden Layer (default: 128)')
    parser.add_argument(
        '--dropout2',
        type=float,
        default=0.5,
        help='Size of first kernel (default: 0.5)')
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    mnist.load_data()  # we do this because it's not threadsafe

    ray.init()
    sched = AsyncHyperBandScheduler(
        time_attr="timesteps_total",
        reward_attr="mean_accuracy",
        max_t=400,
        grace_period=20)

    tune.register_trainable(
        "TRAIN_FN",
        lambda config, reporter: train_mnist(args, config, reporter))
    tune.run(
        "TRAIN_FN",
        name="exp",
        verbose=0,
        scheduler=sched,
        **{
            "stop": {
                "mean_accuracy": 0.99,
                "timesteps_total": 10 if args.smoke_test else 300
            },
            "num_samples": 1 if args.smoke_test else 10,
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
