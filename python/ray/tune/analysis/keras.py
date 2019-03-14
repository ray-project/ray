from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import argparse
import numpy as np
import itertools

import keras
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D
from keras.preprocessing.image import ImageDataGenerator
from keras.datasets import mnist
from keras import backend as K

from post_experiment import get_sorted_trials


def parse():
    parser = argparse.ArgumentParser(description='Keras MNIST Example')
    parser.add_argument('--lr', type=float, default=0.1, help='learning rate')
    parser.add_argument(
        '--momentum', type=float, default=0.0, help='SGD momentum')
    parser.add_argument(
        '--kernel1', type=int, default=3, help='Size of first kernel')
    parser.add_argument(
        '--kernel2', type=int, default=3, help='Size of second kernel')
    parser.add_argument(
        '--poolsize', type=int, default=2, help='Size of Poolin')
    parser.add_argument(
        '--dropout1', type=float, default=0.25, help='Size of first kernel')
    parser.add_argument(
        '--hidden', type=int, default=4, help='Size of Hidden Layer')
    parser.add_argument(
        '--dropout2', type=float, default=0.5, help='Size of first kernel')
    return vars(parser.parse_known_args()[0])


def make_model(parameters):
    config = parse().copy()  # This is obtained via the global scope
    config.update(parameters)
    num_classes = 10

    model = Sequential()
    model.add(
        Conv2D(
            32,
            kernel_size=(config["kernel1"], config["kernel1"]),
            activation='relu',
            input_shape=(28, 28, 1)))
    model.add(
        Conv2D(64, (config["kernel2"], config["kernel2"]), activation='relu'))
    model.add(MaxPooling2D(pool_size=(config["poolsize"], config["poolsize"])))
    model.add(Dropout(config["dropout1"]))
    model.add(Flatten())
    model.add(Dense(config["hidden"], activation='relu'))
    model.add(Dropout(config["dropout2"]))
    model.add(Dense(num_classes, activation='softmax'))

    model.compile(
        loss=keras.losses.categorical_crossentropy,
        optimizer=keras.optimizers.SGD(
            lr=config["lr"], momentum=config["momentum"]),
        metrics=['accuracy'])
    return model


def get_best_model(model_creator, trial_list, metric):
    """Restore a model from the best trial."""
    sorted_trials = get_sorted_trials(trial_list, metric)
    for best_trial in sorted_trials:
        try:
            print("Creating model...")
            model = model_creator(best_trial.config)
            weights = os.path.join(best_trial.logdir,
                                   best_trial.last_result["checkpoint"])
            print("Loading from", weights)
            model.load_weights(weights)
            break
        except Exception as e:
            print(e)
            print("Loading failed. Trying next model")
    return model


def shuffled(x, y):
    idx = np.r_[:x.shape[0]]
    np.random.shuffle(idx)
    return x[idx], y[idx]


def load_data(generator=True, num_batches=600):
    num_classes = 10

    # input image dimensions
    img_rows, img_cols = 28, 28

    # the data, split between train and test sets
    (x_train, y_train), (x_test, y_test) = mnist.load_data()

    if K.image_data_format() == 'channels_first':
        x_train = x_train.reshape(x_train.shape[0], 1, img_rows, img_cols)
        x_test = x_test.reshape(x_test.shape[0], 1, img_rows, img_cols)
    else:
        x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
        x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)

    x_train = x_train.astype('float32')
    x_test = x_test.astype('float32')
    x_train /= 255
    x_test /= 255
    print('x_train shape:', x_train.shape)
    print(x_train.shape[0], 'train samples')
    print(x_test.shape[0], 'test samples')
    x_train, y_train = shuffled(x_train, y_train)
    x_test, y_test = shuffled(x_test, y_test)

    # convert class vectors to binary class matrices
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)
    if generator:
        datagen = ImageDataGenerator()
        return itertools.islice(datagen.flow(x_train, y_train), num_batches)
    return x_train, x_test, y_train, y_test


def evaluate(model, validation=True):
    train_data, val_data, train_labels, val_labels = load_data(generator=False)
    data = val_data if validation else train_data
    labels = val_labels if validation else train_labels

    res = model.evaluate(data, labels)
    print("Model evaluation results:", dict(zip(model.metrics_names, res)))
