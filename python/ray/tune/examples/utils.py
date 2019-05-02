from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import keras
from keras.datasets import mnist
from keras import backend as K


class TuneKerasCallback(keras.callbacks.Callback):
    def __init__(self, reporter, logs={}):
        self.reporter = reporter
        self.iteration = 0
        super(TuneKerasCallback, self).__init__()

    def on_train_end(self, epoch, logs={}):
        self.reporter(
            timesteps_total=self.iteration, done=1, mean_accuracy=logs["acc"])

    def on_batch_end(self, batch, logs={}):
        self.iteration += 1
        self.reporter(
            timesteps_total=self.iteration, mean_accuracy=logs["acc"])


def get_mnist_data():
    img_rows, img_cols = 28, 28
    num_classes = 10

    # the data, split between train and test sets
    (x_train, y_train), (x_test, y_test) = mnist.load_data()

    if K.image_data_format() == "channels_first":
        x_train = x_train.reshape(x_train.shape[0], 1, img_rows, img_cols)
        x_test = x_test.reshape(x_test.shape[0], 1, img_rows, img_cols)
        input_shape = (1, img_rows, img_cols)
    else:
        x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
        x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)
        input_shape = (img_rows, img_cols, 1)

    x_train = x_train.astype("float32")
    x_test = x_test.astype("float32")
    x_train /= 255
    x_test /= 255

    # convert class vectors to binary class matrices
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    return x_train, y_train, x_test, y_test, input_shape


def set_keras_threads(threads):
    # We set threads here to avoid contention, as Keras
    # is heavily parallelized across multiple cores.
    K.set_session(
        K.tf.Session(
            config=K.tf.ConfigProto(
                intra_op_parallelism_threads=threads,
                inter_op_parallelism_threads=threads)))
