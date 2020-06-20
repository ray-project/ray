import tensorflow as tf
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder


def get_iris_data(test_size=0.2):
    iris_data = load_iris()
    x = iris_data.data
    y = iris_data.target.reshape(-1, 1)
    encoder = OneHotEncoder(sparse=False)
    y = encoder.fit_transform(y)
    train_x, test_x, train_y, test_y = train_test_split(x, y)
    return train_x, train_y, test_x, test_y


def set_keras_threads(threads):
    # We set threads here to avoid contention, as Keras
    # is heavily parallelized across multiple cores.
    tf.config.threading.set_inter_op_parallelism_threads(threads)
    tf.config.threading.set_intra_op_parallelism_threads(threads)


def TuneKerasCallback(*args, **kwargs):
    raise DeprecationWarning("TuneKerasCallback is now "
                             "tune.integration.keras.TuneReporterCallback.")
