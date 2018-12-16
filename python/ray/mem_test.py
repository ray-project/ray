import ray


def test_start_stopw():
    import ray.rllib.models
    import scipy.signal

    ray.init()
    ray.shutdown()


def test_start_stopx():
    import distutils.version
    import tensorflow.contrib.rnn
    import tensorflow.contrib.slim
    import scipy.signal

    ray.init()
    ray.shutdown()


def test_start_stopy():
    import numpy
    import tensorflow.contrib.rnn
    import tensorflow.contrib.slim
    import scipy.signal

    ray.init()
    ray.shutdown()

def test_start_stop0():
    import numpy
    import tensorflow
    import tensorflow.contrib.rnn
    import tensorflow.contrib.slim
    import scipy.signal

    ray.init()
    ray.shutdown()

def test_start_stop1():
    import numpy
    import tensorflow.contrib.rnn
    import tensorflow.contrib.slim
    import scipy.signal

    ray.init()
    ray.shutdown()

def test_start_stopz():
    import distutils.version
    import numpy
    import tensorflow
    import tensorflow.contrib.rnn
    import tensorflow.contrib.slim
    import scipy.signal

    ray.init()
    ray.shutdown()


if __name__ == "__main__":
    import ray.rllib.models
    import scipy.signal
    ray.init()
    ray.shutdown()
