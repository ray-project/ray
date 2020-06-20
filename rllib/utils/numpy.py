import numpy as np

from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf = try_import_tf()
torch, _ = try_import_torch()

SMALL_NUMBER = 1e-6
# Some large int number. May be increased here, if needed.
LARGE_INTEGER = 100000000
# Min and Max outputs (clipped) from an NN-output layer interpreted as the
# log(x) of some x (e.g. a stddev of a normal
# distribution).
MIN_LOG_NN_OUTPUT = -20
MAX_LOG_NN_OUTPUT = 2


def huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return np.where(
        np.abs(x) < delta,
        np.power(x, 2.0) * 0.5, delta * (np.abs(x) - 0.5 * delta))


def l2_loss(x):
    """Computes half the L2 norm of a tensor (w/o the sqrt): sum(x**2) / 2

    Args:
        x (np.ndarray): The input tensor.

    Returns:
        The l2-loss output according to the above formula given `x`.
    """
    return np.sum(np.square(x)) / 2.0


def sigmoid(x, derivative=False):
    """
    Returns the sigmoid function applied to x.
    Alternatively, can return the derivative or the sigmoid function.

    Args:
        x (np.ndarray): The input to the sigmoid function.
        derivative (bool): Whether to return the derivative or not.
            Default: False.

    Returns:
        np.ndarray: The sigmoid function (or its derivative) applied to x.
    """
    if derivative:
        return x * (1 - x)
    else:
        return 1 / (1 + np.exp(-x))


def softmax(x, axis=-1):
    """
    Returns the softmax values for x as:
    S(xi) = e^xi / SUMj(e^xj), where j goes over all elements in x.

    Args:
        x (np.ndarray): The input to the softmax function.
        axis (int): The axis along which to softmax.

    Returns:
        np.ndarray: The softmax over x.
    """
    # x_exp = np.maximum(np.exp(x), SMALL_NUMBER)
    x_exp = np.exp(x)
    # return x_exp /
    #   np.maximum(np.sum(x_exp, axis, keepdims=True), SMALL_NUMBER)
    return np.maximum(x_exp / np.sum(x_exp, axis, keepdims=True), SMALL_NUMBER)


def relu(x, alpha=0.0):
    """
    Implementation of the leaky ReLU function:
    y = x * alpha if x < 0 else x

    Args:
        x (np.ndarray): The input values.
        alpha (float): A scaling ("leak") factor to use for negative x.

    Returns:
        np.ndarray: The leaky ReLU output for x.
    """
    return np.maximum(x, x * alpha, x)


def one_hot(x, depth=0, on_value=1, off_value=0):
    """
    One-hot utility function for numpy.
    Thanks to qianyizhang:
    https://gist.github.com/qianyizhang/07ee1c15cad08afb03f5de69349efc30.

    Args:
        x (np.ndarray): The input to be one-hot encoded.
        depth (int): The max. number to be one-hot encoded (size of last rank).
        on_value (float): The value to use for on. Default: 1.0.
        off_value (float): The value to use for off. Default: 0.0.

    Returns:
        np.ndarray: The one-hot encoded equivalent of the input array.
    """
    # Handle torch arrays properly.
    if torch and isinstance(x, torch.Tensor):
        x = x.numpy()

    # Handle bool arrays correctly.
    if x.dtype == np.bool_:
        x = x.astype(np.int)
        depth = 2

    if depth == 0:
        depth = np.max(x) + 1
    assert np.max(x) < depth, \
        "ERROR: The max. index of `x` ({}) is larger than depth ({})!".\
        format(np.max(x), depth)
    shape = x.shape

    # Python 2.7 compatibility, (*shape, depth) is not allowed.
    shape_list = list(shape[:])
    shape_list.append(depth)
    out = np.ones(shape_list) * off_value
    indices = []
    for i in range(x.ndim):
        tiles = [1] * x.ndim
        s = [1] * x.ndim
        s[i] = -1
        r = np.arange(shape[i]).reshape(s)
        if i > 0:
            tiles[i - 1] = shape[i - 1]
            r = np.tile(r, tiles)
        indices.append(r)
    indices.append(x)
    out[tuple(indices)] = on_value
    return out


def fc(x, weights, biases=None, framework=None):
    """
    Calculates the outputs of a fully-connected (dense) layer given
    weights/biases and an input.

    Args:
        x (np.ndarray): The input to the dense layer.
        weights (np.ndarray): The weights matrix.
        biases (Optional[np.ndarray]): The biases vector. All 0s if None.
        framework (Optional[str]): An optional framework hint (to figure out,
            e.g. whether to transpose torch weight matrices).

    Returns:
        The dense layer's output.
    """

    def map_(data, transpose=False):
        if torch:
            if isinstance(data, torch.Tensor):
                data = data.cpu().detach().numpy()
        if tf and tf.executing_eagerly():
            if isinstance(data, tf.Variable):
                data = data.numpy()
        if transpose:
            data = np.transpose(data)
        return data

    x = map_(x)
    # Torch stores matrices in transpose (faster for backprop).
    transpose = (framework == "torch" and (x.shape[1] != weights.shape[0]
                                           and x.shape[1] == weights.shape[1]))
    weights = map_(weights, transpose=transpose)
    biases = map_(biases)

    return np.matmul(x, weights) + (0.0 if biases is None else biases)


def lstm(x,
         weights,
         biases=None,
         initial_internal_states=None,
         time_major=False,
         forget_bias=1.0):
    """
    Calculates the outputs of an LSTM layer given weights/biases,
    internal_states, and input.

    Args:
        x (np.ndarray): The inputs to the LSTM layer including time-rank
            (0th if time-major, else 1st) and the batch-rank
            (1st if time-major, else 0th).

        weights (np.ndarray): The weights matrix.
        biases (Optional[np.ndarray]): The biases vector. All 0s if None.

        initial_internal_states (Optional[np.ndarray]): The initial internal
            states to pass into the layer. All 0s if None.

        time_major (bool): Whether to use time-major or not. Default: False.

        forget_bias (float): Gets added to first sigmoid (forget gate) output.
            Default: 1.0.

    Returns:
        Tuple:
            - The LSTM layer's output.
            - Tuple: Last (c-state, h-state).
    """
    sequence_length = x.shape[0 if time_major else 1]
    batch_size = x.shape[1 if time_major else 0]
    units = weights.shape[1] // 4  # 4 internal layers (3x sigmoid, 1x tanh)

    if initial_internal_states is None:
        c_states = np.zeros(shape=(batch_size, units))
        h_states = np.zeros(shape=(batch_size, units))
    else:
        c_states = initial_internal_states[0]
        h_states = initial_internal_states[1]

    # Create a placeholder for all n-time step outputs.
    if time_major:
        unrolled_outputs = np.zeros(shape=(sequence_length, batch_size, units))
    else:
        unrolled_outputs = np.zeros(shape=(batch_size, sequence_length, units))

    # Push the batch 4 times through the LSTM cell and capture the outputs plus
    # the final h- and c-states.
    for t in range(sequence_length):
        input_matrix = x[t, :, :] if time_major else x[:, t, :]
        input_matrix = np.concatenate((input_matrix, h_states), axis=1)
        input_matmul_matrix = np.matmul(input_matrix, weights) + biases
        # Forget gate (3rd slot in tf output matrix). Add static forget bias.
        sigmoid_1 = sigmoid(input_matmul_matrix[:, units * 2:units * 3] +
                            forget_bias)
        c_states = np.multiply(c_states, sigmoid_1)
        # Add gate (1st and 2nd slots in tf output matrix).
        sigmoid_2 = sigmoid(input_matmul_matrix[:, 0:units])
        tanh_3 = np.tanh(input_matmul_matrix[:, units:units * 2])
        c_states = np.add(c_states, np.multiply(sigmoid_2, tanh_3))
        # Output gate (last slot in tf output matrix).
        sigmoid_4 = sigmoid(input_matmul_matrix[:, units * 3:units * 4])
        h_states = np.multiply(sigmoid_4, np.tanh(c_states))

        # Store this output time-slice.
        if time_major:
            unrolled_outputs[t, :, :] = h_states
        else:
            unrolled_outputs[:, t, :] = h_states

    return unrolled_outputs, (c_states, h_states)
