import numpy as np
import tree  # pip install dm_tree
from typing import List, Optional

from ray.rllib.utils.deprecation import DEPRECATED_VALUE, deprecation_warning
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.typing import TensorType, TensorStructType, Union

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

SMALL_NUMBER = 1e-6
# Some large int number. May be increased here, if needed.
LARGE_INTEGER = 100000000
# Min and Max outputs (clipped) from an NN-output layer interpreted as the
# log(x) of some x (e.g. a stddev of a normal
# distribution).
MIN_LOG_NN_OUTPUT = -5
MAX_LOG_NN_OUTPUT = 2


def aligned_array(size: int, dtype, align: int = 64) -> np.ndarray:
    """Returns an array of a given size that is 64-byte aligned.

    The returned array can be efficiently copied into GPU memory by TensorFlow.

    Args:
        size: The size (total number of items) of the array. For example,
            array([[0.0, 1.0], [2.0, 3.0]]) would have size=4.
        dtype: The numpy dtype of the array.
        align: The alignment to use.

    Returns:
        A np.ndarray with the given specifications.
    """
    n = size * dtype.itemsize
    empty = np.empty(n + (align - 1), dtype=np.uint8)
    data_align = empty.ctypes.data % align
    offset = 0 if data_align == 0 else (align - data_align)
    if n == 0:
        # stop np from optimising out empty slice reference
        output = empty[offset:offset + 1][0:0].view(dtype)
    else:
        output = empty[offset:offset + n].view(dtype)

    assert len(output) == size, len(output)
    assert output.ctypes.data % align == 0, output.ctypes.data
    return output


def concat_aligned(items: List[np.ndarray],
                   time_major: Optional[bool] = None) -> np.ndarray:
    """Concatenate arrays, ensuring the output is 64-byte aligned.

    We only align float arrays; other arrays are concatenated as normal.

    This should be used instead of np.concatenate() to improve performance
    when the output array is likely to be fed into TensorFlow.

    Args:
        items: The list of items to concatenate and align.
        time_major: Whether the data in items is time-major, in which
            case, we will concatenate along axis=1.

    Returns:
        The concat'd and aligned array.
    """

    if len(items) == 0:
        return []
    elif len(items) == 1:
        # we assume the input is aligned. In any case, it doesn't help
        # performance to force align it since that incurs a needless copy.
        return items[0]
    elif (isinstance(items[0], np.ndarray)
          and items[0].dtype in [np.float32, np.float64, np.uint8]):
        dtype = items[0].dtype
        flat = aligned_array(sum(s.size for s in items), dtype)
        if time_major is not None:
            if time_major is True:
                batch_dim = sum(s.shape[1] for s in items)
                new_shape = (
                    items[0].shape[0],
                    batch_dim,
                ) + items[0].shape[2:]
            else:
                batch_dim = sum(s.shape[0] for s in items)
                new_shape = (
                    batch_dim,
                    items[0].shape[1],
                ) + items[0].shape[2:]
        else:
            batch_dim = sum(s.shape[0] for s in items)
            new_shape = (batch_dim, ) + items[0].shape[1:]
        output = flat.reshape(new_shape)
        assert output.ctypes.data % 64 == 0, output.ctypes.data
        np.concatenate(items, out=output, axis=1 if time_major else 0)
        return output
    else:
        return np.concatenate(items, axis=1 if time_major else 0)


def convert_to_numpy(x: TensorStructType,
                     reduce_type: bool = True,
                     reduce_floats=DEPRECATED_VALUE):
    """Converts values in `stats` to non-Tensor numpy or python types.

    Args:
        x: Any (possibly nested) struct, the values in which will be
            converted and returned as a new struct with all torch/tf tensors
            being converted to numpy types.
        reduce_type: Whether to automatically reduce all float64 and int64 data
            into float32 and int32 data, respectively.

    Returns:
        A new struct with the same structure as `x`, but with all
        values converted to numpy arrays (on CPU).
    """

    if reduce_floats != DEPRECATED_VALUE:
        deprecation_warning(
            old="reduce_floats", new="reduce_types", error=False)
        reduce_type = reduce_floats

    # The mapping function used to numpyize torch/tf Tensors (and move them
    # to the CPU beforehand).
    def mapping(item):
        if torch and isinstance(item, torch.Tensor):
            ret = item.cpu().item() if len(item.size()) == 0 else \
                item.detach().cpu().numpy()
        elif tf and isinstance(item, (tf.Tensor, tf.Variable)) and \
                hasattr(item, "numpy"):
            assert tf.executing_eagerly()
            ret = item.numpy()
        else:
            ret = item
        if reduce_type and isinstance(ret, np.ndarray):
            if np.issubdtype(ret.dtype, np.floating):
                ret = ret.astype(np.float32)
            elif np.issubdtype(ret.dtype, int):
                ret = ret.astype(np.int32)
            return ret
        return ret

    return tree.map_structure(mapping, x)


def fc(x: np.ndarray,
       weights: np.ndarray,
       biases: Optional[np.ndarray] = None,
       framework: Optional[str] = None) -> np.ndarray:
    """Calculates FC (dense) layer outputs given weights/biases and input.

    Args:
        x: The input to the dense layer.
        weights: The weights matrix.
        biases: The biases vector. All 0s if None.
        framework: An optional framework hint (to figure out,
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


def huber_loss(x: np.ndarray, delta: float = 1.0) -> np.ndarray:
    """Reference: https://en.wikipedia.org/wiki/Huber_loss."""
    return np.where(
        np.abs(x) < delta,
        np.power(x, 2.0) * 0.5, delta * (np.abs(x) - 0.5 * delta))


def l2_loss(x: np.ndarray) -> np.ndarray:
    """Computes half the L2 norm of a tensor (w/o the sqrt): sum(x**2) / 2.

    Args:
        x: The input tensor.

    Returns:
        The l2-loss output according to the above formula given `x`.
    """
    return np.sum(np.square(x)) / 2.0


def lstm(x,
         weights: np.ndarray,
         biases: Optional[np.ndarray] = None,
         initial_internal_states: Optional[np.ndarray] = None,
         time_major: bool = False,
         forget_bias: float = 1.0):
    """Calculates LSTM layer output given weights/biases, states, and input.

    Args:
        x: The inputs to the LSTM layer including time-rank
            (0th if time-major, else 1st) and the batch-rank
            (1st if time-major, else 0th).
        weights: The weights matrix.
        biases: The biases vector. All 0s if None.
        initial_internal_states: The initial internal
            states to pass into the layer. All 0s if None.
        time_major: Whether to use time-major or not. Default: False.
        forget_bias: Gets added to first sigmoid (forget gate) output.
            Default: 1.0.

    Returns:
        Tuple consisting of 1) The LSTM layer's output and
        2) Tuple: Last (c-state, h-state).
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


def one_hot(x: Union[TensorType, int],
            depth: int = 0,
            on_value: int = 1.0,
            off_value: float = 0.0) -> np.ndarray:
    """One-hot utility function for numpy.

    Thanks to qianyizhang:
    https://gist.github.com/qianyizhang/07ee1c15cad08afb03f5de69349efc30.

    Args:
        x: The input to be one-hot encoded.
        depth: The max. number to be one-hot encoded (size of last rank).
        on_value: The value to use for on. Default: 1.0.
        off_value: The value to use for off. Default: 0.0.

    Returns:
        The one-hot encoded equivalent of the input array.
    """

    # Handle simple ints properly.
    if isinstance(x, int):
        x = np.array(x, dtype=np.int32)
    # Handle torch arrays properly.
    elif torch and isinstance(x, torch.Tensor):
        x = x.numpy()

    # Handle bool arrays correctly.
    if x.dtype == np.bool_:
        x = x.astype(np.int)
        depth = 2

    # If depth is not given, try to infer it from the values in the array.
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


def relu(x: np.ndarray, alpha: float = 0.0) -> np.ndarray:
    """Implementation of the leaky ReLU function.

    y = x * alpha if x < 0 else x

    Args:
        x: The input values.
        alpha: A scaling ("leak") factor to use for negative x.

    Returns:
        The leaky ReLU output for x.
    """
    return np.maximum(x, x * alpha, x)


def sigmoid(x: np.ndarray, derivative: bool = False) -> np.ndarray:
    """
    Returns the sigmoid function applied to x.
    Alternatively, can return the derivative or the sigmoid function.

    Args:
        x: The input to the sigmoid function.
        derivative: Whether to return the derivative or not.
            Default: False.

    Returns:
        The sigmoid function (or its derivative) applied to x.
    """
    if derivative:
        return x * (1 - x)
    else:
        return 1 / (1 + np.exp(-x))


def softmax(x: np.ndarray, axis: int = -1,
            epsilon: Optional[float] = None) -> np.ndarray:
    """Returns the softmax values for x.

    The exact formula used is:
    S(xi) = e^xi / SUMj(e^xj), where j goes over all elements in x.

    Args:
        x: The input to the softmax function.
        axis: The axis along which to softmax.
        epsilon: Optional epsilon as a minimum value. If None, use
            `SMALL_NUMBER`.

    Returns:
        The softmax over x.
    """
    epsilon = epsilon or SMALL_NUMBER
    # x_exp = np.maximum(np.exp(x), SMALL_NUMBER)
    x_exp = np.exp(x)
    # return x_exp /
    #   np.maximum(np.sum(x_exp, axis, keepdims=True), SMALL_NUMBER)
    return np.maximum(x_exp / np.sum(x_exp, axis, keepdims=True), epsilon)
