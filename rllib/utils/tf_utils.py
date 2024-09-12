import logging
from typing import Any, Callable, List, Optional, Type, TYPE_CHECKING, Union

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree
from gymnasium.spaces import Discrete, MultiDiscrete

from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import SMALL_NUMBER
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    NetworkType,
    PartialAlgorithmConfigDict,
    SpaceStruct,
    TensorStructType,
    TensorType,
)

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.learner.learner import ParamDict
    from ray.rllib.policy.eager_tf_policy import EagerTFPolicy
    from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
    from ray.rllib.policy.tf_policy import TFPolicy

logger = logging.getLogger(__name__)
tf1, tf, tfv = try_import_tf()


@PublicAPI
def clip_gradients(
    gradients_dict: "ParamDict",
    *,
    grad_clip: Optional[float] = None,
    grad_clip_by: str,
) -> Optional[float]:
    """Performs gradient clipping on a grad-dict based on a clip value and clip mode.

    Changes the provided gradient dict in place.

    Args:
        gradients_dict: The gradients dict, mapping str to gradient tensors.
        grad_clip: The value to clip with. The way gradients are clipped is defined
            by the `grad_clip_by` arg (see below).
        grad_clip_by: One of 'value', 'norm', or 'global_norm'.

    Returns:
        If `grad_clip_by`="global_norm" and `grad_clip` is not None, returns the global
        norm of all tensors, otherwise returns None.
    """
    # No clipping, return.
    if grad_clip is None:
        return

    # Clip by value (each gradient individually).
    if grad_clip_by == "value":
        for k, v in gradients_dict.copy().items():
            gradients_dict[k] = tf.clip_by_value(v, -grad_clip, grad_clip)

    # Clip by L2-norm (per gradient tensor).
    elif grad_clip_by == "norm":
        for k, v in gradients_dict.copy().items():
            gradients_dict[k] = tf.clip_by_norm(v, grad_clip)

    # Clip by global L2-norm (across all gradient tensors).
    else:
        assert grad_clip_by == "global_norm"

        clipped_grads, global_norm = tf.clip_by_global_norm(
            list(gradients_dict.values()), grad_clip
        )
        for k, v in zip(gradients_dict.copy().keys(), clipped_grads):
            gradients_dict[k] = v

        # Return the computed global norm scalar.
        return global_norm


@PublicAPI
def explained_variance(y: TensorType, pred: TensorType) -> TensorType:
    """Computes the explained variance for a pair of labels and predictions.

    The formula used is:
    max(-1.0, 1.0 - (std(y - pred)^2 / std(y)^2))

    Args:
        y: The labels.
        pred: The predictions.

    Returns:
        The explained variance given a pair of labels and predictions.
    """
    _, y_var = tf.nn.moments(y, axes=[0])
    _, diff_var = tf.nn.moments(y - pred, axes=[0])
    return tf.maximum(-1.0, 1 - (diff_var / (y_var + SMALL_NUMBER)))


@PublicAPI
def flatten_inputs_to_1d_tensor(
    inputs: TensorStructType,
    spaces_struct: Optional[SpaceStruct] = None,
    time_axis: bool = False,
) -> TensorType:
    """Flattens arbitrary input structs according to the given spaces struct.

    Returns a single 1D tensor resulting from the different input
    components' values.

    Thereby:
    - Boxes (any shape) get flattened to (B, [T]?, -1). Note that image boxes
    are not treated differently from other types of Boxes and get
    flattened as well.
    - Discrete (int) values are one-hot'd, e.g. a batch of [1, 0, 3] (B=3 with
    Discrete(4) space) results in [[0, 1, 0, 0], [1, 0, 0, 0], [0, 0, 0, 1]].
    - MultiDiscrete values are multi-one-hot'd, e.g. a batch of
    [[0, 2], [1, 4]] (B=2 with MultiDiscrete([2, 5]) space) results in
    [[1, 0,  0, 0, 1, 0, 0], [0, 1,  0, 0, 0, 0, 1]].

    Args:
        inputs: The inputs to be flattened.
        spaces_struct: The structure of the spaces that behind the input
        time_axis: Whether all inputs have a time-axis (after the batch axis).
            If True, will keep not only the batch axis (0th), but the time axis
            (1st) as-is and flatten everything from the 2nd axis up.

    Returns:
        A single 1D tensor resulting from concatenating all
        flattened/one-hot'd input components. Depending on the time_axis flag,
        the shape is (B, n) or (B, T, n).

    .. testcode::
        :skipif: True

        # B=2
        from ray.rllib.utils.tf_utils import flatten_inputs_to_1d_tensor
        from gymnasium.spaces import Discrete, Box
        out = flatten_inputs_to_1d_tensor(
            {"a": [1, 0], "b": [[[0.0], [0.1]], [1.0], [1.1]]},
            spaces_struct=dict(a=Discrete(2), b=Box(shape=(2, 1)))
        )
        print(out)

        # B=2; T=2
        out = flatten_inputs_to_1d_tensor(
            ([[1, 0], [0, 1]],
             [[[0.0, 0.1], [1.0, 1.1]], [[2.0, 2.1], [3.0, 3.1]]]),
            spaces_struct=tuple([Discrete(2), Box(shape=(2, ))]),
            time_axis=True
        )
        print(out)

    .. testoutput::

        [[0.0, 1.0,  0.0, 0.1], [1.0, 0.0,  1.0, 1.1]]  # B=2 n=4
        [[[0.0, 1.0, 0.0, 0.1], [1.0, 0.0, 1.0, 1.1]],
        [[1.0, 0.0, 2.0, 2.1], [0.0, 1.0, 3.0, 3.1]]]  # B=2 T=2 n=4
    """

    flat_inputs = tree.flatten(inputs)
    flat_spaces = (
        tree.flatten(spaces_struct)
        if spaces_struct is not None
        else [None] * len(flat_inputs)
    )

    B = None
    T = None
    out = []
    for input_, space in zip(flat_inputs, flat_spaces):
        input_ = tf.convert_to_tensor(input_)
        shape = tf.shape(input_)
        # Store batch and (if applicable) time dimension.
        if B is None:
            B = shape[0]
            if time_axis:
                T = shape[1]

        # One-hot encoding.
        if isinstance(space, Discrete):
            if time_axis:
                input_ = tf.reshape(input_, [B * T])
            out.append(tf.cast(one_hot(input_, space), tf.float32))
        elif isinstance(space, MultiDiscrete):
            if time_axis:
                input_ = tf.reshape(input_, [B * T, -1])
            out.append(tf.cast(one_hot(input_, space), tf.float32))
        # Flatten.
        else:
            if time_axis:
                input_ = tf.reshape(input_, [B * T, -1])
            else:
                input_ = tf.reshape(input_, [B, -1])
            out.append(tf.cast(input_, tf.float32))

    merged = tf.concat(out, axis=-1)
    # Restore the time-dimension, if applicable.
    if time_axis:
        merged = tf.reshape(merged, [B, T, -1])

    return merged


@PublicAPI
def get_gpu_devices() -> List[str]:
    """Returns a list of GPU device names, e.g. ["/gpu:0", "/gpu:1"].

    Supports both tf1.x and tf2.x.

    Returns:
        List of GPU device names (str).
    """
    if tfv == 1:
        from tensorflow.python.client import device_lib

        devices = device_lib.list_local_devices()
    else:
        try:
            devices = tf.config.list_physical_devices()
        except Exception:
            devices = tf.config.experimental.list_physical_devices()

    # Expect "GPU", but also stuff like: "XLA_GPU".
    return [d.name for d in devices if "GPU" in d.device_type]


@PublicAPI
def get_placeholder(
    *,
    space: Optional[gym.Space] = None,
    value: Optional[Any] = None,
    name: Optional[str] = None,
    time_axis: bool = False,
    flatten: bool = True,
) -> "tf1.placeholder":
    """Returns a tf1.placeholder object given optional hints, such as a space.

    Note that the returned placeholder will always have a leading batch
    dimension (None).

    Args:
        space: An optional gym.Space to hint the shape and dtype of the
            placeholder.
        value: An optional value to hint the shape and dtype of the
            placeholder.
        name: An optional name for the placeholder.
        time_axis: Whether the placeholder should also receive a time
            dimension (None).
        flatten: Whether to flatten the given space into a plain Box space
            and then create the placeholder from the resulting space.

    Returns:
        The tf1 placeholder.
    """
    from ray.rllib.models.catalog import ModelCatalog

    if space is not None:
        if isinstance(space, (gym.spaces.Dict, gym.spaces.Tuple)):
            if flatten:
                return ModelCatalog.get_action_placeholder(space, None)
            else:
                return tree.map_structure_with_path(
                    lambda path, component: get_placeholder(
                        space=component,
                        name=name + "." + ".".join([str(p) for p in path]),
                    ),
                    get_base_struct_from_space(space),
                )
        return tf1.placeholder(
            shape=(None,) + ((None,) if time_axis else ()) + space.shape,
            dtype=tf.float32 if space.dtype == np.float64 else space.dtype,
            name=name,
        )
    else:
        assert value is not None
        shape = value.shape[1:]
        return tf1.placeholder(
            shape=(None,)
            + ((None,) if time_axis else ())
            + (shape if isinstance(shape, tuple) else tuple(shape.as_list())),
            dtype=tf.float32 if value.dtype == np.float64 else value.dtype,
            name=name,
        )


@PublicAPI
def get_tf_eager_cls_if_necessary(
    orig_cls: Type["TFPolicy"],
    config: Union["AlgorithmConfig", PartialAlgorithmConfigDict],
) -> Type[Union["TFPolicy", "EagerTFPolicy", "EagerTFPolicyV2"]]:
    """Returns the corresponding tf-eager class for a given TFPolicy class.

    Args:
        orig_cls: The original TFPolicy class to get the corresponding tf-eager
            class for.
        config: The Algorithm config dict or AlgorithmConfig object.

    Returns:
        The tf eager policy class corresponding to the given TFPolicy class.
    """
    cls = orig_cls
    framework = config.get("framework", "tf")

    if framework in ["tf2", "tf"] and not tf1:
        raise ImportError("Could not import tensorflow!")

    if framework == "tf2":
        if not tf1.executing_eagerly():
            tf1.enable_eager_execution()
        assert tf1.executing_eagerly()

        from ray.rllib.policy.tf_policy import TFPolicy
        from ray.rllib.policy.eager_tf_policy import EagerTFPolicy
        from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2

        # Create eager-class (if not already one).
        if hasattr(orig_cls, "as_eager") and not issubclass(orig_cls, EagerTFPolicy):
            cls = orig_cls.as_eager()
        # Could be some other type of policy or already
        # eager-ized.
        elif not issubclass(orig_cls, TFPolicy):
            pass
        else:
            raise ValueError(
                "This policy does not support eager execution: {}".format(orig_cls)
            )

        # Now that we know, policy is an eager one, add tracing, if necessary.
        if config.get("eager_tracing") and issubclass(
            cls, (EagerTFPolicy, EagerTFPolicyV2)
        ):
            cls = cls.with_tracing()
    return cls


@PublicAPI
def huber_loss(x: TensorType, delta: float = 1.0) -> TensorType:
    """Computes the huber loss for a given term and delta parameter.

    Reference: https://en.wikipedia.org/wiki/Huber_loss
    Note that the factor of 0.5 is implicitly included in the calculation.

    Formula:
        L = 0.5 * x^2  for small abs x (delta threshold)
        L = delta * (abs(x) - 0.5*delta)  for larger abs x (delta threshold)

    Args:
        x: The input term, e.g. a TD error.
        delta: The delta parmameter in the above formula.

    Returns:
        The Huber loss resulting from `x` and `delta`.
    """
    return tf.where(
        tf.abs(x) < delta,  # for small x -> apply the Huber correction
        tf.math.square(x) * 0.5,
        delta * (tf.abs(x) - 0.5 * delta),
    )


@PublicAPI
def l2_loss(x: TensorType) -> TensorType:
    """Computes half the L2 norm over a tensor's values without the sqrt.

    output = 0.5 * sum(x ** 2)

    Args:
        x: The input tensor.

    Returns:
        0.5 times the L2 norm over the given tensor's values (w/o sqrt).
    """
    return 0.5 * tf.reduce_sum(tf.pow(x, 2.0))


@PublicAPI
def make_tf_callable(
    session_or_none: Optional["tf1.Session"], dynamic_shape: bool = False
) -> Callable:
    """Returns a function that can be executed in either graph or eager mode.

    The function must take only positional args.

    If eager is enabled, this will act as just a function. Otherwise, it
    will build a function that executes a session run with placeholders
    internally.

    Args:
        session_or_none: tf.Session if in graph mode, else None.
        dynamic_shape: True if the placeholders should have a dynamic
            batch dimension. Otherwise they will be fixed shape.

    Returns:
        A function that can be called in either eager or static-graph mode.
    """

    if tf.executing_eagerly():
        assert session_or_none is None
    else:
        assert session_or_none is not None

    def make_wrapper(fn):
        # Static-graph mode: Create placeholders and make a session call each
        # time the wrapped function is called. Returns the output of this
        # session call.
        if session_or_none is not None:
            args_placeholders = []
            kwargs_placeholders = {}

            symbolic_out = [None]

            def call(*args, **kwargs):
                args_flat = []
                for a in args:
                    if type(a) is list:
                        args_flat.extend(a)
                    else:
                        args_flat.append(a)
                args = args_flat

                # We have not built any placeholders yet: Do this once here,
                # then reuse the same placeholders each time we call this
                # function again.
                if symbolic_out[0] is None:
                    with session_or_none.graph.as_default():

                        def _create_placeholders(path, value):
                            if dynamic_shape:
                                if len(value.shape) > 0:
                                    shape = (None,) + value.shape[1:]
                                else:
                                    shape = ()
                            else:
                                shape = value.shape
                            return tf1.placeholder(
                                dtype=value.dtype,
                                shape=shape,
                                name=".".join([str(p) for p in path]),
                            )

                        placeholders = tree.map_structure_with_path(
                            _create_placeholders, args
                        )
                        for ph in tree.flatten(placeholders):
                            args_placeholders.append(ph)

                        placeholders = tree.map_structure_with_path(
                            _create_placeholders, kwargs
                        )
                        for k, ph in placeholders.items():
                            kwargs_placeholders[k] = ph

                        symbolic_out[0] = fn(*args_placeholders, **kwargs_placeholders)
                feed_dict = dict(zip(args_placeholders, tree.flatten(args)))
                tree.map_structure(
                    lambda ph, v: feed_dict.__setitem__(ph, v),
                    kwargs_placeholders,
                    kwargs,
                )
                ret = session_or_none.run(symbolic_out[0], feed_dict)
                return ret

            return call
        # Eager mode (call function as is).
        else:
            return fn

    return make_wrapper


# TODO (sven): Deprecate this function once we have moved completely to the Learner API.
#  Replaced with `clip_gradients()`.
@PublicAPI
def minimize_and_clip(
    optimizer: LocalOptimizer,
    objective: TensorType,
    var_list: List["tf.Variable"],
    clip_val: float = 10.0,
) -> ModelGradients:
    """Computes, then clips gradients using objective, optimizer and var list.

    Ensures the norm of the gradients for each variable is clipped to
    `clip_val`.

    Args:
        optimizer: Either a shim optimizer (tf eager) containing a
            tf.GradientTape under `self.tape` or a tf1 local optimizer
            object.
        objective: The loss tensor to calculate gradients on.
        var_list: The list of tf.Variables to compute gradients over.
        clip_val: The global norm clip value. Will clip around -clip_val and
            +clip_val.

    Returns:
        The resulting model gradients (list or tuples of grads + vars)
        corresponding to the input `var_list`.
    """
    # Accidentally passing values < 0.0 will break all gradients.
    assert clip_val is None or clip_val > 0.0, clip_val

    if tf.executing_eagerly():
        tape = optimizer.tape
        grads_and_vars = list(zip(list(tape.gradient(objective, var_list)), var_list))
    else:
        grads_and_vars = optimizer.compute_gradients(objective, var_list=var_list)

    return [
        (tf.clip_by_norm(g, clip_val) if clip_val is not None else g, v)
        for (g, v) in grads_and_vars
        if g is not None
    ]


@PublicAPI
def one_hot(x: TensorType, space: gym.Space) -> TensorType:
    """Returns a one-hot tensor, given and int tensor and a space.

    Handles the MultiDiscrete case as well.

    Args:
        x: The input tensor.
        space: The space to use for generating the one-hot tensor.

    Returns:
        The resulting one-hot tensor.

    Raises:
        ValueError: If the given space is not a discrete one.

    .. testcode::
        :skipif: True

        import gymnasium as gym
        import tensorflow as tf
        from ray.rllib.utils.tf_utils import one_hot
        x = tf.Variable([0, 3], dtype=tf.int32)  # batch-dim=2
        # Discrete space with 4 (one-hot) slots per batch item.
        s = gym.spaces.Discrete(4)
        one_hot(x, s)

    .. testoutput::

        <tf.Tensor 'one_hot:0' shape=(2, 4) dtype=float32>

    .. testcode::
        :skipif: True

        x = tf.Variable([[0, 1, 2, 3]], dtype=tf.int32)  # batch-dim=1
        # MultiDiscrete space with 5 + 4 + 4 + 7 = 20 (one-hot) slots
        # per batch item.
        s = gym.spaces.MultiDiscrete([5, 4, 4, 7])
        one_hot(x, s)

    .. testoutput::

        <tf.Tensor 'concat:0' shape=(1, 20) dtype=float32>
    """
    if isinstance(space, Discrete):
        return tf.one_hot(x, space.n, dtype=tf.float32)
    elif isinstance(space, MultiDiscrete):
        if isinstance(space.nvec[0], np.ndarray):
            nvec = np.ravel(space.nvec)
            x = tf.reshape(x, (x.shape[0], -1))
        else:
            nvec = space.nvec
        return tf.concat(
            [tf.one_hot(x[:, i], n, dtype=tf.float32) for i, n in enumerate(nvec)],
            axis=-1,
        )
    else:
        raise ValueError("Unsupported space for `one_hot`: {}".format(space))


@PublicAPI
def reduce_mean_ignore_inf(x: TensorType, axis: Optional[int] = None) -> TensorType:
    """Same as tf.reduce_mean() but ignores -inf values.

    Args:
        x: The input tensor to reduce mean over.
        axis: The axis over which to reduce. None for all axes.

    Returns:
        The mean reduced inputs, ignoring inf values.
    """
    mask = tf.not_equal(x, tf.float32.min)
    x_zeroed = tf.where(mask, x, tf.zeros_like(x))
    return tf.math.reduce_sum(x_zeroed, axis) / tf.math.reduce_sum(
        tf.cast(mask, tf.float32), axis
    )


@PublicAPI
def scope_vars(
    scope: Union[str, "tf1.VariableScope"], trainable_only: bool = False
) -> List["tf.Variable"]:
    """Get variables inside a given scope.

    Args:
        scope: Scope in which the variables reside.
        trainable_only: Whether or not to return only the variables that were
            marked as trainable.

    Returns:
        The list of variables in the given `scope`.
    """
    return tf1.get_collection(
        tf1.GraphKeys.TRAINABLE_VARIABLES
        if trainable_only
        else tf1.GraphKeys.VARIABLES,
        scope=scope if isinstance(scope, str) else scope.name,
    )


@PublicAPI
def symlog(x: "tf.Tensor") -> "tf.Tensor":
    """The symlog function as described in [1]:

    [1] Mastering Diverse Domains through World Models - 2023
    D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
    https://arxiv.org/pdf/2301.04104v1.pdf
    """
    return tf.math.sign(x) * tf.math.log(tf.math.abs(x) + 1)


@PublicAPI
def inverse_symlog(y: "tf.Tensor") -> "tf.Tensor":
    """Inverse of the `symlog` function as desribed in [1]:

    [1] Mastering Diverse Domains through World Models - 2023
    D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
    https://arxiv.org/pdf/2301.04104v1.pdf
    """
    # To get to symlog inverse, we solve the symlog equation for x:
    #     y = sign(x) * log(|x| + 1)
    # <=> y / sign(x) = log(|x| + 1)
    # <=> y =  log( x + 1) V x >= 0
    #    -y =  log(-x + 1) V x <  0
    # <=> exp(y)  =  x + 1  V x >= 0
    #     exp(-y) = -x + 1  V x <  0
    # <=> exp(y)  - 1 =  x   V x >= 0
    #     exp(-y) - 1 = -x   V x <  0
    # <=>  exp(y)  - 1 = x   V x >= 0 (if x >= 0, then y must also be >= 0)
    #     -exp(-y) - 1 = x   V x <  0 (if x < 0, then y must also be < 0)
    # <=> sign(y) * (exp(|y|) - 1) = x
    return tf.math.sign(y) * (tf.math.exp(tf.math.abs(y)) - 1)


@PublicAPI
def two_hot(
    value: "tf.Tensor",
    num_buckets: int = 255,
    lower_bound: float = -20.0,
    upper_bound: float = 20.0,
    dtype=None,
):
    """Returns a two-hot vector of dim=num_buckets with two entries that are non-zero.

    See [1] for more details:
    [1] Mastering Diverse Domains through World Models - 2023
    D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
    https://arxiv.org/pdf/2301.04104v1.pdf

    Entries in the vector represent equally sized buckets within some fixed range
    (`lower_bound` to `upper_bound`).
    Those entries not 0.0 at positions k and k+1 encode the actual `value` and sum
    up to 1.0. They are the weights multiplied by the buckets values at k and k+1 for
    retrieving `value`.

    Example:
        num_buckets=11
        lower_bound=-5
        upper_bound=5
        value=2.5
        -> [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 0.5, 0.0, 0.0]
        -> [-5   -4   -3   -2   -1   0    1    2    3    4    5] (0.5*2 + 0.5*3=2.5)

    Example:
        num_buckets=5
        lower_bound=-1
        upper_bound=1
        value=0.1
        -> [0.0, 0.0, 0.8, 0.2, 0.0]
        -> [-1  -0.5   0   0.5   1] (0.2*0.5 + 0.8*0=0.1)

    Args:
        value: The input tensor of shape (B,) to be two-hot encoded.
        num_buckets: The number of buckets to two-hot encode into.
        lower_bound: The lower bound value used for the encoding. If input values are
            lower than this boundary, they will be encoded as `lower_bound`.
        upper_bound: The upper bound value used for the encoding. If input values are
            higher than this boundary, they will be encoded as `upper_bound`.

    Returns:
        The two-hot encoded tensor of shape (B, num_buckets).
    """
    # First make sure, values are clipped.
    value = tf.clip_by_value(value, lower_bound, upper_bound)
    # Tensor of batch indices: [0, B=batch size).
    batch_indices = tf.cast(
        tf.range(0, tf.shape(value)[0]),
        dtype=dtype or tf.float32,
    )
    # Calculate the step deltas (how much space between each bucket's central value?).
    bucket_delta = (upper_bound - lower_bound) / (num_buckets - 1)
    # Compute the float indices (might be non-int numbers: sitting between two buckets).
    idx = (-lower_bound + value) / bucket_delta
    # k
    k = tf.math.floor(idx)
    # k+1
    kp1 = tf.math.ceil(idx)
    # In case k == kp1 (idx is exactly on the bucket boundary), move kp1 up by 1.0.
    # Otherwise, this would result in a NaN in the returned two-hot tensor.
    kp1 = tf.where(tf.equal(k, kp1), kp1 + 1.0, kp1)
    # Iff `kp1` is one beyond our last index (because incoming value is larger than
    # `upper_bound`), move it to one before k (kp1's weight is going to be 0.0 anyways,
    # so it doesn't matter where it points to; we are just avoiding an index error
    # with this).
    kp1 = tf.where(tf.equal(kp1, num_buckets), kp1 - 2.0, kp1)
    # The actual values found at k and k+1 inside the set of buckets.
    values_k = lower_bound + k * bucket_delta
    values_kp1 = lower_bound + kp1 * bucket_delta
    # Compute the two-hot weights (adding up to 1.0) to use at index k and k+1.
    weights_k = (value - values_kp1) / (values_k - values_kp1)
    weights_kp1 = 1.0 - weights_k
    # Compile a tensor of full paths (indices from batch index to feature index) to
    # use for the scatter_nd op.
    indices_k = tf.stack([batch_indices, k], -1)
    indices_kp1 = tf.stack([batch_indices, kp1], -1)
    indices = tf.concat([indices_k, indices_kp1], 0)
    # The actual values (weights adding up to 1.0) to place at the computed indices.
    updates = tf.concat([weights_k, weights_kp1], 0)
    # Call the actual scatter update op, returning a zero-filled tensor, only changed
    # at the given indices.
    return tf.scatter_nd(
        tf.cast(indices, tf.int32),
        updates,
        shape=(tf.shape(value)[0], num_buckets),
    )


@PublicAPI
def update_target_network(
    main_net: NetworkType,
    target_net: NetworkType,
    tau: float,
) -> None:
    """Updates a keras.Model target network using Polyak averaging.

    new_target_net_weight = (
        tau * main_net_weight + (1.0 - tau) * current_target_net_weight
    )

    Args:
        main_net: The keras.Model to update from.
        target_net: The target network to update.
        tau: The tau value to use in the Polyak averaging formula.
    """
    for old_var, current_var in zip(target_net.variables, main_net.variables):
        updated_var = tau * current_var + (1.0 - tau) * old_var
        old_var.assign(updated_var)


@PublicAPI
def zero_logps_from_actions(actions: TensorStructType) -> TensorType:
    """Helper function useful for returning dummy logp's (0) for some actions.

    Args:
        actions: The input actions. This can be any struct
            of complex action components or a simple tensor of different
            dimensions, e.g. [B], [B, 2], or {"a": [B, 4, 5], "b": [B]}.

    Returns:
        A 1D tensor of 0.0 (dummy logp's) matching the batch
        dim of `actions` (shape=[B]).
    """
    # Need to flatten `actions` in case we have a complex action space.
    # Take the 0th component to extract the batch dim.
    action_component = tree.flatten(actions)[0]
    logp_ = tf.zeros_like(action_component, dtype=tf.float32)
    # Logp's should be single values (but with the same batch dim as
    # `deterministic_actions` or `stochastic_actions`). In case
    # actions are just [B], zeros_like works just fine here, but if
    # actions are [B, ...], we have to reduce logp back to just [B].
    while len(logp_.shape) > 1:
        logp_ = logp_[:, 0]
    return logp_


@DeveloperAPI
def warn_if_infinite_kl_divergence(
    policy: Type["TFPolicy"], mean_kl: TensorType
) -> None:
    def print_warning():
        logger.warning(
            "KL divergence is non-finite, this will likely destabilize your model and"
            " the training process. Action(s) in a specific state have near-zero"
            " probability. This can happen naturally in deterministic environments"
            " where the optimal policy has zero mass for a specific action. To fix this"
            " issue, consider setting the coefficient for the KL loss term to zero or"
            " increasing policy entropy."
        )
        return tf.constant(0.0)

    if policy.loss_initialized():
        tf.cond(
            tf.math.is_inf(mean_kl),
            false_fn=lambda: tf.constant(0.0),
            true_fn=lambda: print_warning(),
        )
