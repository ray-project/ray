import gym
from gym.spaces import Discrete, MultiDiscrete
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorStructType, TensorType

tf1, tf, tfv = try_import_tf()


def convert_to_non_tf_type(stats):
    """Converts values in `stats` to non-Tensor numpy or python types.

    Args:
        stats (any): Any (possibly nested) struct, the values in which will be
            converted and returned as a new struct with all tf (eager) tensors
            being converted to numpy types.

    Returns:
        Any: A new struct with the same structure as `stats`, but with all
            values converted to non-tf Tensor types.
    """

    # The mapping function used to numpyize torch Tensors.
    def mapping(item):
        if isinstance(item, (tf.Tensor, tf.Variable)):
            return item.numpy()
        else:
            return item

    return tree.map_structure(mapping, stats)


def explained_variance(y, pred):
    _, y_var = tf.nn.moments(y, axes=[0])
    _, diff_var = tf.nn.moments(y - pred, axes=[0])
    return tf.maximum(-1.0, 1 - (diff_var / y_var))


def get_gpu_devices():
    """Returns a list of GPU device names, e.g. ["/gpu:0", "/gpu:1"].

    Supports both tf1.x and tf2.x.
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


def get_placeholder(*, space=None, value=None, name=None, time_axis=False):
    from ray.rllib.models.catalog import ModelCatalog

    if space is not None:
        if isinstance(space, (gym.spaces.Dict, gym.spaces.Tuple)):
            return ModelCatalog.get_action_placeholder(space, None)
        return tf1.placeholder(
            shape=(None, ) + ((None, ) if time_axis else ()) + space.shape,
            dtype=tf.float32 if space.dtype == np.float64 else space.dtype,
            name=name,
        )
    else:
        assert value is not None
        shape = value.shape[1:]
        return tf1.placeholder(
            shape=(None, ) + ((None, )
                              if time_axis else ()) + (shape if isinstance(
                                  shape, tuple) else tuple(shape.as_list())),
            dtype=tf.float32 if value.dtype == np.float64 else value.dtype,
            name=name,
        )


def get_tf_eager_cls_if_necessary(orig_cls, config):
    cls = orig_cls
    framework = config.get("framework", "tf")
    if framework in ["tf2", "tf", "tfe"]:
        if not tf1:
            raise ImportError("Could not import tensorflow!")
        if framework in ["tf2", "tfe"]:
            assert tf1.executing_eagerly()

            from ray.rllib.policy.tf_policy import TFPolicy

            # Create eager-class.
            if hasattr(orig_cls, "as_eager"):
                cls = orig_cls.as_eager()
                if config.get("eager_tracing"):
                    cls = cls.with_tracing()
            # Could be some other type of policy.
            elif not issubclass(orig_cls, TFPolicy):
                pass
            else:
                raise ValueError("This policy does not support eager "
                                 "execution: {}".format(orig_cls))
    return cls


def huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return tf.where(
        tf.abs(x) < delta,
        tf.math.square(x) * 0.5, delta * (tf.abs(x) - 0.5 * delta))


def zero_logps_from_actions(actions: TensorStructType) -> TensorType:
    """Helper function useful for returning dummy logp's (0) for some actions.

    Args:
        actions (TensorStructType): The input actions. This can be any struct
            of complex action components or a simple tensor of different
            dimensions, e.g. [B], [B, 2], or {"a": [B, 4, 5], "b": [B]}.

    Returns:
        TensorType: A 1D tensor of 0.0 (dummy logp's) matching the batch
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
    if len(logp_.shape) > 1:
        logp_ = logp_[:, 0]
    return logp_


def one_hot(x, space):
    if isinstance(space, Discrete):
        return tf.one_hot(x, space.n)
    elif isinstance(space, MultiDiscrete):
        return tf.concat(
            [tf.one_hot(x[:, i], n) for i, n in enumerate(space.nvec)],
            axis=-1)
    else:
        raise ValueError("Unsupported space for `one_hot`: {}".format(space))


def reduce_mean_ignore_inf(x, axis):
    """Same as tf.reduce_mean() but ignores -inf values."""
    mask = tf.not_equal(x, tf.float32.min)
    x_zeroed = tf.where(mask, x, tf.zeros_like(x))
    return (tf.reduce_sum(x_zeroed, axis) / tf.reduce_sum(
        tf.cast(mask, tf.float32), axis))


def minimize_and_clip(optimizer, objective, var_list, clip_val=10.0):
    """Minimized `objective` using `optimizer` w.r.t. variables in
    `var_list` while ensure the norm of the gradients for each
    variable is clipped to `clip_val`
    """
    # Accidentally passing values < 0.0 will break all gradients.
    assert clip_val is None or clip_val > 0.0, clip_val

    if tf.executing_eagerly():
        tape = optimizer.tape
        grads_and_vars = list(
            zip(list(tape.gradient(objective, var_list)), var_list))
    else:
        grads_and_vars = optimizer.compute_gradients(
            objective, var_list=var_list)

    return [(tf.clip_by_norm(g, clip_val) if clip_val is not None else g, v)
            for (g, v) in grads_and_vars if g is not None]


def make_tf_callable(session_or_none, dynamic_shape=False):
    """Returns a function that can be executed in either graph or eager mode.

    The function must take only positional args.

    If eager is enabled, this will act as just a function. Otherwise, it
    will build a function that executes a session run with placeholders
    internally.

    Args:
        session_or_none (tf.Session): tf.Session if in graph mode, else None.
        dynamic_shape (bool): True if the placeholders should have a dynamic
            batch dimension. Otherwise they will be fixed shape.

    Returns:
        a Python function that can be called in either mode.
    """

    if tf.executing_eagerly():
        assert session_or_none is None
    else:
        assert session_or_none is not None

    def make_wrapper(fn):
        # Static-graph mode: Create placeholders and make a session call each
        # time the wrapped function is called. Return this session call's
        # outputs.
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
                if symbolic_out[0] is None:
                    with session_or_none.graph.as_default():
                        for i, v in enumerate(args):
                            if dynamic_shape:
                                if len(v.shape) > 0:
                                    shape = (None, ) + v.shape[1:]
                                else:
                                    shape = ()
                            else:
                                shape = v.shape
                            args_placeholders.append(
                                tf1.placeholder(
                                    dtype=v.dtype,
                                    shape=shape,
                                    name="arg_{}".format(i)))
                        for k, v in kwargs.items():
                            if dynamic_shape:
                                if len(v.shape) > 0:
                                    shape = (None, ) + v.shape[1:]
                                else:
                                    shape = ()
                            else:
                                shape = v.shape
                            kwargs_placeholders[k] = \
                                tf1.placeholder(
                                    dtype=v.dtype,
                                    shape=shape,
                                    name="kwarg_{}".format(k))
                        symbolic_out[0] = fn(*args_placeholders,
                                             **kwargs_placeholders)
                feed_dict = dict(zip(args_placeholders, args))
                feed_dict.update(
                    {kwargs_placeholders[k]: kwargs[k]
                     for k in kwargs.keys()})
                ret = session_or_none.run(symbolic_out[0], feed_dict)
                return ret

            return call
        # Eager mode (call function as is).
        else:
            return fn

    return make_wrapper


def scope_vars(scope, trainable_only=False):
    """
    Get variables inside a scope
    The scope can be specified as a string

    Parameters
    ----------
    scope: str or VariableScope
      scope in which the variables reside.
    trainable_only: bool
      whether or not to return only the variables that were marked as
      trainable.

    Returns
    -------
    vars: [tf.Variable]
      list of variables in `scope`.
    """
    return tf1.get_collection(
        tf1.GraphKeys.TRAINABLE_VARIABLES
        if trainable_only else tf1.GraphKeys.VARIABLES,
        scope=scope if isinstance(scope, str) else scope.name)
