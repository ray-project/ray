from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return tf.where(
        tf.abs(x) < delta,
        tf.square(x) * 0.5, delta * (tf.abs(x) - 0.5 * delta))


def reduce_mean_ignore_inf(x, axis):
    """Same as tf.reduce_mean() but ignores -inf values."""
    mask = tf.not_equal(x, tf.float32.min)
    x_zeroed = tf.where(mask, x, tf.zeros_like(x))
    return (tf.reduce_sum(x_zeroed, axis) / tf.reduce_sum(
        tf.cast(mask, tf.float32), axis))


def minimize_and_clip(optimizer, objective, var_list, clip_val=10):
    """Minimized `objective` using `optimizer` w.r.t. variables in
    `var_list` while ensure the norm of the gradients for each
    variable is clipped to `clip_val`
    """
    gradients = optimizer.compute_gradients(objective, var_list=var_list)
    for i, (grad, var) in enumerate(gradients):
        if grad is not None:
            gradients[i] = (tf.clip_by_norm(grad, clip_val), var)
    return gradients


def make_tf_callable(session_or_none, dynamic_shape=False):
    """Returns a function that can be executed in either graph or eager mode.

    The function must take only positional args.

    If eager is enabled, this will act as just a function. Otherwise, it
    will build a function that executes a session run with placeholders
    internally.

    Arguments:
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
        if session_or_none:
            placeholders = []
            symbolic_out = [None]

            def call(*args):
                args_flat = []
                for a in args:
                    if type(a) is list:
                        args_flat.extend(a)
                    else:
                        args_flat.append(a)
                args = args_flat
                if not placeholders:
                    with session_or_none.graph.as_default():
                        for i, v in enumerate(args):
                            if dynamic_shape:
                                if len(v.shape) > 0:
                                    shape = (None, ) + v.shape[1:]
                                else:
                                    shape = ()
                            else:
                                shape = v.shape
                            placeholders.append(
                                tf.placeholder(
                                    dtype=v.dtype,
                                    shape=shape,
                                    name="arg_{}".format(i)))
                        symbolic_out[0] = fn(*placeholders)
                feed_dict = dict(zip(placeholders, args))
                return session_or_none.run(symbolic_out[0], feed_dict)

            return call
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
    return tf.get_collection(
        tf.GraphKeys.TRAINABLE_VARIABLES
        if trainable_only else tf.GraphKeys.VARIABLES,
        scope=scope if isinstance(scope, str) else scope.name)
