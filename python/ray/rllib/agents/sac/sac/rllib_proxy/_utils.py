import tensorflow as tf


def add_mixins(base, mixins):
    """Returns a new class with mixins applied in priority order."""

    mixins = list(mixins or [])

    while mixins:

        class new_base(mixins.pop(), base):
            pass

        base = new_base

    return base


def executing_eagerly():
    try:
        return tf.executing_eagerly()
    except AttributeError:
        return False


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

    if executing_eagerly():
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
                if symbolic_out[0] is None:
                    with session_or_none.graph.as_default():
                        for i, v in enumerate(args):
                            if dynamic_shape:
                                if len(v.shape) > 0:
                                    shape = (None,) + v.shape[1:]
                                else:
                                    shape = ()
                            else:
                                shape = v.shape
                            placeholders.append(
                                tf.placeholder(
                                    dtype=v.dtype, shape=shape, name="arg_{}".format(i)
                                )
                            )
                        symbolic_out[0] = fn(*placeholders)
                feed_dict = dict(zip(placeholders, args))
                return session_or_none.run(symbolic_out[0], feed_dict)

            return call
        else:
            return fn

    return make_wrapper


class UsageTrackingDict(dict):
    """Dict that tracks which keys have been accessed.

    It can also intercept gets and allow an arbitrary callback to be applied
    (i.e., to lazily convert numpy arrays to Tensors).

    We make the simplifying assumption only __getitem__ is used to access
    values.
    """

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.accessed_keys = set()
        self.intercepted_values = {}
        self.get_interceptor = None

    def set_get_interceptor(self, fn):
        self.get_interceptor = fn

    def __getitem__(self, key):
        self.accessed_keys.add(key)
        value = dict.__getitem__(self, key)
        if self.get_interceptor:
            if key not in self.intercepted_values:
                self.intercepted_values[key] = self.get_interceptor(value)
            value = self.intercepted_values[key]
        return value

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        if key in self.intercepted_values:
            self.intercepted_values[key] = value
