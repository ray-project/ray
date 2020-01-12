import numpy as np

from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


def check(x, y, decimals=5, atol=None, rtol=None, false=False):
    """
    Checks two structures (dict, tuple, list,
    np.array, float, int, etc..) for (almost) numeric identity.
    All numbers in the two structures have to match up to `decimal` digits
    after the floating point. Uses assertions.

    Args:
        x (any): The first value to be compared (to `y`).
        y (any): The second value to be compared (to `x`).
        decimals (int): The number of digits after the floating point up to
            which all numeric values have to match.
        atol (float): Absolute tolerance of the difference between x and y
            (overrides `decimals` if given).
        rtol (float): Relative tolerance of the difference between x and y
            (overrides `decimals` if given).
        false (bool): Whether to check that x and y are NOT the same.
    """
    # A dict type.
    if isinstance(x, dict):
        assert isinstance(y, dict), \
            "ERROR: If x is dict, y needs to be a dict as well!"
        y_keys = set(x.keys())
        for key, value in x.items():
            assert key in y, \
                "ERROR: y does not have x's key='{}'! y={}".format(key, y)
            check(value, y[key], decimals=decimals, atol=atol, rtol=rtol,
                  false=false)
            y_keys.remove(key)
        assert not y_keys, \
            "ERROR: y contains keys ({}) that are not in x! y={}".\
            format(list(y_keys), y)
    # A tuple type.
    elif isinstance(x, (tuple, list)):
        assert isinstance(y, (tuple, list)),\
            "ERROR: If x is tuple, y needs to be a tuple as well!"
        assert len(y) == len(x),\
            "ERROR: y does not have the same length as x ({} vs {})!".\
            format(len(y), len(x))
        for i, value in enumerate(x):
            check(value, y[i], decimals=decimals, atol=atol, rtol=rtol,
                  false=false)
    # Boolean comparison.
    elif isinstance(x, (np.bool_, bool)):
        if false is True:
            assert bool(x) is not bool(y), \
                "ERROR: x ({}) is y ({})!".format(x, y)
        else:
            assert bool(x) is bool(y), \
                "ERROR: x ({}) is not y ({})!".format(x, y)
    # Nones.
    elif x is None or y is None:
        if false is True:
            assert x != y, "ERROR: x ({}) is the same as y ({})!".format(x, y)
        else:
            assert x == y, \
                "ERROR: x ({}) is not the same as y ({})!".format(x, y)
    # String comparison.
    elif hasattr(x, "dtype") and x.dtype == np.object:
        try:
            np.testing.assert_array_equal(x, y)
            if false is True:
                assert False, \
                    "ERROR: x ({}) is the same as y ({})!".format(x, y)
        except AssertionError as e:
            if false is False:
                raise e
    # Everything else (assume numeric).
    else:
        # Numpyize tensors if necessary.
        if tf is not None and isinstance(x, tf.Tensor):
            x = x.numpy()
        if tf is not None and isinstance(y, tf.Tensor):
            y = y.numpy()

        # Using decimals.
        if atol is None and rtol is None:
            try:
                np.testing.assert_almost_equal(x, y, decimal=decimals)
                if false is True:
                    assert False, \
                        "ERROR: x ({}) is the same as y ({})!".format(x, y)
            except AssertionError as e:
                if false is False:
                    raise e

        # Using atol/rtol.
        else:
            # Provide defaults for either one of atol/rtol.
            if atol is None:
                atol = 0
            if rtol is None:
                rtol = 1e-7
            try:
                np.testing.assert_allclose(x, y, atol=atol, rtol=rtol)
                if false is True:
                    assert False, \
                        "ERROR: x ({}) is the same as y ({})!".format(x, y)
            except AssertionError as e:
                if false is False:
                    raise e
