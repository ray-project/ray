import numbers
import numpy as np


def _is_arraylike(x):
    """Returns whether the input is array-like"""
    return hasattr(x, "__len__") or hasattr(x, "shape") or hasattr(
        x, "__array__")


def _num_samples(x):
    """Return number of samples in array-like x."""
    if hasattr(x, "fit") and callable(x.fit):
        # Don't get num_samples from an ensembles length!
        raise TypeError("Expected sequence or array-like, got "
                        "estimator %s" % x)
    if not hasattr(x, "__len__") and not hasattr(x, "shape"):
        if hasattr(x, "__array__"):
            x = np.asarray(x)
        else:
            raise TypeError(
                "Expected sequence or array-like, got %s" % type(x))
    if hasattr(x, "shape"):
        if len(x.shape) == 0:
            raise TypeError("Singleton array %r cannot be considered"
                            " a valid collection." % x)
        # Check that shape is returning an integer or default to len
        # Dask dataframes may not return numeric shape[0] value
        if isinstance(x.shape[0], numbers.Integral):
            return x.shape[0]
        else:
            return len(x)
    else:
        return len(x)
