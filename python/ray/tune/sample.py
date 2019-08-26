from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np

logger = logging.getLogger(__name__)


class sample_from(object):
    """Specify that tune should sample configuration values from this function.

    The use of function arguments in tune configs must be disambiguated by
    either wrapped the function in tune.sample_from() or tune.function().

    Arguments:
        func: An callable function to draw a sample from.
    """

    def __init__(self, func):
        self.func = func

    def __str__(self):
        return "tune.sample_from({})".format(str(self.func))

    def __repr__(self):
        return "tune.sample_from({})".format(repr(self.func))


class function(object):
    """Wraps `func` to make sure it is not expanded during resolution.

    The use of function arguments in tune configs must be disambiguated by
    either wrapped the function in tune.sample_from() or tune.function().

    Arguments:
        func: A function literal.
    """

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __str__(self):
        return "tune.function({})".format(str(self.func))

    def __repr__(self):
        return "tune.function({})".format(repr(self.func))


def uniform(*args, **kwargs):
    """A wrapper around np.random.uniform."""
    return sample_from(lambda _: np.random.uniform(*args, **kwargs))


def loguniform(min_bound, max_bound, base=10):
    """Sugar for sampling in different orders of magnitude.

    Args:
        min_bound (float): Lower boundary of the output interval (1e-4)
        max_bound (float): Upper boundary of the output interval (1e-2)
        base (float): Base of the log. Defaults to 10.
        """
    logmin = np.log(min_bound) / np.log(base)
    logmax = np.log(max_bound) / np.log(base)

    def apply_log(_):
        return base**(np.random.uniform(logmin, logmax))

    return sample_from(apply_log)


def choice(*args, **kwargs):
    """A wrapper around np.random.choice."""
    return sample_from(lambda _: np.random.choice(*args, **kwargs))


def randint(*args, **kwargs):
    """A wrapper around np.random.randint."""
    return sample_from(lambda _: np.random.randint(*args, **kwargs))


def randn(*args, **kwargs):
    """A wrapper around np.random.randn."""
    return sample_from(lambda _: np.random.randn(*args, **kwargs))
