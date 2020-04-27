import logging
import numpy as np

logger = logging.getLogger(__name__)


class sample_from:
    """Specify that tune should sample configuration values from this function.

    Arguments:
        func: An callable function to draw a sample from.
    """

    def __init__(self, func):
        self.func = func

    def __str__(self):
        return "tune.sample_from({})".format(str(self.func))

    def __repr__(self):
        return "tune.sample_from({})".format(repr(self.func))


def function(func):
    logger.warning(
        "DeprecationWarning: wrapping {} with tune.function() is no "
        "longer needed".format(func))
    return func


def uniform(*args, **kwargs):
    """Wraps tune.sample_from around ``np.random.uniform``.

    ``tune.uniform(1, 10)`` is equivalent to
    ``tune.sample_from(lambda _: np.random.uniform(1, 10))``

    """
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
    """Wraps tune.sample_from around ``np.random.choice``.

    ``tune.choice(10)`` is equivalent to
    ``tune.sample_from(lambda _: np.random.choice(10))``

    """
    return sample_from(lambda _: np.random.choice(*args, **kwargs))


def randint(*args, **kwargs):
    """Wraps tune.sample_from around ``np.random.randint``.

    ``tune.randint(10)`` is equivalent to
    ``tune.sample_from(lambda _: np.random.randint(10))``

    """
    return sample_from(lambda _: np.random.randint(*args, **kwargs))


def randn(*args, **kwargs):
    """Wraps tune.sample_from around ``np.random.randn``.

    ``tune.randn(10)`` is equivalent to
    ``tune.sample_from(lambda _: np.random.randn(10))``

    """
    return sample_from(lambda _: np.random.randn(*args, **kwargs))
