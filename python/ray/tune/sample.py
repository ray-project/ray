import logging
import random
from copy import copy
from typing import Callable, Dict, Iterator, List, Optional, \
    Sequence, \
    Union

import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


class Domain:
    sampler = None

    def set_sampler(self, sampler):
        if self.sampler:
            raise ValueError("You can only choose one sampler for parameter "
                             "domains. Existing sampler for parameter {}: "
                             "{}. Tried to add {}".format(
                                 self.__class__.__name__, self.sampler,
                                 sampler))
        self.sampler = sampler

    def has_sampler(self, cls):
        sampler = self.sampler
        if not sampler:
            sampler = Uniform()
        return isinstance(sampler, cls)

    def sample(self, spec=None, size=1):
        sampler = self.sampler
        if not sampler:
            sampler = Uniform()
        return sampler.sample(self, spec=spec, size=size)

    def is_grid(self):
        return isinstance(self.sampler, Grid)

    def is_function(self):
        return False


class Float(Domain):
    def __init__(self, min=float("-inf"), max=float("inf")):
        self.min = min
        self.max = max

    def normal(self, mean=0., sd=1.):
        new = copy(self)
        new.set_sampler(Normal(mean, sd))
        return new

    def uniform(self):
        if not self.min > float("-inf"):
            raise ValueError(
                "Uniform requires a minimum bound. Make sure to set the "
                "`min` parameter of `Float()`.")
        if not self.max < float("inf"):
            raise ValueError(
                "Uniform requires a maximum bound. Make sure to set the "
                "`max` parameter of `Float()`.")
        new = copy(self)
        new.set_sampler(Uniform())
        return new

    def loguniform(self, base: int = 10):
        if not self.min > 0:
            raise ValueError(
                "LogUniform requires a minimum bound greater than 0. "
                "Make sure to set the `min` parameter of `Float()` correctly.")
        if not 0 < self.max < float("inf"):
            raise ValueError(
                "LogUniform requires a minimum bound greater than 0. "
                "Make sure to set the `max` parameter of `Float()` correctly.")
        new = copy(self)
        new.set_sampler(LogUniform(base))
        return new


class Integer(Domain):
    def __init__(self, min, max):
        self.min = min
        self.max = max

    def uniform(self):
        new = copy(self)
        new.set_sampler(Uniform())
        return new


class Categorical(Domain):
    def __init__(self, categories: Sequence):
        self.categories = list(categories)

    def uniform(self):
        new = copy(self)
        new.set_sampler(Uniform())
        return new

    def grid(self):
        new = copy(self)
        new.set_sampler(Grid())
        return new

    def __len__(self):
        return len(self.categories)

    def __getitem__(self, item):
        return self.categories[item]


class Iterative(Domain):
    def __init__(self, iterator: Iterator):
        self.iterator = iterator

    def uniform(self):
        new = copy(self)
        new.set_sampler(Uniform())
        return new


class Function(Domain):
    def __init__(self, func: Callable):
        self.func = func

    def uniform(self):
        new = copy(self)
        new.set_sampler(Uniform())
        return new

    def is_function(self):
        return True


class Sampler:
    pass


class Uniform(Sampler):
    def sample(self,
               domain: Domain,
               spec: Optional[Union[List[Dict], Dict]] = None,
               size: int = 1):
        if isinstance(spec, list) and spec:
            assert len(spec) == size, \
                "Number of passed specs must match sample size"
        elif isinstance(spec, dict) and spec:
            assert size == 1, \
                "Cannot sample the same parameter more than once for one spec"

        if isinstance(domain, Float):
            assert domain.min > float("-inf"), \
                "Uniform needs a minimum bound"
            assert 0 < domain.max < float("inf"), \
                "Uniform needs a maximum bound"
            items = np.random.uniform(domain.min, domain.max, size=size)
            if len(items) == 1:
                return items[0]
            return list(items)
        elif isinstance(domain, Integer):
            items = np.random.randint(domain.min, domain.max, size=size)
            if len(items) == 1:
                return items[0]
            return list(items)
        elif isinstance(domain, Categorical):
            choices = []
            for i in range(size):
                choices.append(random.choice(domain.categories))
            if len(choices) == 1:
                return choices[0]
            return choices
        elif isinstance(domain, Function):
            items = []
            for i in range(size):
                this_spec = spec[i] if isinstance(spec, list) else spec
                items.append(domain.func(this_spec))
            if len(items) == 1:
                return items[0]
            return items
        elif isinstance(domain, Iterative):
            items = []
            for i in range(size):
                items.append(next(domain.iterator))
            if len(items) == 1:
                return items[0]
            return items
        else:
            raise RuntimeError(
                "Uniform sampler does not support parameters of type {}. "
                "Allowed types: {}".format(
                    domain.__class__.__name__,
                    [Float, Integer, Categorical, Function, Iterative]))


class LogUniform(Sampler):
    def __init__(self, base: int = 10):
        self.base = base
        assert self.base > 0, "Base has to be strictly greater than 0"

    def sample(self,
               domain: Domain,
               spec: Optional[Union[List[Dict], Dict]] = None,
               size: int = 1):
        if isinstance(domain, Float):
            assert domain.min > 0, \
                "LogUniform needs a minimum bound greater than 0"
            assert 0 < domain.max < float("inf"), \
                "LogUniform needs a maximum bound greater than 0"
            logmin = np.log(domain.min) / np.log(self.base)
            logmax = np.log(domain.max) / np.log(self.base)

            items = self.base**(np.random.uniform(logmin, logmax, size=size))
            if len(items) == 1:
                return items[0]
            return list(items)
        else:
            raise RuntimeError(
                "LogUniform sampler does not support parameters of type {}. "
                "Allowed types: {}".format(domain.__class__.__name__, [Float]))


class Normal(Sampler):
    def __init__(self, mean: float = 0., sd: float = 0.):
        self.mean = mean
        self.sd = sd

        assert self.sd > 0, "SD has to be strictly greater than 0"

    def sample(self,
               domain: Domain,
               spec: Optional[Union[List[Dict], Dict]] = None,
               size: int = 1):
        if isinstance(domain, Float):
            # Use a truncated normal to avoid oversampling border values
            dist = stats.truncnorm(
                (domain.min - self.mean) / self.sd,
                (domain.max - self.mean) / self.sd,
                loc=self.mean,
                scale=self.sd)
            items = dist.rvs(size)
            if len(items) == 1:
                return items[0]
            return list(items)
        else:
            raise ValueError(
                "Normal sampler does not support parameters of type {}. "
                "Allowed types: {}".format(domain.__class__.__name__, [Float]))


class Grid(Sampler):
    def sample(self,
               domain: Domain,
               spec: Optional[Union[List[Dict], Dict]] = None,
               size: int = 1):
        return RuntimeError("Do not call `sample()` on grid.")


def sample_from(func):
    """Specify that tune should sample configuration values from this function.

    Arguments:
        func: An callable function to draw a sample from.
    """
    return Function(func)


def uniform(min, max):
    """Sample a float value uniformly between ``min`` and ``max``.

    Sampling from ``tune.uniform(1, 10)`` is equivalent to sampling from
    ``np.random.uniform(1, 10))``

    """
    return Float(min, max).uniform()


def loguniform(min, max, base=10):
    """Sugar for sampling in different orders of magnitude.

    Args:
        min (float): Lower boundary of the output interval (e.g. 1e-4)
        max (float): Upper boundary of the output interval (e.g. 1e-2)
        base (float): Base of the log. Defaults to 10.

    """
    return Float(min, max).loguniform(base)


def choice(categories):
    """Sample a categorical value.

    Sampling from ``tune.choice([1, 2])`` is equivalent to sampling from
    ``random.choice([1, 2])``

    """
    return Categorical(categories).uniform()


def randint(min, max):
    """Sample an integer value uniformly between ``min`` and ``max``.

    ``min`` is inclusive, ``max`` is exlcusive.

    Sampling from ``tune.randint(10)`` is equivalent to sampling from
    ``np.random.randint(10)``

    """
    return Integer(min, max).uniform()


def randn(mean: float = 0.,
          sd: float = 1.,
          min: float = float("-inf"),
          max: float = float("inf")):
    """Sample a float value normally with ``mean`` and ``sd``.

    Will truncate the normal distribution at ``min`` and ``max`` to avoid
    oversampling the border regions.

    Args:
        mean (float): Mean of the normal distribution. Defaults to 0.
        sd (float): SD of the normal distribution. Defaults to 1.
        min (float): Minimum bound. Defaults to -inf.
        max (float): Maximum bound. Defaults to inf.

    """
    return Float(min, max).normal(mean, sd)
