import logging
import random
from copy import copy
from numbers import Number
from typing import Any, Callable, Dict, Iterator, List, Optional, \
    Sequence, \
    Union

import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


class Domain:
    sampler = None
    default_sampler_cls = None

    def set_sampler(self, sampler, allow_override=False):
        if self.sampler and not allow_override:
            raise ValueError("You can only choose one sampler for parameter "
                             "domains. Existing sampler for parameter {}: "
                             "{}. Tried to add {}".format(
                                 self.__class__.__name__, self.sampler,
                                 sampler))
        self.sampler = sampler

    def get_sampler(self):
        sampler = self.sampler
        if not sampler:
            sampler = self.default_sampler_cls()
        return sampler

    def sample(self, spec=None, size=1):
        sampler = self.get_sampler()
        return sampler.sample(self, spec=spec, size=size)

    def is_grid(self):
        return isinstance(self.sampler, Grid)

    def is_function(self):
        return False


class Sampler:
    def sample(self,
               domain: Domain,
               spec: Optional[Union[List[Dict], Dict]] = None,
               size: int = 1):
        raise NotImplementedError


class BaseSampler(Sampler):
    pass


class Uniform(Sampler):
    pass


class LogUniform(Sampler):
    pass


class Normal(Sampler):
    pass


class Grid(Sampler):
    """Dummy sampler used for grid search"""

    def sample(self,
               domain: Domain,
               spec: Optional[Union[List[Dict], Dict]] = None,
               size: int = 1):
        return RuntimeError("Do not call `sample()` on grid.")


class Float(Domain):
    class _Uniform(Uniform):
        def sample(self,
                   domain: "Float",
                   spec: Optional[Union[List[Dict], Dict]] = None,
                   size: int = 1):
            assert domain.min > float("-inf"), \
                "Uniform needs a minimum bound"
            assert 0 < domain.max < float("inf"), \
                "Uniform needs a maximum bound"
            items = np.random.uniform(domain.min, domain.max, size=size)
            if len(items) == 1:
                return items[0]
            return list(items)

    class _LogUniform(LogUniform):
        def __init__(self, base: int = 10):
            self.base = base
            assert self.base > 0, "Base has to be strictly greater than 0"

        def sample(self,
                   domain: "Float",
                   spec: Optional[Union[List[Dict], Dict]] = None,
                   size: int = 1):
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

    class _Normal(Normal):
        def __init__(self, mean: float = 0., sd: float = 0.):
            self.mean = mean
            self.sd = sd

            assert self.sd > 0, "SD has to be strictly greater than 0"

        def sample(self,
                   domain: "Float",
                   spec: Optional[Union[List[Dict], Dict]] = None,
                   size: int = 1):
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

    default_sampler_cls = _Uniform

    def __init__(self, min=float("-inf"), max=float("inf")):
        self.min = min
        self.max = max

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
        new.set_sampler(self._Uniform())
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
        new.set_sampler(self._LogUniform(base))
        return new

    def normal(self, mean=0., sd=1.):
        new = copy(self)
        new.set_sampler(self._Normal(mean, sd))
        return new

    def quantized(self, q: Number):
        new = copy(self)
        new.set_sampler(Quantized(new.get_sampler(), q), allow_override=True)
        return new


class Integer(Domain):
    class _Uniform(Uniform):
        def sample(self,
                   domain: "Integer",
                   spec: Optional[Union[List[Dict], Dict]] = None,
                   size: int = 1):
            items = np.random.randint(domain.min, domain.max, size=size)
            if len(items) == 1:
                return items[0]
            return list(items)

    default_sampler_cls = _Uniform

    def __init__(self, min, max):
        self.min = min
        self.max = max

    def quantized(self, q: Number):
        new = copy(self)
        new.set_sampler(Quantized(new.get_sampler(), q), allow_override=True)
        return new

    def uniform(self):
        new = copy(self)
        new.set_sampler(self._Uniform())
        return new


class Categorical(Domain):
    class _Uniform(Uniform):
        def sample(self,
                   domain: "Categorical",
                   spec: Optional[Union[List[Dict], Dict]] = None,
                   size: int = 1):
            choices = []
            for i in range(size):
                choices.append(random.choice(domain.categories))
            if len(choices) == 1:
                return choices[0]
            return choices

    default_sampler_cls = _Uniform

    def __init__(self, categories: Sequence):
        self.categories = list(categories)

    def uniform(self):
        new = copy(self)
        new.set_sampler(self._Uniform())
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
    class _NextSampler(BaseSampler):
        def sample(self,
                   domain: "Iterative",
                   spec: Optional[Union[List[Dict], Dict]] = None,
                   size: int = 1):
            items = []
            for i in range(size):
                items.append(next(domain.iterator))
            if len(items) == 1:
                return items[0]
            return items

    default_sampler_cls = _NextSampler

    def __init__(self, iterator: Iterator):
        self.iterator = iterator


class Function(Domain):
    class _CallSampler(BaseSampler):
        def sample(self,
                   domain: "Function",
                   spec: Optional[Union[List[Dict], Dict]] = None,
                   size: int = 1):
            items = []
            for i in range(size):
                this_spec = spec[i] if isinstance(spec, list) else spec
                items.append(domain.func(this_spec))
            if len(items) == 1:
                return items[0]
            return items

    default_sampler_cls = _CallSampler

    def __init__(self, func: Callable):
        self.func = func

    def is_function(self):
        return True


class Quantized(Sampler):
    def __init__(self, sampler: Sampler, q: Number):
        self.sampler = sampler
        self.q = q

        assert self.sampler, "Quantized() expects a sampler instance"

    def get_sampler(self):
        return self.sampler

    def sample(self,
               domain: Domain,
               spec: Optional[Union[List[Dict], Dict]] = None,
               size: int = 1):
        values = self.sampler.sample(domain, spec, size)
        quantized = np.round(np.divide(values, self.q)) * self.q
        if len(quantized) == 1:
            return quantized[0]
        return list(quantized)


def sample_from(func: Callable[[Dict], Any]):
    """Specify that tune should sample configuration values from this function.

    Arguments:
        func: An callable function to draw a sample from.
    """
    return Function(func)


def uniform(min: float, max: float):
    """Sample a float value uniformly between ``min`` and ``max``.

    Sampling from ``tune.uniform(1, 10)`` is equivalent to sampling from
    ``np.random.uniform(1, 10))``

    """
    return Float(min, max).uniform()


def quniform(min: float, max: float, q: float):
    """Sample a quantized float value uniformly between ``min`` and ``max``.

    Sampling from ``tune.uniform(1, 10)`` is equivalent to sampling from
    ``np.random.uniform(1, 10))``

    The value will be quantized, i.e. rounded to an integer increment of ``q``.

    """
    return Float(min, max).uniform().quantized(q)


def loguniform(min: float, max: float, base: float = 10):
    """Sugar for sampling in different orders of magnitude.

    Args:
        min (float): Lower boundary of the output interval (e.g. 1e-4)
        max (float): Upper boundary of the output interval (e.g. 1e-2)
        base (int): Base of the log. Defaults to 10.

    """
    return Float(min, max).loguniform(base)


def qloguniform(min, max, q, base=10):
    """Sugar for sampling in different orders of magnitude.

    The value will be quantized, i.e. rounded to an integer increment of ``q``.

    Args:
        min (float): Lower boundary of the output interval (e.g. 1e-4)
        max (float): Upper boundary of the output interval (e.g. 1e-2)
        q (float): Quantization number. The result will be rounded to an
            integer increment of this value.
        base (int): Base of the log. Defaults to 10.

    """
    return Float(min, max).loguniform(base).quantized(q)


def choice(categories: List):
    """Sample a categorical value.

    Sampling from ``tune.choice([1, 2])`` is equivalent to sampling from
    ``random.choice([1, 2])``

    """
    return Categorical(categories).uniform()


def randint(min: int, max: int):
    """Sample an integer value uniformly between ``min`` and ``max``.

    ``min`` is inclusive, ``max`` is exclusive.

    Sampling from ``tune.randint(10)`` is equivalent to sampling from
    ``np.random.randint(10)``

    """
    return Integer(min, max).uniform()


def qrandint(min: int, max: int, q: int = 1):
    """Sample an integer value uniformly between ``min`` and ``max``.

    ``min`` is inclusive, ``max`` is exclusive.

    The value will be quantized, i.e. rounded to an integer increment of ``q``.

    Sampling from ``tune.randint(10)`` is equivalent to sampling from
    ``np.random.randint(10)``

    """
    return Integer(min, max).uniform().quantized(q)


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


def qrandn(q: float,
           mean: float = 0.,
           sd: float = 1.,
           min: float = float("-inf"),
           max: float = float("inf")):
    """Sample a float value normally with ``mean`` and ``sd``.

    The value will be quantized, i.e. rounded to an integer increment of ``q``.

    Will truncate the normal distribution at ``min`` and ``max`` to avoid
    oversampling the border regions.

    Args:
        q (float): Quantization number. The result will be rounded to an
            integer increment of this value.
        mean (float): Mean of the normal distribution. Defaults to 0.
        sd (float): SD of the normal distribution. Defaults to 1.
        min (float): Minimum bound. Defaults to -inf.
        max (float): Maximum bound. Defaults to inf.

    """
    return Float(min, max).normal(mean, sd).quantized(q)
