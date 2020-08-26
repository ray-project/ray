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
    _sampler = None

    def set_sampler(self, sampler):
        if self._sampler:
            raise ValueError("You can only choose one sampler for parameter "
                             "domains. Existing sampler for parameter {}: "
                             "{}. Tried to add {}".format(
                                 self.__class__.__name__, self._sampler,
                                 sampler))
        self._sampler = sampler

    def sample(self, spec=None, size=1):
        sampler = self._sampler
        if not sampler:
            sampler = Uniform()
        return sampler.sample(self, spec=spec, size=size)


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
        self.min = min,
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
            return np.random.uniform(domain.min, domain.max, size=size)
        elif isinstance(domain, Integer):
            return np.random.randint(domain.min, domain.max, size=size)
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

            return self.base**(np.random.uniform(logmin, logmax, size=size))
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
            return dist.rvs(size)
        else:
            raise ValueError(
                "Normal sampler does not support parameters of type {}. "
                "Allowed types: {}".format(domain.__class__.__name__, [Float]))


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


class uniform(sample_from):
    """Wraps tune.sample_from around ``np.random.uniform``.

    ``tune.uniform(1, 10)`` is equivalent to
    ``tune.sample_from(lambda _: np.random.uniform(1, 10))``

    """

    def __init__(self, *args, **kwargs):
        super().__init__(lambda _: np.random.uniform(*args, **kwargs))


class loguniform(sample_from):
    """Sugar for sampling in different orders of magnitude.

    Args:
        min_bound (float): Lower boundary of the output interval (1e-4)
        max_bound (float): Upper boundary of the output interval (1e-2)
        base (float): Base of the log. Defaults to 10.
    """

    def __init__(self, min_bound, max_bound, base=10):
        logmin = np.log(min_bound) / np.log(base)
        logmax = np.log(max_bound) / np.log(base)

        def apply_log(_):
            return base**(np.random.uniform(logmin, logmax))

        super().__init__(apply_log)


class choice(sample_from):
    """Wraps tune.sample_from around ``random.choice``.

    ``tune.choice([1, 2])`` is equivalent to
    ``tune.sample_from(lambda _: random.choice([1, 2]))``

    """

    def __init__(self, *args, **kwargs):
        super().__init__(lambda _: random.choice(*args, **kwargs))


class randint(sample_from):
    """Wraps tune.sample_from around ``np.random.randint``.

    ``tune.randint(10)`` is equivalent to
    ``tune.sample_from(lambda _: np.random.randint(10))``

    """

    def __init__(self, *args, **kwargs):
        super().__init__(lambda _: np.random.randint(*args, **kwargs))


class randn(sample_from):
    """Wraps tune.sample_from around ``np.random.randn``.

    ``tune.randn(10)`` is equivalent to
    ``tune.sample_from(lambda _: np.random.randn(10))``

    """

    def __init__(self, *args, **kwargs):
        super().__init__(lambda _: np.random.randn(*args, **kwargs))
