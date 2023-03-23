import logging
from copy import copy
from inspect import signature
from math import isclose
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import numpy as np

# Backwards compatibility
from ray.util.annotations import DeveloperAPI, PublicAPI

try:
    # Added in numpy>=1.17 but we require numpy>=1.16
    np_random_generator = np.random.Generator
    LEGACY_RNG = False
except AttributeError:

    class np_random_generator:
        pass

    LEGACY_RNG = True

logger = logging.getLogger(__name__)


class _BackwardsCompatibleNumpyRng:
    """Thin wrapper to ensure backwards compatibility between
    new and old numpy randomness generators.
    """

    _rng = None

    def __init__(
        self,
        generator_or_seed: Optional[
            Union["np_random_generator", np.random.RandomState, int]
        ] = None,
    ):
        if generator_or_seed is None or isinstance(
            generator_or_seed, (np.random.RandomState, np_random_generator)
        ):
            self._rng = generator_or_seed
        elif LEGACY_RNG:
            self._rng = np.random.RandomState(generator_or_seed)
        else:
            self._rng = np.random.default_rng(generator_or_seed)

    @property
    def legacy_rng(self) -> bool:
        return not isinstance(self._rng, np_random_generator)

    @property
    def rng(self):
        # don't set self._rng to np.random to avoid picking issues
        return self._rng if self._rng is not None else np.random

    def __getattr__(self, name: str) -> Any:
        # https://numpy.org/doc/stable/reference/random/new-or-different.html
        if self.legacy_rng:
            if name == "integers":
                name = "randint"
            elif name == "random":
                name = "rand"
        return getattr(self.rng, name)


RandomState = Union[
    None, _BackwardsCompatibleNumpyRng, np_random_generator, np.random.RandomState, int
]


@DeveloperAPI
class Domain:
    """Base class to specify a type and valid range to sample parameters from.

    This base class is implemented by parameter spaces, like float ranges
    (``Float``), integer ranges (``Integer``), or categorical variables
    (``Categorical``). The ``Domain`` object contains information about
    valid values (e.g. minimum and maximum values), and exposes methods that
    allow specification of specific samplers (e.g. ``uniform()`` or
    ``loguniform()``).

    """

    sampler = None
    default_sampler_cls = None

    def cast(self, value):
        """Cast value to domain type"""
        return value

    def set_sampler(self, sampler, allow_override=False):
        if self.sampler and not allow_override:
            raise ValueError(
                "You can only choose one sampler for parameter "
                "domains. Existing sampler for parameter {}: "
                "{}. Tried to add {}".format(
                    self.__class__.__name__, self.sampler, sampler
                )
            )
        self.sampler = sampler

    def get_sampler(self):
        sampler = self.sampler
        if not sampler:
            sampler = self.default_sampler_cls()
        return sampler

    def sample(
        self,
        config: Optional[Union[List[Dict], Dict]] = None,
        size: int = 1,
        random_state: "RandomState" = None,
    ):
        if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
            random_state = _BackwardsCompatibleNumpyRng(random_state)
        sampler = self.get_sampler()
        return sampler.sample(self, config=config, size=size, random_state=random_state)

    def is_grid(self):
        return isinstance(self.sampler, Grid)

    def is_function(self):
        return False

    def is_valid(self, value: Any):
        """Returns True if `value` is a valid value in this domain."""
        raise NotImplementedError

    @property
    def domain_str(self):
        return "(unknown)"


@DeveloperAPI
class Sampler:
    def sample(
        self,
        domain: Domain,
        config: Optional[Union[List[Dict], Dict]] = None,
        size: int = 1,
        random_state: "RandomState" = None,
    ):
        raise NotImplementedError


@DeveloperAPI
class BaseSampler(Sampler):
    def __str__(self):
        return "Base"


@DeveloperAPI
class Uniform(Sampler):
    def __str__(self):
        return "Uniform"


@DeveloperAPI
class LogUniform(Sampler):
    def __init__(self, base: float = 10):
        self.base = base
        assert self.base > 0, "Base has to be strictly greater than 0"

    def __str__(self):
        return "LogUniform"


@DeveloperAPI
class Normal(Sampler):
    def __init__(self, mean: float = 0.0, sd: float = 0.0):
        self.mean = mean
        self.sd = sd

        assert self.sd > 0, "SD has to be strictly greater than 0"

    def __str__(self):
        return "Normal"


@DeveloperAPI
class Grid(Sampler):
    """Dummy sampler used for grid search"""

    def sample(
        self,
        domain: Domain,
        config: Optional[Union[List[Dict], Dict]] = None,
        size: int = 1,
        random_state: "RandomState" = None,
    ):
        return RuntimeError("Do not call `sample()` on grid.")


@DeveloperAPI
class Float(Domain):
    class _Uniform(Uniform):
        def sample(
            self,
            domain: "Float",
            config: Optional[Union[List[Dict], Dict]] = None,
            size: int = 1,
            random_state: "RandomState" = None,
        ):
            if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
                random_state = _BackwardsCompatibleNumpyRng(random_state)
            assert domain.lower > float("-inf"), "Uniform needs a lower bound"
            assert domain.upper < float("inf"), "Uniform needs a upper bound"
            items = random_state.uniform(domain.lower, domain.upper, size=size)
            return items if len(items) > 1 else domain.cast(items[0])

    class _LogUniform(LogUniform):
        def sample(
            self,
            domain: "Float",
            config: Optional[Union[List[Dict], Dict]] = None,
            size: int = 1,
            random_state: "RandomState" = None,
        ):
            if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
                random_state = _BackwardsCompatibleNumpyRng(random_state)
            assert domain.lower > 0, "LogUniform needs a lower bound greater than 0"
            assert (
                0 < domain.upper < float("inf")
            ), "LogUniform needs a upper bound greater than 0"
            logmin = np.log(domain.lower) / np.log(self.base)
            logmax = np.log(domain.upper) / np.log(self.base)

            items = self.base ** (random_state.uniform(logmin, logmax, size=size))
            return items if len(items) > 1 else domain.cast(items[0])

    class _Normal(Normal):
        def sample(
            self,
            domain: "Float",
            config: Optional[Union[List[Dict], Dict]] = None,
            size: int = 1,
            random_state: "RandomState" = None,
        ):
            if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
                random_state = _BackwardsCompatibleNumpyRng(random_state)
            assert not domain.lower or domain.lower == float(
                "-inf"
            ), "Normal sampling does not allow a lower value bound."
            assert not domain.upper or domain.upper == float(
                "inf"
            ), "Normal sampling does not allow a upper value bound."
            items = random_state.normal(self.mean, self.sd, size=size)
            return items if len(items) > 1 else domain.cast(items[0])

    default_sampler_cls = _Uniform

    def __init__(self, lower: Optional[float], upper: Optional[float]):
        # Need to explicitly check for None
        self.lower = lower if lower is not None else float("-inf")
        self.upper = upper if upper is not None else float("inf")

    def cast(self, value):
        return float(value)

    def uniform(self):
        if not self.lower > float("-inf"):
            raise ValueError(
                "Uniform requires a lower bound. Make sure to set the "
                "`lower` parameter of `Float()`."
            )
        if not self.upper < float("inf"):
            raise ValueError(
                "Uniform requires a upper bound. Make sure to set the "
                "`upper` parameter of `Float()`."
            )
        new = copy(self)
        new.set_sampler(self._Uniform())
        return new

    def loguniform(self, base: float = 10):
        if not self.lower > 0:
            raise ValueError(
                "LogUniform requires a lower bound greater than 0."
                f"Got: {self.lower}. Did you pass a variable that has "
                "been log-transformed? If so, pass the non-transformed value "
                "instead."
            )
        if not 0 < self.upper < float("inf"):
            raise ValueError(
                "LogUniform requires a upper bound greater than 0. "
                f"Got: {self.lower}. Did you pass a variable that has "
                "been log-transformed? If so, pass the non-transformed value "
                "instead."
            )
        new = copy(self)
        new.set_sampler(self._LogUniform(base))
        return new

    def normal(self, mean=0.0, sd=1.0):
        new = copy(self)
        new.set_sampler(self._Normal(mean, sd))
        return new

    def quantized(self, q: float):
        if self.lower > float("-inf") and not isclose(
            self.lower / q, round(self.lower / q)
        ):
            raise ValueError(
                f"Your lower variable bound {self.lower} is not divisible by "
                f"quantization factor {q}."
            )
        if self.upper < float("inf") and not isclose(
            self.upper / q, round(self.upper / q)
        ):
            raise ValueError(
                f"Your upper variable bound {self.upper} is not divisible by "
                f"quantization factor {q}."
            )

        new = copy(self)
        new.set_sampler(Quantized(new.get_sampler(), q), allow_override=True)
        return new

    def is_valid(self, value: float):
        return self.lower <= value <= self.upper

    @property
    def domain_str(self):
        return f"({self.lower}, {self.upper})"


@DeveloperAPI
class Integer(Domain):
    class _Uniform(Uniform):
        def sample(
            self,
            domain: "Integer",
            config: Optional[Union[List[Dict], Dict]] = None,
            size: int = 1,
            random_state: "RandomState" = None,
        ):
            if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
                random_state = _BackwardsCompatibleNumpyRng(random_state)
            items = random_state.integers(domain.lower, domain.upper, size=size)
            return items if len(items) > 1 else domain.cast(items[0])

    class _LogUniform(LogUniform):
        def sample(
            self,
            domain: "Integer",
            config: Optional[Union[List[Dict], Dict]] = None,
            size: int = 1,
            random_state: "RandomState" = None,
        ):
            if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
                random_state = _BackwardsCompatibleNumpyRng(random_state)
            assert domain.lower > 0, "LogUniform needs a lower bound greater than 0"
            assert (
                0 < domain.upper < float("inf")
            ), "LogUniform needs a upper bound greater than 0"
            logmin = np.log(domain.lower) / np.log(self.base)
            logmax = np.log(domain.upper) / np.log(self.base)

            items = self.base ** (random_state.uniform(logmin, logmax, size=size))
            items = np.floor(items).astype(int)
            return items if len(items) > 1 else domain.cast(items[0])

    default_sampler_cls = _Uniform

    def __init__(self, lower, upper):
        self.lower = lower
        self.upper = upper

    def cast(self, value):
        return int(value)

    def quantized(self, q: int):
        new = copy(self)
        new.set_sampler(Quantized(new.get_sampler(), q), allow_override=True)
        return new

    def uniform(self):
        new = copy(self)
        new.set_sampler(self._Uniform())
        return new

    def loguniform(self, base: float = 10):
        if not self.lower > 0:
            raise ValueError(
                "LogUniform requires a lower bound greater than 0."
                f"Got: {self.lower}. Did you pass a variable that has "
                "been log-transformed? If so, pass the non-transformed value "
                "instead."
            )
        if not 0 < self.upper < float("inf"):
            raise ValueError(
                "LogUniform requires a upper bound greater than 0. "
                f"Got: {self.lower}. Did you pass a variable that has "
                "been log-transformed? If so, pass the non-transformed value "
                "instead."
            )
        new = copy(self)
        new.set_sampler(self._LogUniform(base))
        return new

    def is_valid(self, value: int):
        return self.lower <= value <= self.upper

    @property
    def domain_str(self):
        return f"({self.lower}, {self.upper})"


@DeveloperAPI
class Categorical(Domain):
    class _Uniform(Uniform):
        def sample(
            self,
            domain: "Categorical",
            config: Optional[Union[List[Dict], Dict]] = None,
            size: int = 1,
            random_state: "RandomState" = None,
        ):
            if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
                random_state = _BackwardsCompatibleNumpyRng(random_state)
            # do not use .choice() directly on domain.categories
            # as that will coerce them to a single dtype
            indices = random_state.choice(
                np.arange(0, len(domain.categories)), size=size
            )
            items = [domain.categories[index] for index in indices]
            return items if len(items) > 1 else domain.cast(items[0])

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

    def is_valid(self, value: Any):
        return value in self.categories

    @property
    def domain_str(self):
        return f"{self.categories}"


@DeveloperAPI
class Function(Domain):
    class _CallSampler(BaseSampler):
        def __try_fn(self, domain: "Function", config: Dict[str, Any]):
            try:
                return domain.func(config)
            except (AttributeError, KeyError):
                from ray.tune.search.variant_generator import _UnresolvedAccessGuard

                r = domain.func(_UnresolvedAccessGuard({"config": config}))
                logger.warning(
                    "sample_from functions that take a spec dict are "
                    "deprecated. Please update your function to work with "
                    "the config dict directly."
                )
                return r

        def sample(
            self,
            domain: "Function",
            config: Optional[Union[List[Dict], Dict]] = None,
            size: int = 1,
            random_state: "RandomState" = None,
        ):
            if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
                random_state = _BackwardsCompatibleNumpyRng(random_state)
            if domain.pass_config:
                items = [
                    self.__try_fn(domain, config[i])
                    if isinstance(config, list)
                    else self.__try_fn(domain, config)
                    for i in range(size)
                ]
            else:
                items = [domain.func() for i in range(size)]

            return items if len(items) > 1 else domain.cast(items[0])

    default_sampler_cls = _CallSampler

    def __init__(self, func: Callable):
        sig = signature(func)

        pass_config = True  # whether we should pass `config` when calling `func`
        try:
            sig.bind({})
        except TypeError:
            pass_config = False

        if not pass_config:
            try:
                sig.bind()
            except TypeError as exc:
                raise ValueError(
                    "The function passed to a `Function` parameter must be "
                    "callable with either 0 or 1 parameters."
                ) from exc

        self.pass_config = pass_config
        self.func = func

    def is_function(self):
        return True

    def is_valid(self, value: Any):
        return True  # This is user-defined, so lets not assume anything

    @property
    def domain_str(self):
        return f"{self.func}()"


@DeveloperAPI
class Quantized(Sampler):
    def __init__(self, sampler: Sampler, q: Union[float, int]):
        self.sampler = sampler
        self.q = q

        assert self.sampler, "Quantized() expects a sampler instance"

    def get_sampler(self):
        return self.sampler

    def sample(
        self,
        domain: Domain,
        config: Optional[Union[List[Dict], Dict]] = None,
        size: int = 1,
        random_state: "RandomState" = None,
    ):
        if not isinstance(random_state, _BackwardsCompatibleNumpyRng):
            random_state = _BackwardsCompatibleNumpyRng(random_state)

        if self.q == 1:
            return self.sampler.sample(domain, config, size, random_state=random_state)

        quantized_domain = copy(domain)
        quantized_domain.lower = np.ceil(domain.lower / self.q) * self.q
        quantized_domain.upper = np.floor(domain.upper / self.q) * self.q
        values = self.sampler.sample(
            quantized_domain, config, size, random_state=random_state
        )
        quantized = np.round(np.divide(values, self.q)) * self.q

        if not isinstance(quantized, np.ndarray):
            return domain.cast(quantized)
        return list(quantized)


@PublicAPI
def sample_from(func: Callable[[Dict], Any]):
    """Specify that tune should sample configuration values from this function.

    Arguments:
        func: An callable function to draw a sample from.
    """
    return Function(func)


@PublicAPI
def uniform(lower: float, upper: float):
    """Sample a float value uniformly between ``lower`` and ``upper``.

    Sampling from ``tune.uniform(1, 10)`` is equivalent to sampling from
    ``np.random.uniform(1, 10))``

    """
    return Float(lower, upper).uniform()


@PublicAPI
def quniform(lower: float, upper: float, q: float):
    """Sample a quantized float value uniformly between ``lower`` and ``upper``.

    Sampling from ``tune.uniform(1, 10)`` is equivalent to sampling from
    ``np.random.uniform(1, 10))``

    The value will be quantized, i.e. rounded to an integer increment of ``q``.
    Quantization makes the upper bound inclusive.

    """
    return Float(lower, upper).uniform().quantized(q)


@PublicAPI
def loguniform(lower: float, upper: float, base: float = 10):
    """Sugar for sampling in different orders of magnitude.

    Args:
        lower: Lower boundary of the output interval (e.g. 1e-4)
        upper: Upper boundary of the output interval (e.g. 1e-2)
        base: Base of the log. Defaults to 10.

    """
    return Float(lower, upper).loguniform(base)


@PublicAPI
def qloguniform(lower: float, upper: float, q: float, base: float = 10):
    """Sugar for sampling in different orders of magnitude.

    The value will be quantized, i.e. rounded to an integer increment of ``q``.

    Quantization makes the upper bound inclusive.

    Args:
        lower: Lower boundary of the output interval (e.g. 1e-4)
        upper: Upper boundary of the output interval (e.g. 1e-2)
        q: Quantization number. The result will be rounded to an
            integer increment of this value.
        base: Base of the log. Defaults to 10.

    """
    return Float(lower, upper).loguniform(base).quantized(q)


@PublicAPI
def choice(categories: Sequence):
    """Sample a categorical value.

    Sampling from ``tune.choice([1, 2])`` is equivalent to sampling from
    ``np.random.choice([1, 2])``

    """
    return Categorical(categories).uniform()


@PublicAPI
def randint(lower: int, upper: int):
    """Sample an integer value uniformly between ``lower`` and ``upper``.

    ``lower`` is inclusive, ``upper`` is exclusive.

    Sampling from ``tune.randint(10)`` is equivalent to sampling from
    ``np.random.randint(10)``

    .. versionchanged:: 1.5.0
        When converting Ray Tune configs to searcher-specific search spaces,
        the lower and upper limits are adjusted to keep compatibility with
        the bounds stated in the docstring above.

    """
    return Integer(lower, upper).uniform()


@PublicAPI
def lograndint(lower: int, upper: int, base: float = 10):
    """Sample an integer value log-uniformly between ``lower`` and ``upper``,
    with ``base`` being the base of logarithm.

    ``lower`` is inclusive, ``upper`` is exclusive.

    .. versionchanged:: 1.5.0
        When converting Ray Tune configs to searcher-specific search spaces,
        the lower and upper limits are adjusted to keep compatibility with
        the bounds stated in the docstring above.

    """
    return Integer(lower, upper).loguniform(base)


@PublicAPI
def qrandint(lower: int, upper: int, q: int = 1):
    """Sample an integer value uniformly between ``lower`` and ``upper``.

    ``lower`` is inclusive, ``upper`` is also inclusive (!).

    The value will be quantized, i.e. rounded to an integer increment of ``q``.
    Quantization makes the upper bound inclusive.

    .. versionchanged:: 1.5.0
        When converting Ray Tune configs to searcher-specific search spaces,
        the lower and upper limits are adjusted to keep compatibility with
        the bounds stated in the docstring above.

    """
    return Integer(lower, upper).uniform().quantized(q)


@PublicAPI
def qlograndint(lower: int, upper: int, q: int, base: float = 10):
    """Sample an integer value log-uniformly between ``lower`` and ``upper``,
    with ``base`` being the base of logarithm.

    ``lower`` is inclusive, ``upper`` is also inclusive (!).

    The value will be quantized, i.e. rounded to an integer increment of ``q``.
    Quantization makes the upper bound inclusive.

    .. versionchanged:: 1.5.0
        When converting Ray Tune configs to searcher-specific search spaces,
        the lower and upper limits are adjusted to keep compatibility with
        the bounds stated in the docstring above.

    """
    return Integer(lower, upper).loguniform(base).quantized(q)


@PublicAPI
def randn(mean: float = 0.0, sd: float = 1.0):
    """Sample a float value normally with ``mean`` and ``sd``.

    Args:
        mean: Mean of the normal distribution. Defaults to 0.
        sd: SD of the normal distribution. Defaults to 1.

    """
    return Float(None, None).normal(mean, sd)


@PublicAPI
def qrandn(mean: float, sd: float, q: float):
    """Sample a float value normally with ``mean`` and ``sd``.

    The value will be quantized, i.e. rounded to an integer increment of ``q``.

    Args:
        mean: Mean of the normal distribution.
        sd: SD of the normal distribution.
        q: Quantization number. The result will be rounded to an
            integer increment of this value.

    """
    return Float(None, None).normal(mean, sd).quantized(q)
