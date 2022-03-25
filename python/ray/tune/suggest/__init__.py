from ray._private.utils import get_function_args
from ray.tune.suggest.search import SearchAlgorithm
from ray.tune.suggest.basic_variant import BasicVariantGenerator
from ray.tune.suggest.suggestion import Searcher, ConcurrencyLimiter
from ray.tune.suggest.search_generator import SearchGenerator
from ray.tune.suggest.variant_generator import grid_search
from ray.tune.suggest.repeater import Repeater


def _import_variant_generator():
    return BasicVariantGenerator


def _import_ax_search():
    from ray.tune.suggest.ax import AxSearch

    return AxSearch


def _import_blendsearch_search():
    from ray.tune.suggest.flaml import BlendSearch

    return BlendSearch


def _import_cfo_search():
    from ray.tune.suggest.flaml import CFO

    return CFO


def _import_dragonfly_search():
    from ray.tune.suggest.dragonfly import DragonflySearch

    return DragonflySearch


def _import_skopt_search():
    from ray.tune.suggest.skopt import SkOptSearch

    return SkOptSearch


def _import_hyperopt_search():
    from ray.tune.suggest.hyperopt import HyperOptSearch

    return HyperOptSearch


def _import_bayesopt_search():
    from ray.tune.suggest.bayesopt import BayesOptSearch

    return BayesOptSearch


def _import_bohb_search():
    from ray.tune.suggest.bohb import TuneBOHB

    return TuneBOHB


def _import_nevergrad_search():
    from ray.tune.suggest.nevergrad import NevergradSearch

    return NevergradSearch


def _import_optuna_search():
    from ray.tune.suggest.optuna import OptunaSearch

    return OptunaSearch


def _import_zoopt_search():
    from ray.tune.suggest.zoopt import ZOOptSearch

    return ZOOptSearch


def _import_sigopt_search():
    from ray.tune.suggest.sigopt import SigOptSearch

    return SigOptSearch


def _import_hebo_search():
    from ray.tune.suggest.hebo import HEBOSearch

    return HEBOSearch


SEARCH_ALG_IMPORT = {
    "variant_generator": _import_variant_generator,
    "random": _import_variant_generator,
    "ax": _import_ax_search,
    "dragonfly": _import_dragonfly_search,
    "skopt": _import_skopt_search,
    "hyperopt": _import_hyperopt_search,
    "bayesopt": _import_bayesopt_search,
    "bohb": _import_bohb_search,
    "nevergrad": _import_nevergrad_search,
    "optuna": _import_optuna_search,
    "zoopt": _import_zoopt_search,
    "sigopt": _import_sigopt_search,
    "hebo": _import_hebo_search,
    "blendsearch": _import_blendsearch_search,
    "cfo": _import_cfo_search,
}


def create_searcher(
    search_alg,
    **kwargs,
):
    """Instantiate a search algorithm based on the given string.

    This is useful for swapping between different search algorithms.

    Args:
        search_alg (str): The search algorithm to use.
        metric (str): The training result objective value attribute. Stopping
            procedures will use this attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        **kwargs: Additional parameters.
            These keyword arguments will be passed to the initialization
            function of the chosen class.
    Returns:
        ray.tune.suggest.Searcher: The search algorithm.
    Example:
        >>> from ray import tune
        >>> search_alg = tune.create_searcher('ax')
    """

    search_alg = search_alg.lower()
    if search_alg not in SEARCH_ALG_IMPORT:
        raise ValueError(
            f"The `search_alg` argument must be one of "
            f"{list(SEARCH_ALG_IMPORT)}. "
            f"Got: {search_alg}"
        )

    SearcherClass = SEARCH_ALG_IMPORT[search_alg]()

    search_alg_args = get_function_args(SearcherClass)
    trimmed_kwargs = {k: v for k, v in kwargs.items() if k in search_alg_args}

    return SearcherClass(**trimmed_kwargs)


__all__ = [
    "SearchAlgorithm",
    "Searcher",
    "BasicVariantGenerator",
    "SearchGenerator",
    "grid_search",
    "Repeater",
    "ConcurrencyLimiter",
]


def BayesOptSearch(*args, **kwargs):
    raise DeprecationWarning(
        """This class has been moved. Please import via
        `from ray.tune.suggest.bayesopt import BayesOptSearch`"""
    )


def HyperOptSearch(*args, **kwargs):
    raise DeprecationWarning(
        """This class has been moved. Please import via
        `from ray.tune.suggest.hyperopt import HyperOptSearch`"""
    )


def NevergradSearch(*args, **kwargs):
    raise DeprecationWarning(
        """This class has been moved. Please import via
        `from ray.tune.suggest.nevergrad import NevergradSearch`"""
    )


def SkOptSearch(*args, **kwargs):
    raise DeprecationWarning(
        """This class has been moved. Please import via
        `from ray.tune.suggest.skopt import SkOptSearch`"""
    )


def SigOptSearch(*args, **kwargs):
    raise DeprecationWarning(
        """This class has been moved. Please import via
        `from ray.tune.suggest.sigopt import SigOptSearch`"""
    )
