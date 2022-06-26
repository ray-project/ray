from ray.tune.search.search_algorithm import SearchAlgorithm
from ray.tune.search.searcher import Searcher
from ray.tune.search.concurrency_limiter import ConcurrencyLimiter
from ray.tune.search.repeater import Repeater

from ray.tune.search.basic_variant import BasicVariantGenerator
from ray.tune.search.variant_generator import grid_search
from ray.tune.search.search_generator import SearchGenerator

from ray._private.utils import get_function_args


def _import_variant_generator():
    return BasicVariantGenerator


def _import_ax_search():
    from ray.tune.search.ax.ax_search import AxSearch

    return AxSearch


def _import_blendsearch_search():
    from ray.tune.search.flaml.flaml_search import BlendSearch

    return BlendSearch


def _import_cfo_search():
    from ray.tune.search.flaml.flaml_search import CFO

    return CFO


def _import_dragonfly_search():
    from ray.tune.search.dragonfly.dragonfly_search import DragonflySearch

    return DragonflySearch


def _import_skopt_search():
    from ray.tune.search.skopt.skopt_search import SkOptSearch

    return SkOptSearch


def _import_hyperopt_search():
    from ray.tune.search.hyperopt.hyperopt_search import HyperOptSearch

    return HyperOptSearch


def _import_bayesopt_search():
    from ray.tune.search.bayesopt.bayesopt_search import BayesOptSearch

    return BayesOptSearch


def _import_bohb_search():
    from ray.tune.search.bohb.bohb_search import TuneBOHB

    return TuneBOHB


def _import_nevergrad_search():
    from ray.tune.search.nevergrad.nevergrad_search import NevergradSearch

    return NevergradSearch


def _import_optuna_search():
    from ray.tune.search.optuna.optuna_search import OptunaSearch

    return OptunaSearch


def _import_zoopt_search():
    from ray.tune.search.zoopt.zoopt_search import ZOOptSearch

    return ZOOptSearch


def _import_sigopt_search():
    from ray.tune.search.sigopt.sigopt_search import SigOptSearch

    return SigOptSearch


def _import_hebo_search():
    from ray.tune.search.hebo.hebo_search import HEBOSearch

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
        search_alg: The search algorithm to use.
        metric: The training result objective value attribute. Stopping
            procedures will use this attribute.
        mode: One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        **kwargs: Additional parameters.
            These keyword arguments will be passed to the initialization
            function of the chosen class.
    Returns:
        ray.tune.search.Searcher: The search algorithm.
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


UNRESOLVED_SEARCH_SPACE = str(
    "You passed a `{par}` parameter to {cls} that contained unresolved search "
    "space definitions. {cls} should however be instantiated with fully "
    "configured search spaces only. To use Ray Tune's automatic search space "
    "conversion, pass the space definition as part of the `config` argument "
    "to `tune.run()` instead."
)

UNDEFINED_SEARCH_SPACE = str(
    "Trying to sample a configuration from {cls}, but no search "
    "space has been defined. Either pass the `{space}` argument when "
    "instantiating the search algorithm, or pass a `config` to "
    "`tune.run()`."
)

UNDEFINED_METRIC_MODE = str(
    "Trying to sample a configuration from {cls}, but the `metric` "
    "({metric}) or `mode` ({mode}) parameters have not been set. "
    "Either pass these arguments when instantiating the search algorithm, "
    "or pass them to `tune.run()`."
)


__all__ = [
    "SearchAlgorithm",
    "Searcher",
    "ConcurrencyLimiter",
    "Repeater",
    "BasicVariantGenerator",
    "grid_search",
    "SearchGenerator",
    "UNRESOLVED_SEARCH_SPACE",
    "UNDEFINED_SEARCH_SPACE",
    "UNDEFINED_METRIC_MODE",
]
