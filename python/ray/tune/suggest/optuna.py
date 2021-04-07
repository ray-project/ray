import logging
import pickle
from typing import Any, Dict, List, Optional, Tuple, Union

from ray.tune.result import DEFAULT_METRIC, TRAINING_ITERATION
from ray.tune.sample import Categorical, Domain, Float, Integer, LogUniform, \
    Quantized, Uniform
from ray.tune.suggest.suggestion import UNRESOLVED_SEARCH_SPACE, \
    UNDEFINED_METRIC_MODE, UNDEFINED_SEARCH_SPACE
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils.util import flatten_dict, unflatten_dict

try:
    import optuna as ot
    from optuna.samplers import BaseSampler
except ImportError:
    ot = None
    BaseSampler = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


# Deprecate: 1.5
class _Param:
    def __getattr__(self, item):
        def _inner(*args, **kwargs):
            return (item, args, kwargs)

        return _inner


param = _Param()


class OptunaSearch(Searcher):
    """A wrapper around Optuna to provide trial suggestions.

    `Optuna <https://optuna.org/>`_ is a hyperparameter optimization library.
    In contrast to other libraries, it employs define-by-run style
    hyperparameter definitions.

    This Searcher is a thin wrapper around Optuna's search algorithms.
    You can pass any Optuna sampler, which will be used to generate
    hyperparameter suggestions.

    Please note that this wrapper does not support define-by-run, so the
    search space will be configured before running the optimization. You will
    also need to use a Tune trainable (e.g. using the function API) with
    this wrapper.

    For defining the search space, use ``ray.tune.suggest.optuna.param``
    (see example).

    Args:
        space (list): Hyperparameter search space definition for Optuna's
            sampler. This is a list, and samples for the parameters will
            be obtained in order.
        metric (str): The training result objective value attribute. If None
            but a mode was passed, the anonymous metric `_metric` will be used
            per default.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate (list): Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want to run first to help the algorithm make better suggestions
            for future parameters. Needs to be a list of dicts containing the
            configurations.
        sampler (optuna.samplers.BaseSampler): Optuna sampler used to
            draw hyperparameter configurations. Defaults to ``TPESampler``.

    Tune automatically converts search spaces to Optuna's format:

    .. code-block:: python

        from ray.tune.suggest.optuna import OptunaSearch

        config = {
            "a": tune.uniform(6, 8)
            "b": tune.loguniform(1e-4, 1e-2)
        }

        optuna_search = OptunaSearch(
            metric="loss",
            mode="min")

        tune.run(trainable, config=config, search_alg=optuna_search)

    If you would like to pass the search space manually, the code would
    look like this:

    .. code-block:: python

        from ray.tune.suggest.optuna import OptunaSearch
        import optuna

        config = {
            "a": optuna.distributions.UniformDistribution(6, 8),
            "b": optuna.distributions.LogUniformDistribution(1e-4, 1e-2),
        }

        optuna_search = OptunaSearch(
            space,
            metric="loss",
            mode="min")

        tune.run(trainable, search_alg=optuna_search)

    .. versionadded:: 0.8.8

    """

    def __init__(self,
                 space: Optional[Union[Dict, List[Tuple]]] = None,
                 metric: Optional[str] = None,
                 mode: Optional[str] = None,
                 points_to_evaluate: Optional[List[Dict]] = None,
                 sampler: Optional[BaseSampler] = None):
        assert ot is not None, (
            "Optuna must be installed! Run `pip install optuna`.")
        super(OptunaSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=None,
            use_early_stopped_trials=None)

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(
                        par="space", cls=type(self).__name__))
                space = self.convert_search_space(space)
            else:
                # Flatten to support nested dicts
                space = flatten_dict(space, "/")

        # Deprecate: 1.5
        if isinstance(space, list):
            logger.warning(
                "Passing lists of `param.suggest_*()` calls to OptunaSearch "
                "as a search space is deprecated and will be removed in "
                "a future release of Ray. Please pass a dict mapping "
                "to `optuna.distributions` objects instead.")

        self._space = space

        self._points_to_evaluate = points_to_evaluate or []

        self._study_name = "optuna"  # Fixed study name for in-memory storage
        self._sampler = sampler or ot.samplers.TPESampler()
        assert isinstance(self._sampler, BaseSampler), \
            "You can only pass an instance of `optuna.samplers.BaseSampler` " \
            "as a sampler to `OptunaSearcher`."

        self._ot_trials = {}
        self._ot_study = None
        if self._space:
            self._setup_study(mode)

    def _setup_study(self, mode: str):
        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        pruner = ot.pruners.NopPruner()
        storage = ot.storages.InMemoryStorage()

        self._ot_study = ot.study.create_study(
            storage=storage,
            sampler=self._sampler,
            pruner=pruner,
            study_name=self._study_name,
            direction="minimize" if mode == "min" else "maximize",
            load_if_exists=True)

        for point in self._points_to_evaluate:
            self._ot_study.enqueue_trial(point)

    def set_search_properties(self, metric: Optional[str], mode: Optional[str],
                              config: Dict) -> bool:
        if self._space:
            return False
        space = self.convert_search_space(config)
        self._space = space
        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._setup_study(mode)
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._space:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="space"))
        if not self._metric or not self._mode:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__,
                    metric=self._metric,
                    mode=self._mode))

        if isinstance(self._space, list):
            # Keep for backwards compatibility
            # Deprecate: 1.5
            if trial_id not in self._ot_trials:
                self._ot_trials[trial_id] = self._ot_study.ask()

            ot_trial = self._ot_trials[trial_id]

            # getattr will fetch the trial.suggest_ function on Optuna trials
            params = {
                args[0] if len(args) > 0 else kwargs["name"]: getattr(
                    ot_trial, fn)(*args, **kwargs)
                for (fn, args, kwargs) in self._space
            }
        else:
            # Use Optuna ask interface (since version 2.6.0)
            if trial_id not in self._ot_trials:
                self._ot_trials[trial_id] = self._ot_study.ask(
                    fixed_distributions=self._space)
            ot_trial = self._ot_trials[trial_id]
            params = ot_trial.params

        return unflatten_dict(params)

    def on_trial_result(self, trial_id: str, result: Dict):
        metric = result[self.metric]
        step = result[TRAINING_ITERATION]
        ot_trial = self._ot_trials[trial_id]
        ot_trial.report(metric, step)

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False):
        ot_trial = self._ot_trials[trial_id]

        val = result.get(self.metric, None) if result else None
        try:
            self._ot_study.tell(ot_trial, val)
        except ValueError as exc:
            logger.warning(exc)  # E.g. if NaN was reported

    def save(self, checkpoint_path: str):
        save_object = (self._sampler, self._ot_trials, self._ot_study,
                       self._points_to_evaluate)
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self._sampler, self._ot_trials, self._ot_study, \
            self._points_to_evaluate = save_object

    @staticmethod
    def convert_search_space(spec: Dict) -> Dict[str, Any]:
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if not domain_vars and not grid_vars:
            return {}

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to an Optuna search space.")

        # Flatten and resolve again after checking for grid search.
        spec = flatten_dict(spec, prevent_delimiter=True)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        def resolve_value(domain: Domain) -> ot.distributions.BaseDistribution:
            quantize = None

            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                quantize = sampler.q
                sampler = sampler.sampler

            if isinstance(domain, Float):
                if isinstance(sampler, LogUniform):
                    if quantize:
                        logger.warning(
                            "Optuna does not support both quantization and "
                            "sampling from LogUniform. Dropped quantization.")
                    return ot.distributions.LogUniformDistribution(
                        domain.lower, domain.upper)

                elif isinstance(sampler, Uniform):
                    if quantize:
                        return ot.distributions.DiscreteUniformDistribution(
                            domain.lower, domain.upper, quantize)
                    return ot.distributions.UniformDistribution(
                        domain.lower, domain.upper)

            elif isinstance(domain, Integer):
                if isinstance(sampler, LogUniform):
                    return ot.distributions.IntLogUniformDistribution(
                        domain.lower, domain.upper, step=quantize or 1)
                elif isinstance(sampler, Uniform):
                    return ot.distributions.IntUniformDistribution(
                        domain.lower, domain.upper, step=quantize or 1)
            elif isinstance(domain, Categorical):
                if isinstance(sampler, Uniform):
                    return ot.distributions.CategoricalDistribution(
                        domain.categories)

            raise ValueError(
                "Optuna search does not support parameters of type "
                "`{}` with samplers of type `{}`".format(
                    type(domain).__name__,
                    type(domain.sampler).__name__))

        # Parameter name is e.g. "a/b/c" for nested dicts
        values = {
            "/".join(path): resolve_value(domain)
            for path, domain in domain_vars
        }

        return values
