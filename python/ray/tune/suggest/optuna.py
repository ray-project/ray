import time
import logging
import pickle
import functools
import warnings
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ray.tune.result import DEFAULT_METRIC, TRAINING_ITERATION
from ray.tune.sample import Categorical, Domain, Float, Integer, LogUniform, \
    Quantized, Uniform
from ray.tune.suggest.suggestion import UNRESOLVED_SEARCH_SPACE, \
    UNDEFINED_METRIC_MODE, UNDEFINED_SEARCH_SPACE
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils.util import flatten_dict, unflatten_dict, \
    validate_warmstart

try:
    import optuna as ot
    from optuna.distributions import BaseDistribution as OptunaDistribution
    from optuna.samplers import BaseSampler
    from optuna.trial import TrialState as OptunaTrialState
    from optuna.trial import Trial as OptunaTrial
except ImportError:
    ot = None
    OptunaDistribution = None
    BaseSampler = None
    OptunaTrialState = None
    OptunaTrial = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)

# print a warning if define by run function takes longer than this to execute
DEFINE_BY_RUN_WARN_THRESHOLD_S = 1  # 1 is arbitrary


# Deprecate: 1.5
class _Param:
    def __getattr__(self, item):
        def _inner(*args, **kwargs):
            return (item, args, kwargs)

        return _inner


param = _Param()


class _OptunaTrialSuggestCaptor:
    """Utility to capture returned values from Optuna's suggest_ methods.

    This will wrap around the ``optuna.Trial` object and decorate all
    `suggest_` callables with a function capturing the returned value,
    which will be saved in the ``captured_values`` dict.
    """

    def __init__(self, ot_trial: OptunaTrial) -> None:
        self.ot_trial = ot_trial
        self.captured_values: Dict[str, Any] = {}

    def _get_wrapper(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # name is always the first arg for suggest_ methods
            name = kwargs.get("name", args[0])
            ret = func(*args, **kwargs)
            self.captured_values[name] = ret
            return ret

        return wrapper

    def __getattr__(self, item_name: str) -> Any:
        item = getattr(self.ot_trial, item_name)
        if item_name.startswith("suggest_") and callable(item):
            return self._get_wrapper(item)
        return item


class OptunaSearch(Searcher):
    """A wrapper around Optuna to provide trial suggestions.

    `Optuna <https://optuna.org/>`_ is a hyperparameter optimization library.
    In contrast to other libraries, it employs define-by-run style
    hyperparameter definitions.

    This Searcher is a thin wrapper around Optuna's search algorithms.
    You can pass any Optuna sampler, which will be used to generate
    hyperparameter suggestions.

    Args:
        space (dict|Callable): Hyperparameter search space definition for
            Optuna's sampler. This can be either a :class:`dict` with
            parameter names as keys and ``optuna.distributions`` as values,
            or a Callable - in which case, it should be a define-by-run
            function using ``optuna.trial`` to obtain the hyperparameter
            values. The function should return either a :class:`dict` of
            constant values with names as keys, or None.
            For more information, see https://optuna.readthedocs.io\
/en/stable/tutorial/10_key_features/002_configurations.html.

            .. warning::
                No actual computation should take place in the define-by-run
                function. Instead, put the training logic inside the function
                or class trainable passed to ``tune.run``.

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
        seed (int): Seed to initialize sampler with. This parameter is only
            used when ``sampler=None``. In all other cases, the sampler
            you pass should be initialized with the seed already.
        evaluated_rewards (list): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate.

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

        # Equivalent Optuna define-by-run function approach:

        def define_search_space(trial: optuna.Trial):
            trial.suggest_float("a", 6, 8)
            trial.suggest_float("b", 1e-4, 1e-2, log=True)
            # training logic goes into trainable, this is just
            # for search space definition

        optuna_search = OptunaSearch(
            define_search_space,
            metric="loss",
            mode="min")

        tune.run(trainable, search_alg=optuna_search)

    .. versionadded:: 0.8.8

    """

    def __init__(self,
                 space: Optional[Union[Dict[str, "OptunaDistribution"], List[
                     Tuple], Callable[["OptunaTrial"], Optional[Dict[
                         str, Any]]]]] = None,
                 metric: Optional[str] = None,
                 mode: Optional[str] = None,
                 points_to_evaluate: Optional[List[Dict]] = None,
                 sampler: Optional["BaseSampler"] = None,
                 seed: Optional[int] = None,
                 evaluated_rewards: Optional[List] = None):
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
        self._evaluated_rewards = evaluated_rewards

        self._study_name = "optuna"  # Fixed study name for in-memory storage

        if sampler and seed:
            logger.warning(
                "You passed an initialized sampler to `OptunaSearch`. The "
                "`seed` parameter has to be passed to the sampler directly "
                "and will be ignored.")

        self._sampler = sampler or ot.samplers.TPESampler(seed=seed)

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

        if self._points_to_evaluate:
            validate_warmstart(
                self._space,
                self._points_to_evaluate,
                self._evaluated_rewards,
                validate_point_name_lengths=not callable(self._space))
            if self._evaluated_rewards:
                for point, reward in zip(self._points_to_evaluate,
                                         self._evaluated_rewards):
                    self.add_evaluated_point(point, reward)
            else:
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

    def _suggest_from_define_by_run_func(
            self, func: Callable[["OptunaTrial"], Optional[Dict[str, Any]]],
            ot_trial: "OptunaTrial") -> Dict:
        captor = _OptunaTrialSuggestCaptor(ot_trial)
        time_start = time.time()
        ret = func(captor)
        time_taken = time.time() - time_start
        if time_taken > DEFINE_BY_RUN_WARN_THRESHOLD_S:
            warnings.warn(
                "Define-by-run function passed in the `space` argument "
                f"took {time_taken} seconds to "
                "run. Ensure that actual computation, training takes "
                "place inside Tune's train functions or Trainables "
                "passed to `tune.run`.")
        if ret is not None:
            if not isinstance(ret, dict):
                raise TypeError(
                    "The return value of the define-by-run function "
                    "passed in the `space` argument should be "
                    "either None or a `dict` with `str` keys. "
                    f"Got {type(ret)}.")
            if not all(isinstance(k, str) for k in ret.keys()):
                raise TypeError(
                    "At least one of the keys in the dict returned by the "
                    "define-by-run function passed in the `space` argument "
                    "was not a `str`.")
        return {
            **captor.captured_values,
            **ret
        } if ret else captor.captured_values

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
        elif callable(self._space):
            if trial_id not in self._ot_trials:
                self._ot_trials[trial_id] = self._ot_study.ask()

            ot_trial = self._ot_trials[trial_id]

            params = self._suggest_from_define_by_run_func(
                self._space, ot_trial)
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
        ot_trial_state = OptunaTrialState.COMPLETE
        if val is None:
            if error:
                ot_trial_state = OptunaTrialState.FAIL
            else:
                ot_trial_state = OptunaTrialState.PRUNED
        try:
            self._ot_study.tell(ot_trial, val, state=ot_trial_state)
        except ValueError as exc:
            logger.warning(exc)  # E.g. if NaN was reported

    def add_evaluated_point(self,
                            parameters: Dict,
                            value: float,
                            error: bool = False,
                            pruned: bool = False,
                            intermediate_values: Optional[List[float]] = None):
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

        ot_trial_state = OptunaTrialState.COMPLETE
        if error:
            ot_trial_state = OptunaTrialState.FAIL
        elif pruned:
            ot_trial_state = OptunaTrialState.PRUNED

        if intermediate_values:
            intermediate_values_dict = {
                i: value
                for i, value in enumerate(intermediate_values)
            }
        else:
            intermediate_values_dict = None

        trial = ot.trial.create_trial(
            state=ot_trial_state,
            value=value,
            params=parameters,
            distributions=self._space,
            intermediate_values=intermediate_values_dict)

        self._ot_study.add_trial(trial)

    def save(self, checkpoint_path: str):
        save_object = (self._sampler, self._ot_trials, self._ot_study,
                       self._points_to_evaluate, self._evaluated_rewards)
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        if len(save_object) == 5:
            self._sampler, self._ot_trials, self._ot_study, \
                self._points_to_evaluate, self._evaluated_rewards = save_object
        else:
            # Backwards compatibility
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
                if isinstance(sampler, LogUniform):
                    logger.warning(
                        "Optuna does not handle quantization in loguniform "
                        "sampling. The parameter will be passed but it will "
                        "probably be ignored.")

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
                        domain.lower, domain.upper - 1, step=quantize or 1)
                elif isinstance(sampler, Uniform):
                    # Upper bound should be inclusive for quantization and
                    # exclusive otherwise
                    return ot.distributions.IntUniformDistribution(
                        domain.lower,
                        domain.upper - int(bool(not quantize)),
                        step=quantize or 1)
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
