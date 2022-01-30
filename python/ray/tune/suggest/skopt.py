import copy
import logging
import numpy as np
import pickle
from typing import Dict, List, Optional, Tuple, Union, Any

from ray.tune.result import DEFAULT_METRIC
from ray.tune.sample import Categorical, Domain, Float, Integer, Quantized, LogUniform
from ray.tune.suggest import Searcher
from ray.tune.suggest.suggestion import (
    UNRESOLVED_SEARCH_SPACE,
    UNDEFINED_METRIC_MODE,
    UNDEFINED_SEARCH_SPACE,
)
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils import flatten_dict
from ray.tune.utils.util import is_nan_or_inf, unflatten_dict, validate_warmstart

try:
    import skopt as sko
except ImportError:
    sko = None

logger = logging.getLogger(__name__)


class SkOptSearch(Searcher):
    """Uses Scikit Optimize (skopt) to optimize hyperparameters.

    Scikit-optimize is a black-box optimization library.
    Read more here: https://scikit-optimize.github.io.

    You will need to install Scikit-Optimize to use this module.

    .. code-block:: bash

        pip install scikit-optimize

    This Search Algorithm requires you to pass in a `skopt Optimizer object`_.

    This searcher will automatically filter out any NaN, inf or -inf
    results.

    Parameters:
        optimizer (skopt.optimizer.Optimizer): Optimizer provided
            from skopt.
        space (dict|list): A dict mapping parameter names to valid parameters,
            i.e. tuples for numerical parameters and lists for categorical
            parameters. If you passed an optimizer instance as the
            `optimizer` argument, this should be a list of parameter names
            instead.
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
        evaluated_rewards (list): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate. (See tune/examples/skopt_example.py)
        convert_to_python (bool): SkOpt outputs numpy primitives (e.g.
            ``np.int64``) instead of Python types. If this setting is set
            to ``True``, the values will be converted to Python primitives.
        max_concurrent: Deprecated.
        use_early_stopped_trials: Deprecated.

    Tune automatically converts search spaces to SkOpt's format:

    .. code-block:: python

        config = {
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100)
        }

        current_best_params = [
            {
                "width": 10,
                "height": 0,
            },
            {
                "width": 15,
                "height": -20,
            }
        ]

        skopt_search = SkOptSearch(
            metric="mean_loss",
            mode="min",
            points_to_evaluate=current_best_params)

        tune.run(my_trainable, config=config, search_alg=skopt_search)

    If you would like to pass the search space/optimizer manually,
    the code would look like this:

    .. code-block:: python

        parameter_names = ["width", "height"]
        parameter_ranges = [(0,20),(-100,100)]
        current_best_params = [[10, 0], [15, -20]]

        skopt_search = SkOptSearch(
            parameter_names=parameter_names,
            parameter_ranges=parameter_ranges,
            metric="mean_loss",
            mode="min",
            points_to_evaluate=current_best_params)

        tune.run(my_trainable, search_alg=skopt_search)

    """

    def __init__(
        self,
        optimizer: Optional["sko.optimizer.Optimizer"] = None,
        space: Union[List[str], Dict[str, Union[Tuple, List]]] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        points_to_evaluate: Optional[List[Dict]] = None,
        evaluated_rewards: Optional[List] = None,
        convert_to_python: bool = True,
        max_concurrent: Optional[int] = None,
        use_early_stopped_trials: Optional[bool] = None,
    ):
        assert sko is not None, (
            "skopt must be installed! "
            "You can install Skopt with the command: "
            "`pip install scikit-optimize`."
        )

        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        self.max_concurrent = max_concurrent
        super(SkOptSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials,
        )

        self._initial_points = []
        self._parameters = None
        self._parameter_names = None
        self._parameter_ranges = None

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(par="space", cls=type(self))
                )
                space = self.convert_search_space(space, join=True)

        self._space = space

        if self._space:
            if isinstance(optimizer, sko.Optimizer):
                if not isinstance(space, list):
                    raise ValueError(
                        "You passed an optimizer instance to SkOpt. Your "
                        "`space` parameter should be a list of parameter"
                        "names."
                    )
                self._parameter_names = space
            else:
                self._parameter_names = list(space.keys())
                self._parameter_ranges = list(space.values())

        self._points_to_evaluate = copy.deepcopy(points_to_evaluate)

        self._evaluated_rewards = evaluated_rewards

        self._convert_to_python = convert_to_python

        self._skopt_opt = optimizer
        if self._skopt_opt or self._space:
            self._setup_skopt()

        self._live_trial_mapping = {}

    def _setup_skopt(self):
        if self._points_to_evaluate and isinstance(self._points_to_evaluate, list):
            if isinstance(self._points_to_evaluate[0], list):
                # Keep backwards compatibility
                self._points_to_evaluate = [
                    dict(zip(self._parameter_names, point))
                    for point in self._points_to_evaluate
                ]
            # Else: self._points_to_evaluate is already in correct format

        validate_warmstart(
            self._parameter_names, self._points_to_evaluate, self._evaluated_rewards
        )

        if not self._skopt_opt:
            if not self._space:
                raise ValueError(
                    "If you don't pass an optimizer instance to SkOptSearch, "
                    "pass a valid `space` parameter."
                )

            self._skopt_opt = sko.Optimizer(self._parameter_ranges)

        if self._points_to_evaluate and self._evaluated_rewards:
            skopt_points = [
                [point[par] for par in self._parameter_names]
                for point in self._points_to_evaluate
            ]
            self._skopt_opt.tell(skopt_points, self._evaluated_rewards)
        elif self._points_to_evaluate:
            self._initial_points = self._points_to_evaluate
        self._parameters = self._parameter_names

        # Skopt internally minimizes, so "max" => -1
        if self._mode == "max":
            self._metric_op = -1.0
        elif self._mode == "min":
            self._metric_op = 1.0

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

    def add_evaluated_point(
        self,
        parameters: Dict,
        value: float,
        error: bool = False,
        pruned: bool = False,
        intermediate_values: Optional[List[float]] = None,
    ):
        assert self._skopt_opt, "Optimizer must be set."
        if intermediate_values:
            logger.warning("SkOpt doesn't use intermediate_values. Ignoring.")
        if not error and not pruned:
            self._skopt_opt.tell(
                [parameters[par] for par in self._parameter_names], value
            )

        else:
            logger.warning(
                "Only non errored and non pruned points" " can be added to SkOpt."
            )

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self._skopt_opt:
            return False
        space = self.convert_search_space(config)

        self._space = space
        self._parameter_names = list(space.keys())
        self._parameter_ranges = list(space.values())

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._setup_skopt()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._skopt_opt:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="space"
                )
            )
        if not self._metric or not self._mode:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__, metric=self._metric, mode=self._mode
                )
            )

        if self._initial_points:
            suggested_config = self._initial_points.pop(0)
            skopt_config = [suggested_config[par] for par in self._parameters]
        else:
            skopt_config = self._skopt_opt.ask()
            suggested_config = dict(zip(self._parameters, skopt_config))
        self._live_trial_mapping[trial_id] = skopt_config

        if self._convert_to_python:
            for k, v in list(suggested_config.items()):
                if isinstance(v, np.number):
                    suggested_config[k] = v.item()

        return unflatten_dict(suggested_config)

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        """Notification for the completion of trial.

        The result is internally negated when interacting with Skopt
        so that Skopt Optimizers can "maximize" this value,
        as it minimizes on default.
        """

        if result:
            self._process_result(trial_id, result)
        self._live_trial_mapping.pop(trial_id)

    def _process_result(self, trial_id: str, result: Dict):
        skopt_trial_info = self._live_trial_mapping[trial_id]
        if result and not is_nan_or_inf(result[self._metric]):
            self._skopt_opt.tell(
                skopt_trial_info, self._metric_op * result[self._metric]
            )

    def get_state(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        return state

    def set_state(self, state: Dict[str, Any]):
        self.__dict__.update(state)

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        if not isinstance(save_object, dict):
            # backwards compatibility
            # Deprecate: 1.8
            self._initial_points, self._skopt_opt = save_object
        self.__dict__.update(save_object)

    @staticmethod
    def convert_search_space(spec: Dict, join: bool = False) -> Dict:
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a SkOpt search space."
            )

        # Flatten and resolve again after checking for grid search.
        spec = flatten_dict(spec, prevent_delimiter=True)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        def resolve_value(domain: Domain) -> Union[Tuple, List]:
            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                logger.warning(
                    "SkOpt search does not support quantization. "
                    "Dropped quantization."
                )
                sampler = sampler.get_sampler()

            if isinstance(domain, Float):
                if isinstance(domain.sampler, LogUniform):
                    return sko.space.Real(
                        domain.lower, domain.upper, prior="log-uniform"
                    )
                return sko.space.Real(domain.lower, domain.upper, prior="uniform")

            elif isinstance(domain, Integer):
                if isinstance(domain.sampler, LogUniform):
                    return sko.space.Integer(
                        domain.lower, domain.upper - 1, prior="log-uniform"
                    )
                return sko.space.Integer(
                    domain.lower, domain.upper - 1, prior="uniform"
                )

            elif isinstance(domain, Categorical):
                return sko.space.Categorical(domain.categories)

            raise ValueError(
                "SkOpt does not support parameters of type "
                "`{}` with samplers of type `{}`".format(
                    type(domain).__name__, type(domain.sampler).__name__
                )
            )

        # Parameter name is e.g. "a/b/c" for nested dicts
        space = {"/".join(path): resolve_value(domain) for path, domain in domain_vars}

        if join:
            spec.update(space)
            space = spec

        return space
