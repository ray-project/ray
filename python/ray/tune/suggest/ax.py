import copy
from typing import Dict, List, Optional, Union

from ax.service.ax_client import AxClient
from ray.tune.result import DEFAULT_METRIC
from ray.tune.sample import Categorical, Float, Integer, LogUniform, \
    Quantized, Uniform
from ray.tune.suggest.suggestion import UNRESOLVED_SEARCH_SPACE, \
    UNDEFINED_METRIC_MODE, UNDEFINED_SEARCH_SPACE
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils.util import flatten_dict, unflatten_dict

try:
    import ax
except ImportError:
    ax = None
import logging

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class AxSearch(Searcher):
    """Uses `Ax <https://ax.dev/>`_ to optimize hyperparameters.

    Ax is a platform for understanding, managing, deploying, and
    automating adaptive experiments. Ax provides an easy to use
    interface with BoTorch, a flexible, modern library for Bayesian
    optimization in PyTorch. More information can be found in https://ax.dev/.

    To use this search algorithm, you must install Ax and sqlalchemy:

    .. code-block:: bash

        $ pip install ax-platform sqlalchemy

    Parameters:
        space (list[dict]): Parameters in the experiment search space.
            Required elements in the dictionaries are: "name" (name of
            this parameter, string), "type" (type of the parameter: "range",
            "fixed", or "choice", string), "bounds" for range parameters
            (list of two values, lower bound first), "values" for choice
            parameters (list of values), and "value" for fixed parameters
            (single value).
        metric (str): Name of the metric used as objective in this
            experiment. This metric must be present in `raw_data` argument
            to `log_data`. This metric must also be present in the dict
            reported/returned by the Trainable. If None but a mode was passed,
            the `ray.tune.result.DEFAULT_METRIC` will be used per default.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute. Defaults to "max".
        points_to_evaluate (list): Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want to run first to help the algorithm make better suggestions
            for future parameters. Needs to be a list of dicts containing the
            configurations.
        parameter_constraints (list[str]): Parameter constraints, such as
            "x3 >= x4" or "x3 + x4 >= 2".
        outcome_constraints (list[str]): Outcome constraints of form
            "metric_name >= bound", like "m1 <= 3."
        ax_client (AxClient): Optional AxClient instance. If this is set, do
            not pass any values to these parameters: `space`, `metric`,
            `parameter_constraints`, `outcome_constraints`.
        use_early_stopped_trials: Deprecated.
        max_concurrent (int): Deprecated.

    Tune automatically converts search spaces to Ax's format:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.ax import AxSearch

        config = {
            "x1": tune.uniform(0.0, 1.0),
            "x2": tune.uniform(0.0, 1.0)
        }

        def easy_objective(config):
            for i in range(100):
                intermediate_result = config["x1"] + config["x2"] * i
                tune.report(score=intermediate_result)

        ax_search = AxSearch(metric="score")
        tune.run(
            config=config,
            easy_objective,
            search_alg=ax_search)

    If you would like to pass the search space manually, the code would
    look like this:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.ax import AxSearch

        parameters = [
            {"name": "x1", "type": "range", "bounds": [0.0, 1.0]},
            {"name": "x2", "type": "range", "bounds": [0.0, 1.0]},
        ]

        def easy_objective(config):
            for i in range(100):
                intermediate_result = config["x1"] + config["x2"] * i
                tune.report(score=intermediate_result)

        ax_search = AxSearch(space=parameters, metric="score")
        tune.run(easy_objective, search_alg=ax_search)

    """

    def __init__(self,
                 space: Optional[Union[Dict, List[Dict]]] = None,
                 metric: Optional[str] = None,
                 mode: Optional[str] = None,
                 points_to_evaluate: Optional[List[Dict]] = None,
                 parameter_constraints: Optional[List] = None,
                 outcome_constraints: Optional[List] = None,
                 ax_client: Optional[AxClient] = None,
                 use_early_stopped_trials: Optional[bool] = None,
                 max_concurrent: Optional[int] = None):
        assert ax is not None, """Ax must be installed!
            You can install AxSearch with the command:
            `pip install ax-platform sqlalchemy`."""
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."

        super(AxSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)

        self._ax = ax_client

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(
                        par="space", cls=type(self)))
                space = self.convert_search_space(space)

        self._space = space
        self._parameter_constraints = parameter_constraints
        self._outcome_constraints = outcome_constraints

        self._points_to_evaluate = copy.deepcopy(points_to_evaluate)

        self.max_concurrent = max_concurrent

        self._objective_name = metric
        self._parameters = []
        self._live_trial_mapping = {}

        if self._ax or self._space:
            self._setup_experiment()

    def _setup_experiment(self):
        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        if not self._ax:
            self._ax = AxClient()

        try:
            exp = self._ax.experiment
            has_experiment = True
        except ValueError:
            has_experiment = False

        if not has_experiment:
            if not self._space:
                raise ValueError(
                    "You have to create an Ax experiment by calling "
                    "`AxClient.create_experiment()`, or you should pass an "
                    "Ax search space as the `space` parameter to `AxSearch`, "
                    "or pass a `config` dict to `tune.run()`.")
            self._ax.create_experiment(
                parameters=self._space,
                objective_name=self._metric,
                parameter_constraints=self._parameter_constraints,
                outcome_constraints=self._outcome_constraints,
                minimize=self._mode != "max")
        else:
            if any([
                    self._space, self._parameter_constraints,
                    self._outcome_constraints
            ]):
                raise ValueError(
                    "If you create the Ax experiment yourself, do not pass "
                    "values for these parameters to `AxSearch`: {}.".format([
                        "space", "parameter_constraints", "outcome_constraints"
                    ]))

        exp = self._ax.experiment
        self._objective_name = exp.optimization_config.objective.metric.name
        self._parameters = list(exp.parameters)

        if self._ax._enforce_sequential_optimization:
            logger.warning("Detected sequential enforcement. Be sure to use "
                           "a ConcurrencyLimiter.")

    def set_search_properties(self, metric: Optional[str], mode: Optional[str],
                              config: Dict):
        if self._ax:
            return False
        space = self.convert_search_space(config)
        self._space = space
        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._setup_experiment()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._ax:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="space"))

        if not self._metric or not self._mode:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__,
                    metric=self._metric,
                    mode=self._mode))

        if self.max_concurrent:
            if len(self._live_trial_mapping) >= self.max_concurrent:
                return None

        if self._points_to_evaluate:
            config = self._points_to_evaluate.pop(0)
            parameters, trial_index = self._ax.attach_trial(config)
        else:
            parameters, trial_index = self._ax.get_next_trial()

        self._live_trial_mapping[trial_id] = trial_index
        return unflatten_dict(parameters)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        Data of form key value dictionary of metric names and values.
        """
        if result:
            self._process_result(trial_id, result)
        self._live_trial_mapping.pop(trial_id)

    def _process_result(self, trial_id, result):
        ax_trial_index = self._live_trial_mapping[trial_id]
        metric_dict = {
            self._objective_name: (result[self._objective_name], 0.0)
        }
        outcome_names = [
            oc.metric.name for oc in
            self._ax.experiment.optimization_config.outcome_constraints
        ]
        metric_dict.update({on: (result[on], 0.0) for on in outcome_names})
        self._ax.complete_trial(
            trial_index=ax_trial_index, raw_data=metric_dict)

    @staticmethod
    def convert_search_space(spec: Dict):
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to an Ax search space.")

        # Flatten and resolve again after checking for grid search.
        spec = flatten_dict(spec, prevent_delimiter=True)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        def resolve_value(par, domain):
            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                logger.warning("AxSearch does not support quantization. "
                               "Dropped quantization.")
                sampler = sampler.sampler

            if isinstance(domain, Float):
                if isinstance(sampler, LogUniform):
                    return {
                        "name": par,
                        "type": "range",
                        "bounds": [domain.lower, domain.upper],
                        "value_type": "float",
                        "log_scale": True
                    }
                elif isinstance(sampler, Uniform):
                    return {
                        "name": par,
                        "type": "range",
                        "bounds": [domain.lower, domain.upper],
                        "value_type": "float",
                        "log_scale": False
                    }
            elif isinstance(domain, Integer):
                if isinstance(sampler, LogUniform):
                    return {
                        "name": par,
                        "type": "range",
                        "bounds": [domain.lower, domain.upper],
                        "value_type": "int",
                        "log_scale": True
                    }
                elif isinstance(sampler, Uniform):
                    return {
                        "name": par,
                        "type": "range",
                        "bounds": [domain.lower, domain.upper],
                        "value_type": "int",
                        "log_scale": False
                    }
            elif isinstance(domain, Categorical):
                if isinstance(sampler, Uniform):
                    return {
                        "name": par,
                        "type": "choice",
                        "values": domain.categories
                    }

            raise ValueError("AxSearch does not support parameters of type "
                             "`{}` with samplers of type `{}`".format(
                                 type(domain).__name__,
                                 type(domain.sampler).__name__))

        # Fixed vars
        fixed_values = [{
            "name": "/".join(path),
            "type": "fixed",
            "value": val
        } for path, val in resolved_vars]

        # Parameter name is e.g. "a/b/c" for nested dicts
        resolved_values = [
            resolve_value("/".join(path), domain)
            for path, domain in domain_vars
        ]

        return fixed_values + resolved_values
