import logging
import pickle
from typing import Dict, List, Optional, Tuple, Union

from ray.tune.result import DEFAULT_METRIC
from ray.tune.sample import Categorical, Domain, Float, Integer, Quantized
from ray.tune.suggest.suggestion import UNRESOLVED_SEARCH_SPACE, \
    UNDEFINED_METRIC_MODE, UNDEFINED_SEARCH_SPACE
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils import flatten_dict
from ray.tune.utils.util import is_nan_or_inf, unflatten_dict

try:
    import skopt as sko
except ImportError:
    sko = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


def _validate_warmstart(parameter_names: List[str],
                        points_to_evaluate: List[List],
                        evaluated_rewards: List):
    if points_to_evaluate:
        if not isinstance(points_to_evaluate, list):
            raise TypeError(
                "points_to_evaluate expected to be a list, got {}.".format(
                    type(points_to_evaluate)))
        for point in points_to_evaluate:
            if not isinstance(point, list):
                raise TypeError(
                    "points_to_evaluate expected to include list, got {}.".
                    format(point))

            if not len(point) == len(parameter_names):
                raise ValueError("Dim of point {}".format(point) +
                                 " and parameter_names {}".format(
                                     parameter_names) + " do not match.")

    if points_to_evaluate and evaluated_rewards:
        if not isinstance(evaluated_rewards, list):
            raise TypeError(
                "evaluated_rewards expected to be a list, got {}.".format(
                    type(evaluated_rewards)))
        if not len(evaluated_rewards) == len(points_to_evaluate):
            raise ValueError(
                "Dim of evaluated_rewards {}".format(evaluated_rewards) +
                " and points_to_evaluate {}".format(points_to_evaluate) +
                " do not match.")


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
        points_to_evaluate (list of lists): A list of points you'd like to run
            first before sampling from the optimiser, e.g. these could be
            parameter configurations you already know work well to help
            the optimiser select good values. Each point is a list of the
            parameters using the order definition given by parameter_names.
        evaluated_rewards (list): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate. (See tune/examples/skopt_example.py)
        max_concurrent: Deprecated.
        use_early_stopped_trials: Deprecated.

    Tune automatically converts search spaces to SkOpt's format:

    .. code-block:: python

        config = {
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100)
        }

        current_best_params = [[10, 0], [15, -20]]

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

    def __init__(self,
                 optimizer: Optional[sko.optimizer.Optimizer] = None,
                 space: Union[List[str], Dict[str, Union[Tuple, List]]] = None,
                 metric: Optional[str] = None,
                 mode: Optional[str] = None,
                 points_to_evaluate: Optional[List[List]] = None,
                 evaluated_rewards: Optional[List] = None,
                 max_concurrent: Optional[int] = None,
                 use_early_stopped_trials: Optional[bool] = None):
        assert sko is not None, """skopt must be installed!
            You can install Skopt with the command:
            `pip install scikit-optimize`."""

        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        self.max_concurrent = max_concurrent
        super(SkOptSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)

        self._initial_points = []
        self._parameters = None
        self._parameter_names = None
        self._parameter_ranges = None

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(
                        par="space", cls=type(self)))
                space = self.convert_search_space(space, join=True)

        self._space = space

        if self._space:
            if isinstance(optimizer, sko.Optimizer):
                if not isinstance(space, list):
                    raise ValueError(
                        "You passed an optimizer instance to SkOpt. Your "
                        "`space` parameter should be a list of parameter"
                        "names.")
                self._parameter_names = space
            else:
                self._parameter_names = list(space.keys())
                self._parameter_ranges = space.values()

        self._points_to_evaluate = points_to_evaluate
        self._evaluated_rewards = evaluated_rewards

        self._skopt_opt = optimizer
        if self._skopt_opt or self._space:
            self._setup_skopt()

        self._live_trial_mapping = {}

    def _setup_skopt(self):
        _validate_warmstart(self._parameter_names, self._points_to_evaluate,
                            self._evaluated_rewards)

        if not self._skopt_opt:
            if not self._space:
                raise ValueError(
                    "If you don't pass an optimizer instance to SkOptSearch, "
                    "pass a valid `space` parameter.")

            self._skopt_opt = sko.Optimizer(self._parameter_ranges)

        if self._points_to_evaluate and self._evaluated_rewards:
            self._skopt_opt.tell(self._points_to_evaluate,
                                 self._evaluated_rewards)
        elif self._points_to_evaluate:
            self._initial_points = self._points_to_evaluate
        self._parameters = self._parameter_names

        # Skopt internally minimizes, so "max" => -1
        if self._mode == "max":
            self._metric_op = -1.
        elif self._mode == "min":
            self._metric_op = 1.

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

    def set_search_properties(self, metric: Optional[str], mode: Optional[str],
                              config: Dict) -> bool:
        if self._skopt_opt:
            return False
        space = self.convert_search_space(config)

        self._space = space
        self._parameter_names = space.keys()
        self._parameter_ranges = space.values()

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
        if self._initial_points:
            suggested_config = self._initial_points[0]
            del self._initial_points[0]
        else:
            suggested_config = self._skopt_opt.ask()
        self._live_trial_mapping[trial_id] = suggested_config
        return unflatten_dict(dict(zip(self._parameters, suggested_config)))

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False):
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
            self._skopt_opt.tell(skopt_trial_info,
                                 self._metric_op * result[self._metric])

    def save(self, checkpoint_path: str):
        trials_object = (self._initial_points, self._skopt_opt)
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self._initial_points = trials_object[0]
        self._skopt_opt = trials_object[1]

    @staticmethod
    def convert_search_space(spec: Dict, join: bool = False) -> Dict:
        spec = flatten_dict(spec, prevent_delimiter=True)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a SkOpt search space.")

        def resolve_value(domain: Domain) -> Union[Tuple, List]:
            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                logger.warning("SkOpt search does not support quantization. "
                               "Dropped quantization.")
                sampler = sampler.get_sampler()

            if isinstance(domain, Float):
                if domain.sampler is not None:
                    logger.warning(
                        "SkOpt does not support specific sampling methods."
                        " The {} sampler will be dropped.".format(sampler))
                return domain.lower, domain.upper

            if isinstance(domain, Integer):
                if domain.sampler is not None:
                    logger.warning(
                        "SkOpt does not support specific sampling methods."
                        " The {} sampler will be dropped.".format(sampler))
                return domain.lower, domain.upper

            if isinstance(domain, Categorical):
                return domain.categories

            raise ValueError("SkOpt does not support parameters of type "
                             "`{}`".format(type(domain).__name__))

        # Parameter name is e.g. "a/b/c" for nested dicts
        space = {
            "/".join(path): resolve_value(domain)
            for path, domain in domain_vars
        }

        if join:
            spec.update(space)
            space = spec

        return space
