import json
import logging
import pickle
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from ray.tune.result import DEFAULT_METRIC
from ray.tune.search import (
    UNDEFINED_METRIC_MODE,
    UNDEFINED_SEARCH_SPACE,
    UNRESOLVED_SEARCH_SPACE,
    Searcher,
)
from ray.tune.search.sample import Domain, Float, Quantized, Uniform
from ray.tune.search.variant_generator import parse_spec_vars
from ray.tune.utils import flatten_dict
from ray.tune.utils.util import is_nan_or_inf, unflatten_dict

try:  # Python 3 only -- needed for lint test.
    import bayes_opt as byo
except ImportError:
    byo = None


if TYPE_CHECKING:
    from ray.tune import ExperimentAnalysis

logger = logging.getLogger(__name__)


def _dict_hash(config, precision):
    flatconfig = flatten_dict(config)
    for param, value in flatconfig.items():
        if isinstance(value, float):
            flatconfig[param] = "{:.{digits}f}".format(value, digits=precision)

    hashed = json.dumps(flatconfig, sort_keys=True, default=str)
    return hashed


class BayesOptSearch(Searcher):
    """Uses fmfn/BayesianOptimization to optimize hyperparameters.

    fmfn/BayesianOptimization is a library for Bayesian Optimization. More
    info can be found here: https://github.com/fmfn/BayesianOptimization.

    This searcher will automatically filter out any NaN, inf or -inf
    results.

    You will need to install fmfn/BayesianOptimization via the following:

    .. code-block:: bash

        pip install bayesian-optimization

    Initializing this search algorithm with a ``space`` requires that it's
    in the ``BayesianOptimization`` search space format. Otherwise, you
    should instead pass in a Tune search space into ``Tuner(param_space=...)``,
    and the search space will be automatically converted for you.

    See this `BayesianOptimization example notebook
    <https://github.com/fmfn/BayesianOptimization/blob/master/examples/advanced-tour.ipynb>`_
    for an example.

    Args:
        space: Continuous search space. Parameters will be sampled from
            this space which will be used to run trials.
        metric: The training result objective value attribute. If None
            but a mode was passed, the anonymous metric `_metric` will be used
            per default.
        mode: One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate: Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want to run first to help the algorithm make better suggestions
            for future parameters. Needs to be a list of dicts containing the
            configurations.
        utility_kwargs: Parameters to define the utility function.
            The default value is a dictionary with three keys:
            - kind: ucb (Upper Confidence Bound)
            - kappa: 2.576
            - xi: 0.0
        random_state: Used to initialize BayesOpt.
        random_search_steps: Number of initial random searches.
            This is necessary to avoid initial local overfitting
            of the Bayesian process.
        verbose: Sets verbosity level for BayesOpt packages.
        patience: If patience is set and we've repeated a trial numerous times,
            we terminate the experiment.
        skip_duplicate: skip duplicate config
        analysis: Optionally, the previous analysis to integrate.

    Tune automatically converts search spaces to BayesOptSearch's format:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.bayesopt import BayesOptSearch

        config = {
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100)
        }

        bayesopt = BayesOptSearch(metric="mean_loss", mode="min")
        tuner = tune.Tuner(
            my_func,
            tune_config=tune.TuneConfig(
                search_alg=baysopt,
            ),
            param_space=config,
        )
        tuner.fit()

    If you would like to pass the search space manually, the code would
    look like this:

    .. code-block:: python

        from ray import tune
        from ray.tune.search.bayesopt import BayesOptSearch

        space = {
            'width': (0, 20),
            'height': (-100, 100),
        }
        bayesopt = BayesOptSearch(space, metric="mean_loss", mode="min")
        tuner = tune.Tuner(
            my_func,
            tune_config=tune.TuneConfig(
                search_alg=bayesopt,
            ),
        )
        tuner.fit()

    """

    # bayes_opt.BayesianOptimization: Optimization object
    optimizer = None

    def __init__(
        self,
        space: Optional[Dict] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        points_to_evaluate: Optional[List[Dict]] = None,
        utility_kwargs: Optional[Dict] = None,
        random_state: int = 42,
        random_search_steps: int = 10,
        verbose: int = 0,
        patience: int = 5,
        skip_duplicate: bool = True,
        analysis: Optional["ExperimentAnalysis"] = None,
    ):
        assert byo is not None, (
            "BayesOpt must be installed!. You can install BayesOpt with"
            " the command: `pip install bayesian-optimization`."
        )
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        self._config_counter = defaultdict(int)
        self._patience = patience
        # int: Precision at which to hash values.
        self.repeat_float_precision = 5
        if self._patience <= 0:
            raise ValueError("patience must be set to a value greater than 0!")
        self._skip_duplicate = skip_duplicate
        super(BayesOptSearch, self).__init__(
            metric=metric,
            mode=mode,
        )

        if utility_kwargs is None:
            # The defaults arguments are the same
            # as in the package BayesianOptimization
            utility_kwargs = dict(
                kind="ucb",
                kappa=2.576,
                xi=0.0,
            )

        if mode == "max":
            self._metric_op = 1.0
        elif mode == "min":
            self._metric_op = -1.0

        self._points_to_evaluate = points_to_evaluate

        self._live_trial_mapping = {}
        self._buffered_trial_results = []
        self.random_search_trials = random_search_steps
        self._total_random_search_trials = 0

        self.utility = byo.UtilityFunction(**utility_kwargs)

        self._analysis = analysis

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(par="space", cls=type(self))
                )
                space = self.convert_search_space(space, join=True)

        self._space = space
        self._verbose = verbose
        self._random_state = random_state

        self.optimizer = None
        if space:
            self._setup_optimizer()

    def _setup_optimizer(self):
        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        self.optimizer = byo.BayesianOptimization(
            f=None,
            pbounds=self._space,
            verbose=self._verbose,
            random_state=self._random_state,
        )

        # Registering the provided analysis, if given
        if self._analysis is not None:
            self.register_analysis(self._analysis)

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], config: Dict, **spec
    ) -> bool:
        if self.optimizer:
            return False
        space = self.convert_search_space(config)
        self._space = space
        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        if self._mode == "max":
            self._metric_op = 1.0
        elif self._mode == "min":
            self._metric_op = -1.0

        self._setup_optimizer()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        """Return new point to be explored by black box function.

        Args:
            trial_id: Id of the trial.
                This is a short alphanumerical string.

        Returns:
            Either a dictionary describing the new point to explore or
            None, when no new point is to be explored for the time being.
        """
        if not self.optimizer:
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

        if self._points_to_evaluate:
            config = self._points_to_evaluate.pop(0)
        else:
            # We compute the new point to explore
            config = self.optimizer.suggest(self.utility)

        config_hash = _dict_hash(config, self.repeat_float_precision)
        # Check if already computed
        already_seen = config_hash in self._config_counter
        self._config_counter[config_hash] += 1
        top_repeats = max(self._config_counter.values())

        # If patience is set and we've repeated a trial numerous times,
        # we terminate the experiment.
        if self._patience is not None and top_repeats > self._patience:
            return Searcher.FINISHED
        # If we have seen a value before, we'll skip it.
        if already_seen and self._skip_duplicate:
            logger.info("Skipping duplicated config: {}.".format(config))
            return None

        # If we are still in the random search part and we are waiting for
        # trials to complete
        if len(self._buffered_trial_results) < self.random_search_trials:
            # We check if we have already maxed out the number of requested
            # random search trials
            if self._total_random_search_trials == self.random_search_trials:
                # If so we stop the suggestion and return None
                return None
            # Otherwise we increase the total number of rndom search trials
            if config:
                self._total_random_search_trials += 1

        # Save the new trial to the trial mapping
        self._live_trial_mapping[trial_id] = config

        # Return a deep copy of the mapping
        return unflatten_dict(config)

    def register_analysis(self, analysis: "ExperimentAnalysis"):
        """Integrate the given analysis into the gaussian process.

        Args:
            analysis: Optionally, the previous analysis
                to integrate.
        """
        for (_, report), params in zip(
            analysis.dataframe(metric=self._metric, mode=self._mode).iterrows(),
            analysis.get_all_configs().values(),
        ):
            # We add the obtained results to the
            # gaussian process optimizer
            self._register_result(params, report)

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        """Notification for the completion of trial.

        Args:
            trial_id: Id of the trial.
                This is a short alphanumerical string.
            result: Dictionary of result.
                May be none when some error occurs.
            error: Boolean representing a previous error state.
                The result should be None when error is True.
        """
        # We try to get the parameters used for this trial
        params = self._live_trial_mapping.pop(trial_id, None)

        # The results may be None if some exception is raised during the trial.
        # Also, if the parameters are None (were already processed)
        # we interrupt the following procedure.
        # Additionally, if somehow the error is True but
        # the remaining values are not we also block the method
        if result is None or params is None or error:
            return

        # If we don't have to execute some random search steps
        if len(self._buffered_trial_results) >= self.random_search_trials:
            #  we simply register the obtained result
            self._register_result(params, result)
            return

        # We store the results into a temporary cache
        self._buffered_trial_results.append((params, result))

        # If the random search finished,
        # we update the BO with all the computer points.
        if len(self._buffered_trial_results) == self.random_search_trials:
            for params, result in self._buffered_trial_results:
                self._register_result(params, result)

    def _register_result(self, params: Tuple[str], result: Dict):
        """Register given tuple of params and results."""
        if is_nan_or_inf(result[self.metric]):
            return
        self.optimizer.register(params, self._metric_op * result[self.metric])

    def get_state(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        return state

    def set_state(self, state: Dict[str, Any]):
        self.__dict__.update(state)

    def save(self, checkpoint_path: str):
        """Storing current optimizer state."""
        save_object = self.get_state()
        with open(checkpoint_path, "wb") as f:
            pickle.dump(save_object, f)

    def restore(self, checkpoint_path: str):
        """Restoring current optimizer state."""
        with open(checkpoint_path, "rb") as f:
            save_object = pickle.load(f)

        if isinstance(save_object, dict):
            self.set_state(save_object)
        else:
            # Backwards compatibility
            (
                self.optimizer,
                self._buffered_trial_results,
                self._total_random_search_trials,
                self._config_counter,
                self._points_to_evaluate,
            ) = save_object

    @staticmethod
    def convert_search_space(spec: Dict, join: bool = False) -> Dict:
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a BayesOpt search space."
            )

        # Flatten and resolve again after checking for grid search.
        spec = flatten_dict(spec, prevent_delimiter=True)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        def resolve_value(domain: Domain) -> Tuple[float, float]:
            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                logger.warning(
                    "BayesOpt search does not support quantization. "
                    "Dropped quantization."
                )
                sampler = sampler.get_sampler()

            if isinstance(domain, Float):
                if domain.sampler is not None and not isinstance(
                    domain.sampler, Uniform
                ):
                    logger.warning(
                        "BayesOpt does not support specific sampling methods. "
                        "The {} sampler will be dropped.".format(sampler)
                    )
                return (domain.lower, domain.upper)

            raise ValueError(
                "BayesOpt does not support parameters of type "
                "`{}`".format(type(domain).__name__)
            )

        # Parameter name is e.g. "a/b/c" for nested dicts
        bounds = {"/".join(path): resolve_value(domain) for path, domain in domain_vars}

        if join:
            spec.update(bounds)
            bounds = spec

        return bounds
