import copy
from collections import defaultdict
import logging
import pickle
import json
try:  # Python 3 only -- needed for lint test.
    import bayes_opt as byo
except ImportError:
    byo = None

from ray.tune.suggest import Searcher
from ray.tune.utils import flatten_dict

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

    You will need to install fmfn/BayesianOptimization via the following:

    .. code-block:: bash

        pip install bayesian-optimization

    This algorithm requires setting a search space using the
    `BayesianOptimization search space specification`_.

    Args:
        space (dict): Continuous search space. Parameters will be sampled from
            this space which will be used to run trials.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        utility_kwargs (dict): Parameters to define the utility function.
            The default value is a dictionary with three keys:
            - kind: ucb (Upper Confidence Bound)
            - kappa: 2.576
            - xi: 0.0
        random_state (int): Used to initialize BayesOpt.
        random_search_steps (int): Number of initial random searches.
            This is necessary to avoid initial local overfitting
            of the Bayesian process.
        analysis (ExperimentAnalysis): Optionally, the previous analysis
            to integrate.
        verbose (int): Sets verbosity level for BayesOpt packages.
        max_concurrent: Deprecated.
        use_early_stopped_trials: Deprecated.

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.bayesopt import BayesOptSearch

        space = {
            'width': (0, 20),
            'height': (-100, 100),
        }
        algo = BayesOptSearch(space, metric="mean_loss", mode="min")
        tune.run(my_func, search_alg=algo)
    """
    # bayes_opt.BayesianOptimization: Optimization object
    optimizer = None

    def __init__(self,
                 space,
                 metric,
                 mode="max",
                 utility_kwargs=None,
                 random_state=42,
                 random_search_steps=10,
                 verbose=0,
                 patience=5,
                 skip_duplicate=True,
                 analysis=None,
                 max_concurrent=None,
                 use_early_stopped_trials=None):
        """Instantiate new BayesOptSearch object.

        Args:
            space (dict): Continuous search space.
                Parameters will be sampled from
                this space which will be used to run trials.
            metric (str): The training result objective value attribute.
            mode (str): One of {min, max}. Determines whether objective is
                minimizing or maximizing the metric attribute.
            utility_kwargs (dict): Parameters to define the utility function.
                Must provide values for the keys `kind`, `kappa`, and `xi`.
            random_state (int): Used to initialize BayesOpt.
            random_search_steps (int): Number of initial random searches.
                This is necessary to avoid initial local overfitting
                of the Bayesian process.
            patience (int): Must be > 0. If the optimizer suggests a set of
                hyperparameters more than 'patience' times,
                then the whole experiment will stop.
            skip_duplicate (bool): If true, BayesOptSearch will not create
                a trial with a previously seen set of hyperparameters. By
                default, floating values will be reduced to a digit precision
                of 5. You can override this by setting
                ``searcher.repeat_float_precision``.
            analysis (ExperimentAnalysis): Optionally, the previous analysis
                to integrate.
            verbose (int): Sets verbosity level for BayesOpt packages.
            max_concurrent: Deprecated.
            use_early_stopped_trials: Deprecated.
        """
        assert byo is not None, (
            "BayesOpt must be installed!. You can install BayesOpt with"
            " the command: `pip install bayesian-optimization`.")
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self.max_concurrent = max_concurrent
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
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)

        if utility_kwargs is None:
            # The defaults arguments are the same
            # as in the package BayesianOptimization
            utility_kwargs = dict(
                kind="ucb",
                kappa=2.576,
                xi=0.0,
            )

        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.

        self._live_trial_mapping = {}
        self._buffered_trial_results = []
        self.random_search_trials = random_search_steps
        self._total_random_search_trials = 0

        self.optimizer = byo.BayesianOptimization(
            f=None, pbounds=space, verbose=verbose, random_state=random_state)

        self.utility = byo.UtilityFunction(**utility_kwargs)

        # Registering the provided analysis, if given
        if analysis is not None:
            self.register_analysis(analysis)

    def suggest(self, trial_id):
        """Return new point to be explored by black box function.

        Args:
            trial_id (str): Id of the trial.
                This is a short alphanumerical string.

        Returns:
            Either a dictionary describing the new point to explore or
            None, when no new point is to be explored for the time being.
        """
        # If we have more active trials than the allowed maximum
        total_live_trials = len(self._live_trial_mapping)
        if self.max_concurrent and self.max_concurrent <= total_live_trials:
            # we stop the suggestion and return None.
            return None

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
        return copy.deepcopy(config)

    def register_analysis(self, analysis):
        """Integrate the given analysis into the gaussian process.

        Args:
            analysis (ExperimentAnalysis): Optionally, the previous analysis
                to integrate.
        """
        for (_, report), params in zip(analysis.dataframe().iterrows(),
                                       analysis.get_all_configs().values()):
            # We add the obtained results to the
            # gaussian process optimizer
            self._register_result(params, report)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        Args:
            trial_id (str): Id of the trial.
                This is a short alphanumerical string.
            result (dict): Dictionary of result.
                May be none when some error occurs.
            error (bool): Boolean representing a previous error state.
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

    def _register_result(self, params, result):
        """Register given tuple of params and results."""
        self.optimizer.register(params, self._metric_op * result[self.metric])

    def save(self, checkpoint_dir):
        """Storing current optimizer state."""
        with open(checkpoint_dir, "wb") as f:
            pickle.dump(
                (self.optimizer, self._buffered_trial_results,
                 self._total_random_search_trials, self._config_counter), f)

    def restore(self, checkpoint_dir):
        """Restoring current optimizer state."""
        with open(checkpoint_dir, "rb") as f:
            (self.optimizer, self._buffered_trial_results,
             self._total_random_search_trials,
             self._config_counter) = pickle.load(f)
