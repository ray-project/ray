import copy
import logging
import pickle
try:  # Python 3 only -- needed for lint test.
    import bayes_opt as byo
except ImportError:
    byo = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class BayesOptSearch(Searcher):
    """Uses fmfn/BayesianOptimization to optimize hyperparameters.

    fmfn/BayesianOptimization is a library for Bayesian Optimization. More
    info can be found here: https://github.com/fmfn/BayesianOptimization.

    You will need to install fmfn/BayesianOptimization via the following:

    .. code-block:: bash

        pip install bayesian-optimization

    This algorithm requires setting a search space using the
    `BayesianOptimization search space specification`_.

    Parameters:
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
        tune.run(my_func, algo=algo)
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
                 analysis=None,
                 max_concurrent=None,
                 use_early_stopped_trials=None):
        """Instantiate new BayesOptSearch object.

        Parameters:
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
        self._cached_results = []
        self._random_search_steps = random_search_steps
        self._random_search_trials = 0

        self.optimizer = byo.BayesianOptimization(
            f=None, pbounds=space, verbose=verbose, random_state=random_state)

        self.utility = byo.UtilityFunction(**utility_kwargs)

        # Registering the provided analysis, if given
        if analysis is not None:
            self.register_analysis(analysis)

    def suggest(self, trial_id):
        if self.max_concurrent:
            if len(self._live_trial_mapping) >= self.max_concurrent:
                return None
        if self._random_search_steps > 0 and len(self._cached_results) < self._random_search_steps:
            if self._random_search_trials == self._random_search_steps:
                return None
            self._random_search_trials += 1

        new_trial = self.optimizer.suggest(self.utility)
        self._live_trial_mapping[trial_id] = new_trial

        return copy.deepcopy(new_trial)

    def register_analysis(self, analysis):
        """Integrate the given analysis into the gaussian process.

        Parameters
        ------------------
        analysis (ExperimentAnalysis): Optionally, the previous analysis
            to integrate.
        """
        for (_, report), params in zip(analysis.dataframe().iterrows(),
                                       analysis.get_all_configs().values()):
            # We add the obtained results to the
            # gaussian process optimizer
            self._register_result(params, report)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial."""
        # The results may be None when some exception
        # is raised during the trial.
        if result is not None:
            # We retrieve the stored parameters
            params = self._live_trial_mapping[trial_id]
            # If we still have to execute some random
            # search steps
            if self._random_search_steps > 0 and len(self._cached_results) < self._random_search_steps:
                # We store the results into a temporary cache
                self._cached_results.append((params, result))
                # And if we hit zero, we update the BO
                # with all the computer points.
                if len(self._cached_results) == self._random_search_steps:
                    # And for each tuple we register the result
                    for params, result in self._cached_results:
                        self._register_result(params, result)
            else:
                # Otherwise we simply register the obtained result
                self._register_result(params, result)
        # Finally, we delete the computed trial from the dictionary.
        del self._live_trial_mapping[trial_id]

    def _register_result(self, params, result):
        """Register given tuple of params and results."""
        self.optimizer.register(params, self._metric_op * result[self.metric])

    def save(self, checkpoint_dir):
        """Storing current optimizer state."""
        with open(checkpoint_dir, "wb") as f:
            pickle.dump(self.optimizer, f)

    def restore(self, checkpoint_dir):
        """Restoring current optimizer state."""
        with open(checkpoint_dir, "rb") as f:
            self.optimizer = pickle.load(f)
