import copy
import logging
import pickle
try:  # Python 3 only -- needed for lint test.
    import bayes_opt as byo
except ImportError:
    byo = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


def _validate_warmstart(space, points_to_evaluate, evaluated_rewards):
    if points_to_evaluate:
        if not isinstance(points_to_evaluate, list):
            raise TypeError(
                "points_to_evaluate expected to be a list, got {}.".format(
                    type(points_to_evaluate)))
        for point in points_to_evaluate:
            if not isinstance(point, dict):
                raise TypeError(
                    "points_to_evaluate expected to include dict, got {}.".
                    format(point))

            if not set(point) == set(space):
                raise ValueError("Keys of point {}".format(list(
                    point)) + " and parameter_names {}".format(list(space)) +
                                 " do not match.")

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


class BayesOptSearch(Searcher):
    """A wrapper around BayesOpt to provide trial suggestions.

    Requires BayesOpt to be installed. You can install BayesOpt with the
    command: ``pip install bayesian-optimization``.

    Parameters:
        space (dict): Continuous search space. Parameters will be sampled from
            this space which will be used to run trials.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        utility_kwargs (dict): Parameters to define the utility function. Keys
            `kind`, `kappa`, and `xi`.
        points_to_evaluate (List[Dict]): A list of points you'd like to run
            first before sampling from the optimizer, e.g. these could be
            parameter configurations you already know work well to help
            the optimiser select good values. Each point is a list of the
            parameters using the order definition given by parameter_names.
        evaluated_rewards (List): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate.
        random_state (int): Used to initialize BayesOpt.
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
    DEFAULT_UTILITY_KWARGS = {
        "kind": "ucb",
        "kappa": 2.576,
        "xi": 0.0,
    }

    def __init__(self,
                 space,
                 metric="episode_reward_mean",
                 mode="max",
                 utility_kwargs=None,
                 random_state=1,
                 points_to_evaluate=None,
                 evaluated_rewards=None,
                 verbose=0,
                 max_concurrent=None,
                 use_early_stopped_trials=None):
        assert byo is not None, (
            "BayesOpt must be installed!. You can install BayesOpt with"
            " the command: `pip install bayesian-optimization`.")
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        super(BayesOptSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)

        _validate_warmstart(space, points_to_evaluate, evaluated_rewards)

        bayesopt_utility_kwargs = self.DEFAULT_UTILITY_KWARGS.copy()
        if isinstance(utility_kwargs, dict):
            bayesopt_utility_kwargs.update(utility_kwargs)

        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.
        self._live_trial_mapping = {}
        self.optimizer = byo.BayesianOptimization(
            f=None, pbounds=space, verbose=verbose, random_state=random_state)

        self.utility = byo.UtilityFunction(**bayesopt_utility_kwargs)

        if points_to_evaluate and evaluated_rewards:
            for point, target in zip(points_to_evaluate, evaluated_rewards):
                self.optimizer.register(params=point, target=target)
        elif points_to_evaluate:
            self._queue = points_to_evaluate

    def suggest(self, trial_id):
        if self._queue:
            suggested_config = self._queue[0]
            del self._queue[0]
        else:
            suggested_config = self.optimizer.suggest(self.utility)

        self._live_trial_mapping[trial_id] = suggested_config

        return copy.deepcopy(suggested_config)

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial."""
        if trial_id in self._live_trial_mapping:
            if result:
                self._process_result(trial_id, result)
            del self._live_trial_mapping[trial_id]

    def _process_result(self, trial_id, result):
        self.optimizer.register(
            params=self._live_trial_mapping[trial_id],
            target=self._metric_op * result[self.metric])

    def save(self, checkpoint_dir):
        trials_object = (self.optimizer, self._queue)
        with open(checkpoint_dir, "wb") as output:
            pickle.dump(trials_object, output)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as state:
            trials_object = pickle.load(state)
        self.optimizer, self._queue = trials_object
