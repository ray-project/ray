from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import logging
import pickle
try:  # Python 3 only -- needed for lint test.
    import bayes_opt as byo
except ImportError:
    byo = None

from ray.tune.suggest.suggestion import SuggestionAlgorithm

logger = logging.getLogger(__name__)


class BayesOptSearch(SuggestionAlgorithm):
    """A wrapper around BayesOpt to provide trial suggestions.

    Requires BayesOpt to be installed. You can install BayesOpt with the
    command: `pip install bayesian-optimization`.

    Parameters:
        space (dict): Continuous search space. Parameters will be sampled from
            this space which will be used to run trials.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        utility_kwargs (dict): Parameters to define the utility function. Must
            provide values for the keys `kind`, `kappa`, and `xi`.
        random_state (int): Used to initialize BayesOpt.
        verbose (int): Sets verbosity level for BayesOpt packages.
        use_early_stopped_trials (bool): Whether to use early terminated
            trial results in the optimization process.

    Example:
        >>> space = {
        >>>     'width': (0, 20),
        >>>     'height': (-100, 100),
        >>> }
        >>> algo = BayesOptSearch(
        >>>     space, max_concurrent=4, metric="mean_loss", mode="min")
    """

    def __init__(self,
                 space,
                 max_concurrent=10,
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 utility_kwargs=None,
                 random_state=1,
                 verbose=0,
                 **kwargs):
        assert byo is not None, (
            "BayesOpt must be installed!. You can install BayesOpt with"
            " the command: `pip install bayesian-optimization`.")
        assert type(max_concurrent) is int and max_concurrent > 0
        assert utility_kwargs is not None, (
            "Must define arguments for the utiliy function!")
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        if reward_attr is not None:
            mode = "max"
            metric = reward_attr
            logger.warning(
                "`reward_attr` is deprecated and will be removed in a future "
                "version of Tune. "
                "Setting `metric={}` and `mode=max`.".format(reward_attr))

        self._max_concurrent = max_concurrent
        self._metric = metric
        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.
        self._live_trial_mapping = {}

        self.optimizer = byo.BayesianOptimization(
            f=None, pbounds=space, verbose=verbose, random_state=random_state)

        self.utility = byo.UtilityFunction(**utility_kwargs)

        super(BayesOptSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None

        new_trial = self.optimizer.suggest(self.utility)

        self._live_trial_mapping[trial_id] = new_trial

        return copy.deepcopy(new_trial)

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Notification for the completion of trial."""
        if result:
            self._process_result(trial_id, result, early_terminated)
        del self._live_trial_mapping[trial_id]

    def _process_result(self, trial_id, result, early_terminated=False):
        if early_terminated and self._use_early_stopped is False:
            return
        self.optimizer.register(
            params=self._live_trial_mapping[trial_id],
            target=self._metric_op * result[self._metric])

    def _num_live_trials(self):
        return len(self._live_trial_mapping)

    def save(self, checkpoint_dir):
        trials_object = self.optimizer
        with open(checkpoint_dir, "wb") as output:
            pickle.dump(trials_object, output)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as input:
            trials_object = pickle.load(input)
        self.optimizer = trials_object
