from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy

from ray.tune.suggest.suggestion import SuggestionAlgorithm

byo = None


def _import_bayesopt():
    global byo
    import bayes_opt
    byo = bayes_opt


class BayesOptSearch(SuggestionAlgorithm):
    """A wrapper around BayesOpt to provide trial suggestions.

    Requires BayesOpt to be installed. You can install BayesOpt with the
    command: `pip install bayesian-optimization`.

    Parameters:
        space (dict): Continuous search space. Parameters will be sampled from
            this space which will be used to run trials.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.
        utility_kwargs (dict): Parameters to define the utility function. Must
            provide values for the keys `kind`, `kappa`, and `xi`.
        random_state (int): Used to initialize BayesOpt.
        verbose (int): Sets verbosity level for BayesOpt packages.

    Example:
        >>> space = {
        >>>     'width': (0, 20),
        >>>     'height': (-100, 100),
        >>> }
        >>> algo = BayesOptSearch(
        >>>     space, max_concurrent=4, reward_attr="neg_mean_loss")
    """

    def __init__(self,
                 space,
                 max_concurrent=10,
                 reward_attr="episode_reward_mean",
                 utility_kwargs=None,
                 random_state=1,
                 verbose=0,
                 **kwargs):
        _import_bayesopt()
        assert byo is not None, (
            "BayesOpt must be installed!. You can install BayesOpt with"
            " the command: `pip install bayesian-optimization`.")
        assert type(max_concurrent) is int and max_concurrent > 0
        assert utility_kwargs is not None, (
            "Must define arguments for the utiliy function!")
        self._max_concurrent = max_concurrent
        self._reward_attr = reward_attr
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
        """Passes the result to BayesOpt unless early terminated or errored"""
        if result:
            self.optimizer.register(
                params=self._live_trial_mapping[trial_id],
                target=result[self._reward_attr])

        del self._live_trial_mapping[trial_id]

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
