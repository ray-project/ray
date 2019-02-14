from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import skopt
except Exception:
    skopt = None

from ray.tune.suggest.suggestion import SuggestionAlgorithm


class SkOptSearch(SuggestionAlgorithm):
    """A wrapper around skopt to provide trial suggestions.

    Requires skopt to be installed.

    Parameters:
        optimizer (skopt.optimizer.Optimizer): Optimizer provided
            from skopt.
        parameter_names (list): List of parameter names. Should match
            the dimension of the optimizer output.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.

    Example:
        >>> from skopt import Optimizer
        >>> optimizer = Optimizer([(0,20),(-100,100)])
        >>> config = {
        >>>     "my_exp": {
        >>>         "run": "exp",
        >>>         "num_samples": 10,
        >>>         "stop": {
        >>>             "training_iteration": 100
        >>>         },
        >>>     }
        >>> }
        >>> algo = SkOptSearch(optimizer,
        >>>     ["width", "height"], max_concurrent=4,
        >>>     reward_attr="neg_mean_loss")
    """

    def __init__(self,
                 optimizer,
                 parameter_names,
                 max_concurrent=10,
                 reward_attr="episode_reward_mean",
                 **kwargs):
        assert skopt is not None, """skopt must be installed!
            You can install Skopt with the command:
            `pip install scikit-optimize`."""
        assert type(max_concurrent) is int and max_concurrent > 0
        self._max_concurrent = max_concurrent
        self._parameters = parameter_names
        self._reward_attr = reward_attr
        self._skopt_opt = optimizer
        self._live_trial_mapping = {}
        super(SkOptSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        suggested_config = self._skopt_opt.ask()
        self._live_trial_mapping[trial_id] = suggested_config
        return dict(zip(self._parameters, suggested_config))

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Passes the result to skopt unless early terminated or errored.

        The result is internally negated when interacting with Skopt
        so that Skopt Optimizers can "maximize" this value,
        as it minimizes on default.
        """
        skopt_trial_info = self._live_trial_mapping.pop(trial_id)
        if result:
            self._skopt_opt.tell(skopt_trial_info, -result[self._reward_attr])

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
