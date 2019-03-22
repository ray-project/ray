from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import nevergrad
except Exception:
    nevergrad = None

from ray.tune.suggest.suggestion import SuggestionAlgorithm


class NevergradSearch(SuggestionAlgorithm):
    """A wrapper around Nevergrad to provide trial suggestions.

    Requires Nevergrad to be installed.
    Nevergrad is an open source tool from Facebook for derivative free
    optimization of parameters and/or hyperparameters. It features a wide
    range of optimizers in a standard ask and tell interface. More information
    can be found at https://github.com/facebookresearch/nevergrad.

    Parameters:
        optimizer (nevergrad.optimization.Optimizer): Optimizer provided
            from Nevergrad.
        parameter_names (list): List of parameter names. Should match
            the dimension of the optimizer output.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.

    Example:
        >>> from nevergrad.optimization import optimizerlib
        >>> optimizer = optimizerlib.OnePlusOne(dimension=1, budget=100)
        >>> algo = NevergradSearch(
        >>>     optimizer, max_concurrent=4, reward_attr="neg_mean_loss")
    """

    def __init__(self,
                 optimizer,
                 parameter_names,
                 max_concurrent=10,
                 reward_attr="episode_reward_mean",
                 **kwargs):
        assert nevergrad is not None, "Nevergrad must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._max_concurrent = max_concurrent
        self._parameters = parameter_names
        self._reward_attr = reward_attr
        self._nevergrad_opt = optimizer
        self._live_trial_mapping = {}
        super(NevergradSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        suggested_config = self._nevergrad_opt.ask()
        self._live_trial_mapping[trial_id] = suggested_config
        return dict(zip(self._parameters, suggested_config))

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Passes the result to Nevergrad unless early terminated or errored.

        The result is internally negated when interacting with Nevergrad
        so that Nevergrad Optimizers can "maximize" this value,
        as it minimizes on default.
        """
        ng_trial_info = self._live_trial_mapping.pop(trial_id)
        if result:
            self._nevergrad_opt.tell(ng_trial_info, -result[self._reward_attr])

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
