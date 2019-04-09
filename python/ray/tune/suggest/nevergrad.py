from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import nevergrad as ng
except ImportError:
    ng = None

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
            the dimension of the optimizer output. Alternatively, set to None
            if the optimizer is already instrumented with kwargs
            (see nevergrad v0.2.0+).
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.

    Example:
        >>> from nevergrad.optimization import optimizerlib
        >>> instrumentation = 1
        >>> optimizer = optimizerlib.OnePlusOne(instrumentation, budget=100)
        >>> algo = NevergradSearch(optimizer, ["lr"], max_concurrent=4,
        >>>                        reward_attr="neg_mean_loss")

    Note:
        In nevergrad v0.2.0+, optimizers can be instrumented.
        For instance, the following will specifies searching
        for "lr" from 1 to 2.

        >>> from nevergrad.optimization import optimizerlib
        >>> from nevergrad import instrumentation as inst
        >>> lr = inst.var.Array(1).bounded(1, 2).asfloat()
        >>> instrumentation = inst.Instrumentation(lr=lr)
        >>> optimizer = optimizerlib.OnePlusOne(instrumentation, budget=100)
        >>> algo = NevergradSearch(optimizer, None, max_concurrent=4,
        >>>                        reward_attr="neg_mean_loss")

    """

    def __init__(self,
                 optimizer,
                 parameter_names,
                 max_concurrent=10,
                 reward_attr="episode_reward_mean",
                 **kwargs):
        assert ng is not None, "Nevergrad must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._max_concurrent = max_concurrent
        self._parameters = parameter_names
        self._reward_attr = reward_attr
        self._nevergrad_opt = optimizer
        self._live_trial_mapping = {}
        super(NevergradSearch, self).__init__(**kwargs)
        # validate parameters
        if hasattr(optimizer, "instrumentation"):  # added in v0.2.0
            if optimizer.instrumentation.kwargs:
                if optimizer.instrumentation.args:
                    raise ValueError(
                        "Instrumented optimizers should use kwargs only")
                if parameter_names is not None:
                    raise ValueError("Instrumented optimizers should provide "
                                     "None as parameter_names")
            else:
                if parameter_names is None:
                    raise ValueError("Non-instrumented optimizers should have "
                                     "a list of parameter_names")
                if len(optimizer.instrumentation.args) != 1:
                    raise ValueError(
                        "Instrumented optimizers should use kwargs only")
        if parameter_names is not None and optimizer.dimension != len(
                parameter_names):
            raise ValueError("len(parameters_names) must match optimizer "
                             "dimension for non-instrumented optimizers")

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        suggested_config = self._nevergrad_opt.ask()
        self._live_trial_mapping[trial_id] = suggested_config
        # in v0.2.0+, output of ask() is a Candidate,
        # with fields args and kwargs
        if hasattr(self._nevergrad_opt, "instrumentation"):
            if not suggested_config.kwargs:
                return dict(zip(self._parameters, suggested_config.args[0]))
            else:
                return suggested_config.kwargs
        # legacy: output of ask() is a np.ndarray
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
