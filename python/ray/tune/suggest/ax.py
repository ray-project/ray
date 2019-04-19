from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

 try:
    from ax.service.ax_client import AxClient
except ImportError:
    ax = None

 from ray.tune.suggest.suggestion import SuggestionAlgorithm


 class AxSearch(SuggestionAlgorithm):
    """A wrapper around Ax to provide trial suggestions.
     Parameters:
        optimizer (nevergrad.optimization.Optimizer): 
     Example:
        >>> 
    """

     def __init__(self,
                 parameters,
                 objective_name,
                 max_concurrent=10,
                 minimize=False,
                 param_constraints=None,
                 outcome_constraints=None,
                 **kwargs):
        assert ax is not None, "Ax must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._experiment = ax.create_experiment(
            name="axsearch",
            parameters=parameters,
            objective_name=objective_name,
            minimize=minimize,
            parameter_constraints=param_constraints,
            outcome_constraints=outcome_constraints
        )
        self._max_concurrent = max_concurrent
        self._parameters = parameters
        self._objective_name = objective_name
        self._live_trial_mapping = {}
        super(AxSearch, self).__init__(**kwargs)

     def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        suggested_config, _ = ax.get_next_trial()
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
        ax_trial_info = self._live_trial_mapping.pop(trial_id)
        if result:
            ax.complete_trial(trial_id, raw_data=-result[self._objective_name])

     def _num_live_trials(self):
        return len(self._live_trial_mapping)