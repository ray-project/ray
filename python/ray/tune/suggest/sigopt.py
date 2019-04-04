from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import os

try:
    import sigopt as sgo
except Exception:
    sgo = None

from ray.tune.suggest.suggestion import SuggestionAlgorithm


class SigOptSearch(SuggestionAlgorithm):
    """A wrapper around SigOpt to provide trial suggestions.

    Requires SigOpt to be installed. Requires user to store their SigOpt
    API key locally as an environment variable at `SIGOPT_KEY`.

    Parameters:
        space (list of dict): SigOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
        name (str): Name of experiment. Required by SigOpt.
        max_concurrent (int): Number of maximum concurrent trials supported
            based on the user's SigOpt plan. Defaults to 1.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.

    Example:
        >>> space = [
        >>>     {
        >>>         'name': 'width',
        >>>         'type': 'int',
        >>>         'bounds': {
        >>>             'min': 0,
        >>>             'max': 20
        >>>         },
        >>>     },
        >>>     {
        >>>         'name': 'height',
        >>>         'type': 'int',
        >>>         'bounds': {
        >>>             'min': -100,
        >>>             'max': 100
        >>>         },
        >>>     },
        >>> ]
        >>> algo = SigOptSearch(
        >>>     space, name="SigOpt Example Experiment",
        >>>     max_concurrent=1, reward_attr="neg_mean_loss")
    """

    def __init__(self,
                 space,
                 name="Default Tune Experiment",
                 max_concurrent=1,
                 reward_attr="episode_reward_mean",
                 **kwargs):
        assert sgo is not None, "SigOpt must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        assert "SIGOPT_KEY" in os.environ, \
            "SigOpt API key must be stored as environ variable at SIGOPT_KEY"
        self._max_concurrent = max_concurrent
        self._reward_attr = reward_attr
        self._live_trial_mapping = {}

        # Create a connection with SigOpt API, requires API key
        self.conn = sgo.Connection(client_token=os.environ['SIGOPT_KEY'])

        self.experiment = self.conn.experiments().create(
            name=name,
            parameters=space,
            parallel_bandwidth=self._max_concurrent,
        )

        super(SigOptSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None

        # Get new suggestion from SigOpt
        suggestion = self.conn.experiments(
            self.experiment.id).suggestions().create()

        self._live_trial_mapping[trial_id] = suggestion

        return copy.deepcopy(suggestion.assignments)

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Passes the result to SigOpt unless early terminated or errored.

        If a trial fails, it will be reported as a failed Observation, telling
        the optimizer that the Suggestion led to a metric failure, which
        updates the feasible region and improves parameter recommendation.

        Creates SigOpt Observation object for trial.
        """
        if result:
            self.conn.experiments(self.experiment.id).observations().create(
                suggestion=self._live_trial_mapping[trial_id].id,
                value=result[self._reward_attr],
            )
            # Update the experiment object
            self.experiment = self.conn.experiments(self.experiment.id).fetch()
        elif error or early_terminated:
            # Reports a failed Observation
            self.conn.experiments(self.experiment.id).observations().create(
                failed=True, suggestion=self._live_trial_mapping[trial_id].id)
        del self._live_trial_mapping[trial_id]

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
