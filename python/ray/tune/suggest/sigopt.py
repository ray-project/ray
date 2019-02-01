from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import copy
import logging
import os

try:
    from sigopt import Connection
except Exception:
    Connection = None

from ray.tune.error import TuneError
from ray.tune.suggest.suggestion import SuggestionAlgorithm


class SigOptSearch(SuggestionAlgorithm):
    """A wrapper around SigOpt to provide trial suggestions.

    Requires SigOpt to be installed. Can only support 10 concurrent trials at once.

    Parameters: TODO: change depending on how sigopt initializes objects
        space (list of dict): SigOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.

    Example:
        >>> space = [
        >>>     {'name': 'x1',
        >>>      'type': 'double',
        >>>      'bounds': { 'min': -70.0, 'max': 70.0 },
        >>>     },
        >>>     {'name': 'x2',
        >>>      'type': 'double',
        >>>      'bounds': { 'min': -70.0, 'max': 70.0 },
        >>>     },
        >>> ]
        >>> config = {
        >>>     "my_exp": {
        >>>         "run": "exp",
        >>>         "num_samples": 10 if args.smoke_test else 1000,
        >>>         "stop": {
        >>>             "training_iteration": 100
        >>>         },
        >>>     }
        >>> }
        >>> algo = SigOptSearch(
        >>>     parameters, max_concurrent=4, reward_attr="neg_mean_loss")
    """

    def __init__(self,
                 space,
                 name='test', # TODO
                 max_concurrent=10,
                 reward_attr="episode_reward_mean",
                 **kwargs):
        assert Connection is not None, "SigOpt must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._max_concurrent = max_concurrent
        self._reward_attr = reward_attr
        self._live_trial_mapping = {}

        self.conn = Connection(client_token=os.environ['SIGOPT_KEY'])
        
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
        suggestion = self.conn.experiments(self.experiment.id).suggestions().create().assignments
        
        self._live_trial_mapping[trial_id] = suggestion

        return copy.deepcopy(suggestion)

    def on_trial_result(self, trial_id, result):
        pass

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Passes the result to SigOpt unless early terminated or errored.

        The result is internally negated when interacting with HyperOpt
        so that HyperOpt can "maximize" this value, as it minimizes on default.
        """
        pass

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
