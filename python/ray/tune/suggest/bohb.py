"""BOHB (Bayesian Optimization with HyperBand)"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from hpbandster import BOHB
from ray.tune.suggest import SuggestionAlgorithm


class TuneBOHB(SuggestionAlgorithm):
    def __init__(self,
                 space,
                 max_concurrent=8,
                 metric="neg_mean_loss",
                 bohb_config=None):
        self._max_concurrent = max_concurrent
        self.trial_to_params = {}
        self.running = set()
        self.paused = set()
        self.metric = metric
        bohb_config = bohb_config or {}
        self.bohber = BOHB(space, **bohb_config)
        super(TuneBOHB, self).__init__()

    def _suggest(self, trial_id):
        if len(self.running) < self._max_concurrent:
            config, info = self.bohber.get_config()
            self.trial_to_params[trial_id] = list(config)
            self.running.add(trial_id)
            return config
        return None

    def on_trial_result(self, trial_id, result):
        if trial_id not in self.paused:
            self.running.add(trial_id)
        if "budget" in result.get("hyperband_info", {}):
            hbs_wrapper = self.to_wrapper(trial_id, result)
            print("adding new result", vars(hbs_wrapper))
            self.bohber.new_result(hbs_wrapper)

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        del self.trial_to_params[trial_id]
        if trial_id in self.paused:
            self.paused.remove(trial_id)
        elif trial_id in self.running:
            self.running.remove(trial_id)
        else:
            import ipdb; ipdb.set_trace()


    def to_wrapper(self, trial_id, result):
        return JobWrapper(
            -result[self.metric], result["hyperband_info"]["budget"],
            {k: result["config"][k] for k in self.trial_to_params[trial_id]})

    def on_pause(self, trial_id):
        self.paused.add(trial_id)
        self.running.remove(trial_id)

    def on_unpause(self, trial_id):
        self.paused.remove(trial_id)
        self.running.add(trial_id)