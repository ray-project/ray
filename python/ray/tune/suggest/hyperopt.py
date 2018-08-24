from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import copy
try:
    import hyperopt as hpo
except Exception as e:
    hpo = None

from ray.tune.error import TuneError
from ray.tune.suggest.suggestion import SuggestionAlgorithm


class HyperOptSearch(SuggestionAlgorithm):
    """A wrapper around HyperOpt to provide trial suggestions.

    Requires HyperOpt to be installed from source.
    Uses the Tree-structured Parzen Estimators algorithm, although can be
    trivially extended to support any algorithm HyperOpt uses. Externally
    added trials will not be tracked by HyperOpt.

    Parameters:
        space (dict): HyperOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
        max_concurrent (int): Number of maximum concurrent trials. Defaults
            to 10.
        reward_attr (str): The training result objective value attribute.
            This refers to an increasing value.

    Example:
        >>> space = {
        >>>     'width': hp.uniform('width', 0, 20),
        >>>     'height': hp.uniform('height', -100, 100),
        >>>     'activation': hp.choice("activation", ["relu", "tanh"])
        >>> }
        >>> config = {
        >>>     "my_exp": {
        >>>         "run": "exp",
        >>>         "num_samples": 10 if args.smoke_test else 1000,
        >>>         "stop": {
        >>>             "training_iteration": 100
        >>>         },
        >>>     }
        >>> }
        >>> algo = HyperOptSearch(
        >>>     space, max_concurrent=4, reward_attr="neg_mean_loss")
        >>> algo.add_configurations(config)
    """

    def __init__(self,
                 space,
                 max_concurrent=10,
                 reward_attr="episode_reward_mean",
                 **kwargs):
        assert hpo is not None, "HyperOpt must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._max_concurrent = max_concurrent
        self._reward_attr = reward_attr
        self.algo = hpo.tpe.suggest
        self.domain = hpo.Domain(lambda spc: spc, space)
        self._hpopt_trials = hpo.Trials()
        self._live_trial_mapping = {}
        self.rstate = np.random.RandomState()

        super(HyperOptSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None
        new_ids = self._hpopt_trials.new_trial_ids(1)
        self._hpopt_trials.refresh()

        # Get new suggestion from Hyperopt
        new_trials = self.algo(new_ids, self.domain, self._hpopt_trials,
                               self.rstate.randint(2**31 - 1))
        self._hpopt_trials.insert_trial_docs(new_trials)
        self._hpopt_trials.refresh()
        new_trial = new_trials[0]
        self._live_trial_mapping[trial_id] = (new_trial["tid"], new_trial)

        # Taken from HyperOpt.base.evaluate
        config = hpo.base.spec_from_misc(new_trial["misc"])
        ctrl = hpo.base.Ctrl(self._hpopt_trials, current_trial=new_trial)
        memo = self.domain.memo_from_config(config)
        hpo.utils.use_obj_for_literal_in_memo(self.domain.expr, ctrl,
                                              hpo.base.Ctrl, memo)

        suggested_config = hpo.pyll.rec_eval(
            self.domain.expr,
            memo=memo,
            print_node_on_error=self.domain.rec_eval_print_node_on_error)
        return copy.deepcopy(suggested_config)

    def on_trial_result(self, trial_id, result):
        ho_trial = self._get_hyperopt_trial(trial_id)
        if ho_trial is None:
            return
        now = hpo.utils.coarse_utcnow()
        ho_trial['book_time'] = now
        ho_trial['refresh_time'] = now

    def on_trial_complete(self,
                          trial_id,
                          result=None,
                          error=False,
                          early_terminated=False):
        """Passes the result to HyperOpt unless early terminated or errored.

        The result is internally negated when interacting with HyperOpt
        so that HyperOpt can "maximize" this value, as it minimizes on default.
        """
        ho_trial = self._get_hyperopt_trial(trial_id)
        if ho_trial is None:
            return
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        if error:
            ho_trial['state'] = hpo.base.JOB_STATE_ERROR
            ho_trial['misc']['error'] = (str(TuneError), "Tune Error")
        elif early_terminated:
            ho_trial['state'] = hpo.base.JOB_STATE_ERROR
            ho_trial['misc']['error'] = (str(TuneError), "Tune Removed")
        else:
            ho_trial['state'] = hpo.base.JOB_STATE_DONE
            hp_result = self._to_hyperopt_result(result)
            ho_trial['result'] = hp_result
        self._hpopt_trials.refresh()
        del self._live_trial_mapping[trial_id]

    def _to_hyperopt_result(self, result):
        return {"loss": -result[self._reward_attr], "status": "ok"}

    def _get_hyperopt_trial(self, trial_id):
        if trial_id not in self._live_trial_mapping:
            return
        hyperopt_tid = self._live_trial_mapping[trial_id][0]
        return [
            t for t in self._hpopt_trials.trials if t["tid"] == hyperopt_tid
        ][0]

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
