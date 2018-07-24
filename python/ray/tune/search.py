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


class SearchAlgorithm():
    """This class is unaware of Tune trials."""
    NULL_ID = "NULL_ID"
    NOT_READY = "NOT_READY"

    def try_suggest():
        """
        Returns:
            Configuration for a trial
            Suggestion ID
        """
        pass

    def on_trial_result():
        pass

    def on_trial_error():
        pass

    def on_trial_complete():
        pass


class HyperOptAlgorithm(SearchAlgorithm):

    def __init__(
            self, space, max_concurrent=10,
            reward_attr="episode_reward_mean"):
        assert hpo is not None, "HyperOpt must be installed!"
        assert type(max_concurrent) is int and max_concurrent > 0
        self._max_concurrent = max_concurrent
        self._reward_attr = reward_attr
        self.algo = hpo.tpe.suggest
        self.domain = hpo.Domain(lambda spc: spc, space)
        self._hpopt_trials = hpo.Trials()
        self._tune_to_hp = {}

        self.rstate = np.random.RandomState()

    def try_suggest(self):
        if not self._num_live_trials() < self._max_concurrent:
            return SearchAlgorithm.NOT_READY, SearchAlgorithm.NULL_ID
        new_ids = self._hpopt_trials.new_trial_ids(1)
        self._hpopt_trials.refresh()

        # Get new suggestion from
        new_trials = self.algo(new_ids, self.domain, self._hpopt_trials,
                               self.rstate.randint(2**31 - 1))
        self._hpopt_trials.insert_trial_docs(new_trials)
        self._hpopt_trials.refresh()
        new_trial = new_trials[0]
        new_trial_id = new_trial["tid"]

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
        return copy.deepcopy(suggested_config), new_trial_id

    def on_trial_result(self, suggestion_id, result):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[suggestion_id])
        now = hpo.utils.coarse_utcnow()
        ho_trial['book_time'] = now
        ho_trial['refresh_time'] = now

    def on_trial_error(self, suggestion_id):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[suggestion_id])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_ERROR
        ho_trial['misc']['error'] = (str(TuneError), "Tune Error")
        self._hpopt_trials.refresh()
        del self._tune_to_hp[suggestion_id]

    def on_trial_remove(self, suggestion_id):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[suggestion_id])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_ERROR
        ho_trial['misc']['error'] = (str(TuneError), "Tune Removed")
        self._hpopt_trials.refresh()
        del self._tune_to_hp[suggestion_id]

    def on_trial_complete(self, suggestion_id, result):
        ho_trial = self._get_hyperopt_trial(self._tune_to_hp[suggestion_id])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_DONE
        hp_result = self._to_hyperopt_result(result)
        ho_trial['result'] = hp_result
        self._hpopt_trials.refresh()
        del self._tune_to_hp[suggestion_id]

    def _to_hyperopt_result(self, result):
        return {"loss": -getattr(result, self._reward_attr), "status": "ok"}

    def _get_hyperopt_trial(self, tid):
        return [t for t in self._hpopt_trials.trials if t["tid"] == tid][0]

    def _num_live_trials(self):
        return len(self._tune_to_hp)
