from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import copy
import logging
try:
    hyperopt_logger = logging.getLogger("hyperopt")
    hyperopt_logger.setLevel(logging.WARNING)
    import hyperopt as hpo
except ImportError:
    hpo = None

from ray.tune.error import TuneError
from ray.tune.suggest.suggestion import SuggestionAlgorithm

logger = logging.getLogger(__name__)


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
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate (list): Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want hyperopt to run first to help the TPE algorithm
            make better suggestions for future parameters. Needs to be
            a list of dict of hyperopt-named variables.
            Choice variables should be indicated by their index in the
            list (see example)

    Example:
        >>> space = {
        >>>     'width': hp.uniform('width', 0, 20),
        >>>     'height': hp.uniform('height', -100, 100),
        >>>     'activation': hp.choice("activation", ["relu", "tanh"])
        >>> }
        >>> current_best_params = [{
        >>>     'width': 10,
        >>>     'height': 0,
        >>>     'activation': 0, # The index of "relu"
        >>> }]
        >>> algo = HyperOptSearch(
        >>>     space, max_concurrent=4, metric="mean_loss", mode="min",
        >>>     points_to_evaluate=current_best_params)
    """

    def __init__(self,
                 space,
                 max_concurrent=10,
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 points_to_evaluate=None,
                 **kwargs):
        assert hpo is not None, "HyperOpt must be installed!"
        from hyperopt.fmin import generate_trials_to_calculate
        assert type(max_concurrent) is int and max_concurrent > 0
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"

        if reward_attr is not None:
            mode = "max"
            metric = reward_attr
            logger.warning(
                "`reward_attr` is deprecated and will be removed in a future "
                "version of Tune. "
                "Setting `metric={}` and `mode=max`.".format(reward_attr))

        self._max_concurrent = max_concurrent
        self._metric = metric
        # hyperopt internally minimizes, so "max" => -1
        if mode == "max":
            self._metric_op = -1.
        elif mode == "min":
            self._metric_op = 1.
        self.algo = hpo.tpe.suggest
        self.domain = hpo.Domain(lambda spc: spc, space)
        if points_to_evaluate is None:
            self._hpopt_trials = hpo.Trials()
            self._points_to_evaluate = 0
        else:
            assert type(points_to_evaluate) == list
            self._hpopt_trials = generate_trials_to_calculate(
                points_to_evaluate)
            self._hpopt_trials.refresh()
            self._points_to_evaluate = len(points_to_evaluate)
        self._live_trial_mapping = {}
        self.rstate = np.random.RandomState()

        super(HyperOptSearch, self).__init__(**kwargs)

    def _suggest(self, trial_id):
        if self._num_live_trials() >= self._max_concurrent:
            return None

        if self._points_to_evaluate > 0:
            new_trial = self._hpopt_trials.trials[self._points_to_evaluate - 1]
            self._points_to_evaluate -= 1
        else:
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
        ho_trial["book_time"] = now
        ho_trial["refresh_time"] = now

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
        ho_trial["refresh_time"] = hpo.utils.coarse_utcnow()
        if error:
            ho_trial["state"] = hpo.base.JOB_STATE_ERROR
            ho_trial["misc"]["error"] = (str(TuneError), "Tune Error")
        elif early_terminated:
            ho_trial["state"] = hpo.base.JOB_STATE_ERROR
            ho_trial["misc"]["error"] = (str(TuneError), "Tune Removed")
        else:
            ho_trial["state"] = hpo.base.JOB_STATE_DONE
            hp_result = self._to_hyperopt_result(result)
            ho_trial["result"] = hp_result
        self._hpopt_trials.refresh()
        del self._live_trial_mapping[trial_id]

    def _to_hyperopt_result(self, result):
        return {"loss": self._metric_op * result[self._metric], "status": "ok"}

    def _get_hyperopt_trial(self, trial_id):
        if trial_id not in self._live_trial_mapping:
            return
        hyperopt_tid = self._live_trial_mapping[trial_id][0]
        return [
            t for t in self._hpopt_trials.trials if t["tid"] == hyperopt_tid
        ][0]

    def _num_live_trials(self):
        return len(self._live_trial_mapping)
