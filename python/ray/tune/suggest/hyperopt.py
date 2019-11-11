from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import copy
import logging
from functools import partial
import pickle
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
    added trials will not be tracked by HyperOpt. Trials of the current run
    can be saved using save method, trials of a previous run can be loaded
    using restore method, thus enabling a warm start feature.

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
        n_initial_points (int): number of random evaluations of the
            objective function before starting to aproximate it with
            tree parzen estimators. Defaults to 20.
        random_state_seed (int, array_like, None): seed for reproducible
            results. Defaults to None.
        gamma (float in range (0,1)): parameter governing the tree parzen
            estimators suggestion algorithm. Defaults to 0.25.
        use_early_stopped_trials (bool): Whether to use early terminated
            trial results in the optimization process.

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
                 n_initial_points=20,
                 random_state_seed=None,
                 gamma=0.25,
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
        if n_initial_points is None:
            self.algo = hpo.tpe.suggest
        else:
            self.algo = partial(
                hpo.tpe.suggest, n_startup_jobs=n_initial_points)
        if gamma is not None:
            self.algo = partial(self.algo, gamma=gamma)
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
        if random_state_seed is None:
            self.rstate = np.random.RandomState()
        else:
            self.rstate = np.random.RandomState(random_state_seed)

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
        """Notification for the completion of trial.

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
            self._hpopt_trials.refresh()
        else:
            self._process_result(trial_id, result, early_terminated)
        del self._live_trial_mapping[trial_id]

    def _process_result(self, trial_id, result, early_terminated=False):
        ho_trial = self._get_hyperopt_trial(trial_id)
        ho_trial["refresh_time"] = hpo.utils.coarse_utcnow()

        if early_terminated and self._use_early_stopped is False:
            ho_trial["state"] = hpo.base.JOB_STATE_ERROR
            ho_trial["misc"]["error"] = (str(TuneError), "Tune Removed")
            return

        ho_trial["state"] = hpo.base.JOB_STATE_DONE
        hp_result = self._to_hyperopt_result(result)
        ho_trial["result"] = hp_result
        self._hpopt_trials.refresh()

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

    def save(self, checkpoint_dir):
        trials_object = (self._hpopt_trials, self.rstate.get_state())
        with open(checkpoint_dir, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self._hpopt_trials = trials_object[0]
        self.rstate.set_state(trials_object[1])
