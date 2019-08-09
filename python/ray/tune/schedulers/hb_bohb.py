from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging

from ray.tune.schedulers.trial_scheduler import TrialScheduler
from ray.tune.schedulers.hyperband import HyperBandScheduler, Bracket
from ray.tune.trial import Trial

logger = logging.getLogger(__name__)


class HyperBandForBOHB(HyperBandScheduler):
    """Extends HyperBand early stopping algorithm for BOHB.

    Key changes:
        Bracket filling is reversed
        Trial results


    Args:
        time_attr (str): The training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        metric (str): The training result objective value attribute. As
            with `time_attr`, this may refer to any objective value. Stopping
            procedures will use this attribute.
        max_t (int): max time units per trial. Trials will be stopped after
            max_t time units (determined by time_attr) have passed.
            The scheduler will terminate trials after this time has passed.
            Note that this is different from the semantics of `max_t` as
            mentioned in the original HyperBand paper.
        mode (str): One of {min, max}. Determines whether objective is
                minimizing or maximizing the metric attribute.
    """

    def on_trial_add(self, trial_runner, trial):
        """Adds new trial.

        On a new trial add, if current bracket is not filled,
        add to current bracket. Else, if current band is not filled,
        create new bracket, add to current bracket.
        Else, create new iteration, create new bracket, add to bracket."""

        cur_bracket = self._state["bracket"]
        cur_band = self._hyperbands[self._state["band_idx"]]
        if cur_bracket is None or cur_bracket.filled():
            print("Adding a new bracket.")
            retry = True
            while retry:
                # if current iteration is filled, create new iteration
                if self._cur_band_filled():
                    cur_band = []
                    self._hyperbands.append(cur_band)
                    self._state["band_idx"] += 1

                # MAIN CHANGE HERE - largest bracket first!
                # cur_band will always be less than s_max_1 or else filled
                s = self._s_max_1 - len(cur_band) - 1
                assert s >= 0, "Current band is filled!"
                # MAIN CHANGE HERE!
                if self._get_r0(s) == 0:
                    logger.info("Bracket too small - Retrying...")
                    cur_bracket = None
                else:
                    retry = False
                    cur_bracket = ContinuationBracket(self._time_attr,
                                                      self._get_n0(s),
                                                      self._get_r0(s),
                                                      self._max_t_attr,
                                                      self._eta, s)
                cur_band.append(cur_bracket)
                self._state["bracket"] = cur_bracket

        self._state["bracket"].add_trial(trial)
        self._trial_info[trial] = cur_bracket, self._state["band_idx"]

    def on_trial_result(self, trial_runner, trial, result):
        """If bracket is finished, all trials will be stopped.

        If a given trial finishes and bracket iteration is not done,
        the trial will be paused and resources will be given up.

        This scheduler will not start trials but will stop trials.
        The current running trial will not be handled,
        as the trialrunner will be given control to handle it."""
        
        result["hyperband_info"] = {}
        bracket, _ = self._trial_info[trial]
        bracket.update_trial_stats(trial, result)

        if bracket.continue_trial(trial):
            return TrialScheduler.CONTINUE

        result["hyperband_info"]["budget"] = bracket._cumul_r

        # MAIN CHANGE HERE!
        statuses = [(t, t.status) for t in bracket._live_trials]
        if not bracket.filled() or any(status != Trial.PAUSED
                                       for t, status in statuses
                                       if t is not trial):
            trial_runner._search_alg.on_pause(trial.trial_id)
            return TrialScheduler.PAUSE
        action = self._process_bracket(trial_runner, bracket, trial)
        return action

    def _process_bracket(self, trial_runner, bracket):
        """This is called whenever a trial makes progress.

        When all live trials in the bracket have no more iterations left,
        Trials will be successively halved. If bracket is done, all
        non-running trials will be stopped and cleaned up,
        and during each halving phase, bad trials will be stopped while good
        trials will return to "PENDING"."""

        action = TrialScheduler.PAUSE
        if bracket.cur_iter_done():
            if bracket.finished():
                bracket.cleanup_full(trial_runner)
                return TrialScheduler.STOP

            good, bad = bracket.successive_halving(
                self._metric_op * self._metric)
            # kill bad trials
            self._num_stopped += len(bad)
            for t in bad:
                if t.status == Trial.PAUSED:
                    trial_runner.stop_trial(t)
                elif t.status == Trial.RUNNING:
                    bracket.cleanup_trial(t)
                    action = TrialScheduler.STOP
                else:
                    raise Exception("Trial with unexpected status encountered")

            # ready the good trials - if trial is too far ahead, don't continue
            for t in good:
                if t.status not in [Trial.PAUSED, Trial.RUNNING]:
                    raise Exception("Trial with unexpected status encountered")
                if bracket.continue_trial(t):
                    if t.status == Trial.PAUSED:
                        trial_runner.trial_executor.unpause_trial(t)
                        # MAIN CHANGE HERE!
                        trial_runner._search_alg.on_unpause(t.trial_id)
                        # MAIN CHANGE HERE!
                    elif t.status == Trial.RUNNING:
                        action = TrialScheduler.CONTINUE
        return action

    def choose_trial_to_run(self, trial_runner):
        """Fair scheduling within iteration by completion percentage.

        List of trials not used since all trials are tracked as state
        of scheduler. If iteration is occupied (ie, no trials to run),
        then look into next iteration.
        """

        for hyperband in self._hyperbands:
            # band will have None entries if no resources
            # are to be allocated to that bracket.
            scrubbed = [b for b in hyperband if b is not None]
            for bracket in scrubbed:
                for trial in bracket.current_trials():
                    if (trial.status == Trial.PENDING
                            and trial_runner.has_resources(trial.resources)):
                        return trial
        # MAIN CHANGE HERE!
        if not any(t.status == Trial.RUNNING
                   for t in trial_runner.get_trials()):
            for hyperband in self._hyperbands:
                for bracket in hyperband:
                    if bracket and any(trial.status == Trial.PAUSED
                                       for trial in bracket.current_trials()):
                        self._process_bracket(trial_runner, bracket, None)
        # MAIN CHANGE HERE!
        return None


class ContinuationBracket(Bracket):
    """Logical object for tracking Hyperband bracket progress. Keeps track
    of proper parameters as designated by HyperBand.

    Also keeps track of progress to ensure good scheduling.
    """

    def successive_halving(self, metric):
        assert self._halves > 0
        self._halves -= 1
        self._n /= self._eta
        self._n = int(np.ceil(self._n))

        self._r *= self._eta
        self._r = int(min(self._r, self._max_t_attr - self._cumul_r))

        # MAIN CHANGE HERE - don't accumulate R.
        self._cumul_r = self._r

        sorted_trials = sorted(
            self._live_trials, key=lambda t: self._live_trials[t][metric])

        good, bad = sorted_trials[-self._n:], sorted_trials[:-self._n]
        return good, bad

    def update_trial_stats(self, trial, result):
        """Update result for trial. Called after trial has finished
        an iteration - will decrement iteration count."""

        assert trial in self._live_trials
        assert self._get_result_time(result) >= 0

        delta = self._get_result_time(result) - \
            self._get_result_time(self._live_trials[trial])
        self._completed_progress += delta
        self._live_trials[trial] = result
