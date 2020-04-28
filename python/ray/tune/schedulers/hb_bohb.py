import logging

from ray.tune.schedulers.trial_scheduler import TrialScheduler
from ray.tune.schedulers.hyperband import HyperBandScheduler, Bracket
from ray.tune.trial import Trial

logger = logging.getLogger(__name__)


class HyperBandForBOHB(HyperBandScheduler):
    """Extends HyperBand early stopping algorithm for BOHB.

    This implementation removes the ``HyperBandScheduler`` pipelining. This
    class introduces key changes:

    1. Trials are now placed so that the bracket with the largest size is
    filled first.

    2. Trials will be paused even if the bracket is not filled. This allows
    BOHB to insert new trials into the training.

    See ray.tune.schedulers.HyperBandScheduler for parameter docstring.
    """

    def on_trial_add(self, trial_runner, trial):
        """Adds new trial.

        On a new trial add, if current bracket is not filled, add to current
        bracket. Else, if current band is not filled, create new bracket, add
        to current bracket. Else, create new iteration, create new bracket,
        add to bracket.
        """

        cur_bracket = self._state["bracket"]
        cur_band = self._hyperbands[self._state["band_idx"]]
        if cur_bracket is None or cur_bracket.filled():
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
                if self._get_r0(s) == 0:
                    logger.debug("BOHB: Bracket too small - Retrying...")
                    cur_bracket = None
                else:
                    retry = False
                    cur_bracket = Bracket(self._time_attr, self._get_n0(s),
                                          self._get_r0(s), self._max_t_attr,
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
            trial_runner._search_alg.searcher.on_pause(trial.trial_id)
            return TrialScheduler.PAUSE
        action = self._process_bracket(trial_runner, bracket)
        return action

    def _unpause_trial(self, trial_runner, trial):
        trial_runner.trial_executor.unpause_trial(trial)
        trial_runner._search_alg.searcher.on_unpause(trial.trial_id)

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
                        # This will change the trial state and let the
                        # trial runner retry.
                        self._process_bracket(trial_runner, bracket)
        # MAIN CHANGE HERE!
        return None
