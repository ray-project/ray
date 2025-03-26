import logging
from typing import TYPE_CHECKING, Dict, Optional

from ray.tune.experiment import Trial
from ray.tune.schedulers.hyperband import HyperBandScheduler
from ray.tune.schedulers.trial_scheduler import TrialScheduler
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.tune.execution.tune_controller import TuneController

logger = logging.getLogger(__name__)


@PublicAPI
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

    def on_trial_add(self, tune_controller: "TuneController", trial: Trial):
        """Adds new trial.

        On a new trial add, if current bracket is not filled, add to current
        bracket. Else, if current band is not filled, create new bracket, add
        to current bracket. Else, create new iteration, create new bracket,
        add to bracket.
        """
        if not self._metric or not self._metric_op:
            raise ValueError(
                "{} has been instantiated without a valid `metric` ({}) or "
                "`mode` ({}) parameter. Either pass these parameters when "
                "instantiating the scheduler, or pass them as parameters "
                "to `tune.TuneConfig()`".format(
                    self.__class__.__name__, self._metric, self._mode
                )
            )

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
                    cur_bracket = self._create_bracket(s)
                cur_band.append(cur_bracket)
                self._state["bracket"] = cur_bracket

        self._state["bracket"].add_trial(trial)
        self._trial_info[trial] = cur_bracket, self._state["band_idx"]

    def on_trial_result(
        self, tune_controller: "TuneController", trial: Trial, result: Dict
    ) -> str:
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
        if not bracket.filled() or any(
            status != Trial.PAUSED for t, status in statuses if t is not trial
        ):
            # BOHB Specific. This hack existed in old Ray versions
            # and was removed, but it needs to be brought back
            # as otherwise the BOHB doesn't behave as intended.
            # The default concurrency limiter works by discarding
            # new suggestions if there are more running trials
            # than the limit. That doesn't take into account paused
            # trials. With BOHB, this leads to N trials finishing
            # completely and then another N trials starting,
            # instead of trials being paused and resumed in brackets
            # as intended.
            # There should be a better API for this.
            # TODO(team-ml): Refactor alongside HyperBandForBOHB
            tune_controller.search_alg.searcher.on_pause(trial.trial_id)
            return TrialScheduler.PAUSE

        logger.debug(f"Processing bracket after trial {trial} result")
        action = self._process_bracket(tune_controller, bracket)
        if action == TrialScheduler.PAUSE:
            tune_controller.search_alg.searcher.on_pause(trial.trial_id)
        return action

    def _unpause_trial(self, tune_controller: "TuneController", trial: Trial):
        # Hack. See comment in on_trial_result
        tune_controller.search_alg.searcher.on_unpause(trial.trial_id)

    def choose_trial_to_run(
        self, tune_controller: "TuneController", allow_recurse: bool = True
    ) -> Optional[Trial]:
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
                    if (
                        trial.status == Trial.PAUSED
                        and trial in bracket.trials_to_unpause
                    ) or trial.status == Trial.PENDING:
                        return trial
        # MAIN CHANGE HERE!
        if not any(t.status == Trial.RUNNING for t in tune_controller.get_trials()):
            for hyperband in self._hyperbands:
                for bracket in hyperband:
                    if bracket and any(
                        trial.status == Trial.PAUSED
                        for trial in bracket.current_trials()
                    ):
                        # This will change the trial state
                        logger.debug("Processing bracket since no trial is running.")
                        self._process_bracket(tune_controller, bracket)

                        # If there are pending trials now, suggest one.
                        # This is because there might be both PENDING and
                        # PAUSED trials now, and PAUSED trials will raise
                        # an error before the trial runner tries again.
                        if allow_recurse and any(
                            (
                                trial.status == Trial.PAUSED
                                and trial in bracket.trials_to_unpause
                            )
                            or trial.status == Trial.PENDING
                            for trial in bracket.current_trials()
                        ):
                            return self.choose_trial_to_run(
                                tune_controller, allow_recurse=False
                            )
        # MAIN CHANGE HERE!
        return None
