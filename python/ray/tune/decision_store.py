import logging
from typing import Optional, Tuple

from ray.tune import TuneError
from ray.tune.schedulers import SchedulingDecision
from ray.tune.trial import Trial

logger = logging.getLogger(__name__)


class DecisionStore:
    """Scheduling decision store.

    There may be multiple decisions generated per Tune event. Sometimes,
    the execution of these decisions cannot be done within the life span of
    the Tune step that generates them. For example, a trial may need to finish
    saving first before the following decision can be executed. Or a trial
    may need to finish replace(back into RUNNING state) before SAVE can be
    executed. To avoid blocking Tune loop, such "finish I before II" pattern is
    done across multiple Tune steps. This class manages decision caching in
    the process.

    Secondly, the class assigns explicit executing order to various decisions.
    i.e. REPLACE > SAVE > the rest.

    Thirdly, the class serves as a validator to check if certain decision is
    meaningful given the state a trial is at.
    """

    def __init__(self):
        # A map between trial and (STOP, PAUSE, CONTINUE) decisions.
        self._decisions = {}
        # A set of trials to save.
        self._trials_to_save = set()
        # Trials to replace with metadata.
        self._trials_to_replace = {}

    def add_decision(self, trial, scheduling_decision):
        if scheduling_decision.type == SchedulingDecision.SAVE:
            assert trial.status == Trial.RUNNING
            self._trials_to_save.add(trial)
        elif scheduling_decision.type == SchedulingDecision.REPLACE:
            # REPLACE and (PAUSE, CONTINUE, STOP) should be exclusive.
            assert trial not in self._decisions
            assert scheduling_decision.metadata
            self._trials_to_replace[trial] = scheduling_decision
        elif scheduling_decision.type in (SchedulingDecision.PAUSE,
                                          SchedulingDecision.CONTINUE,
                                          SchedulingDecision.STOP):
            # REPLACE and (PAUSE, CONTINUE, STOP) should be exclusive.
            assert trial not in self._trials_to_replace
            decision_types = set()
            if trial in self._decisions:
                decision_types.add(self._decisions[trial].type)
            decision_types.add(scheduling_decision.type)
            if SchedulingDecision.STOP in decision_types:
                self._decisions[trial] = SchedulingDecision(
                    SchedulingDecision.STOP)
            elif SchedulingDecision.PAUSE in decision_types:
                self._decisions[trial] = SchedulingDecision(
                    SchedulingDecision.PAUSE)
            else:
                self._decisions[trial] = SchedulingDecision(
                    SchedulingDecision.CONTINUE)
        else:
            raise TuneError("Unexpected TrialSchedulerV2 decision type!!")

    def add_decisions(self, decisions):
        for trial, decision in decisions.items():
            self.add_decision(trial, decision)

    def get_decision(
            self) -> Tuple[Optional[Trial], Optional[SchedulingDecision]]:
        """Get next decision to execute."""
        if len(self._trials_to_replace) > 0:
            trial, decision = self._trials_to_replace.popitem()
            assert trial.status in (Trial.RUNNING, Trial.PAUSED)
            return trial, decision
        trials_to_save = [
            t for t in self._trials_to_save if t.status == Trial.RUNNING
        ]
        if len(trials_to_save) > 0:
            trial = trials_to_save.pop()
            self._trials_to_save.remove(trial)
            return trial, SchedulingDecision(SchedulingDecision.SAVE)
        for t in self._trials_to_save:
            assert t.status == Trial.PAUSED
            logger.info(f"Delayed saving for {t} as it's just being replaced "
                        f"and is currently in PAUSED state.")
        # Now do the rest.
        if len(self._decisions) > 0:
            trial, decision = self._decisions.popitem()
            if decision.type == SchedulingDecision.STOP:
                assert (trial.status == Trial.RUNNING
                        and trial.internal_status == Trial.WAITING) or (
                            Trial.status == Trial.PAUSED)
            else:
                assert (trial.status == Trial.RUNNING
                        and trial.internal_status == Trial.WAITING)
            return trial, decision
        return None, None

    def should_save(self, trial):
        if trial in self._trials_to_save:
            self._trials_to_save.remove(trial)
            return True
        else:
            return False
