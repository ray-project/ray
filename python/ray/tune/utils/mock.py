from collections import defaultdict
import logging
import os
import random
import time
from pathlib import Path
from typing import Dict

from ray.tune.callback import Callback
from ray.tune.experiment import Trial

logger = logging.getLogger(__name__)


class FailureInjectorCallback(Callback):
    """Adds random failure injection to the TrialExecutor."""

    def __init__(
        self,
        config_path="~/ray_bootstrap_config.yaml",
        probability=0.1,
        time_between_checks=0,
        disable=False,
    ):
        self.probability = probability
        self.config_path = Path(config_path).expanduser().as_posix()
        self.disable = disable

        self.time_between_checks = time_between_checks
        # Initialize with current time so we don't fail right away
        self.last_fail_check = time.monotonic()

    def on_step_begin(self, **info):
        if not os.path.exists(self.config_path):
            return
        if time.monotonic() < self.last_fail_check + self.time_between_checks:
            return
        self.last_fail_check = time.monotonic()
        import click
        from ray.autoscaler._private.commands import kill_node

        failures = 0
        max_failures = 3
        # With 10% probability inject failure to a worker.
        if random.random() < self.probability and not self.disable:
            # With 10% probability fully terminate the node.
            should_terminate = random.random() < self.probability
            while failures < max_failures:
                try:
                    kill_node(
                        self.config_path,
                        yes=True,
                        hard=should_terminate,
                        override_cluster_name=None,
                    )
                    return
                except click.exceptions.ClickException:
                    failures += 1
                    logger.exception(
                        "Killing random node failed in attempt "
                        "{}. "
                        "Retrying {} more times".format(
                            str(failures), str(max_failures - failures)
                        )
                    )


class TrialStatusSnapshot:
    """A sequence of statuses of trials as they progress.

    If all trials keep previous status, no snapshot is taken.
    """

    def __init__(self):
        self._snapshot = []

    def append(self, new_snapshot: Dict[str, str]):
        """May append a new snapshot to the sequence."""
        if not new_snapshot:
            # Don't add an empty snapshot.
            return
        if not self._snapshot or new_snapshot != self._snapshot[-1]:
            self._snapshot.append(new_snapshot)

    def max_running_trials(self) -> int:
        """Outputs the max number of running trials at a given time.

        Usually used to assert certain number given resource restrictions.
        """
        result = 0
        for snapshot in self._snapshot:
            count = 0
            for trial_id in snapshot:
                if snapshot[trial_id] == Trial.RUNNING:
                    count += 1
            result = max(result, count)

        return result

    def all_trials_are_terminated(self) -> bool:
        """True if all trials are terminated."""
        if not self._snapshot:
            return False
        last_snapshot = self._snapshot[-1]
        return all(
            last_snapshot[trial_id] == Trial.TERMINATED for trial_id in last_snapshot
        )


class TrialStatusSnapshotTaker(Callback):
    """Collects a sequence of statuses of trials as they progress.

    If all trials keep previous status, no snapshot is taken.
    """

    def __init__(self, snapshot: TrialStatusSnapshot):
        self._snapshot = snapshot

    def on_step_end(self, iteration, trials, **kwargs):
        new_snapshot = defaultdict(str)
        for trial in trials:
            new_snapshot[trial.trial_id] = trial.status
        self._snapshot.append(new_snapshot)
