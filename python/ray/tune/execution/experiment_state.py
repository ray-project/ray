import fnmatch
import logging
import os
import time
from collections import Counter
from pathlib import Path
from typing import Callable, Dict, Optional, Union

import pyarrow.fs

from ray.train._internal.storage import (
    StorageContext,
    _download_from_fs_path,
    _list_at_fs_path,
    get_fs_and_path,
)
from ray.tune.experiment.trial import Trial
from ray.tune.impl.out_of_band_serialize_dataset import out_of_band_serialize_dataset

logger = logging.getLogger(__name__)


_SLOW_SYNC_WARNING = (
    "This could be due to a large number of trials, "
    "large logfiles from lots of reported metrics, or throttling from the "
    "remote storage if uploading too frequently.\n"
    "You may want to consider switching the `RunConfig(storage_filesystem)`"
    " to a more performant storage backend such as s3fs for a "
    "S3 storage path.\n"
    "You can suppress this error by setting the environment variable "
    "TUNE_WARN_SLOW_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S to a higher "
    "value than the current threshold ({threshold})."
)


def _find_newest_experiment_checkpoint(
    experiment_path: str, fs: Optional[pyarrow.fs.FileSystem] = None
) -> Optional[str]:
    """Returns file name of most recently created experiment checkpoint.

    Args:
        experiment_path: Local or remote path to the experiment directory
            containing at least one experiment checkpoint file.

    Returns:
        str: The local or remote path to the latest experiment checkpoint file
            based on timestamp. None if no experiment checkpoints were found.
    """
    from ray.tune.execution.tune_controller import TuneController

    fs, experiment_fs_path = get_fs_and_path(experiment_path, storage_filesystem=fs)
    filenames = _list_at_fs_path(fs=fs, fs_path=experiment_fs_path)
    pattern = TuneController.CKPT_FILE_TMPL.format("*")
    matching = fnmatch.filter(filenames, pattern)
    if not matching:
        return None
    filename = max(matching)
    return Path(experiment_fs_path, filename).as_posix()


class _ExperimentCheckpointManager:
    """Helper class for managing experiment-level checkpoints.

    This class implements the ``checkpoint()`` method used to checkpoint
    experiment state. When called, this will serialize and write to disk
    the state of the trial runner, trial executor, and search algorithm, to
    a specified checkpoint file.

    The checkpoint period is automatically adjusted to
    ``max(10, time_per_checkpoint * 19)``. This means that at most 5% of the
    time (1/20) will be used for writing checkpoints, while 95% of the time
    (19/20) will be used to handle the rest of the training loop.
    """

    def __init__(
        self,
        *,
        storage: Optional[StorageContext],
        checkpoint_period: Union[int, float, str],
        sync_every_n_trial_checkpoints: Optional[int] = None,
    ):
        self._storage = storage

        self._last_save_time = float("-inf")
        self._last_sync_time = None

        # Dynamic checkpointing period
        self._auto_checkpoint_enabled = checkpoint_period == "auto"
        if self._auto_checkpoint_enabled:
            self._checkpoint_period = 10.0  # Initial value
        else:
            self._checkpoint_period = float(checkpoint_period)

        # TODO(justinvyu): This is a non-performant workaround to force sync
        # every num_to_keep checkpoints in order to maintain consistency
        # between the experiment state's view of the latest checkpoint,
        # and the actual latest checkpoint that was uploaded.
        self._sync_every_n_trial_checkpoints = sync_every_n_trial_checkpoints
        self._trial_num_checkpoints_since_last_sync: Dict[Trial, int] = Counter()
        self._should_force_sync_up: bool = False

        self._excessive_sync_threshold = float(
            os.environ.get(
                "TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S", "5"
            )
        )
        self._slow_sync_threshold = float(
            os.environ.get(
                "TUNE_WARN_SLOW_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S", "30"
            )
        )

    @property
    def auto_checkpoint_enabled(self):
        return self._auto_checkpoint_enabled

    def _update_auto_checkpoint_time(self, time_taken: float):
        if self._auto_checkpoint_enabled:
            # Multiplying this time by 19 means we spend ~5% of the time
            # writing global checkpoints and 95% of the time processing trials
            self._checkpoint_period = max(10.0, time_taken * 19)
            logger.debug(
                f"Experiment state snapshotting took "
                f"{time_taken:.2f} seconds. "
                f"Adjusting snapshotting period to "
                f"{self._checkpoint_period:.2f} seconds."
            )

    def sync_up_experiment_state(
        self,
        save_fn: Callable[[], None],
        force: bool = False,
        wait: bool = False,
    ):
        """Saves execution state to the experiment directory on the storage path.
        This includes an experiment checkpoint file that contains trial statuses
        and the searcher state.

        Overwrites the current session checkpoint, which starts when self
        is instantiated. Throttle depends on self._checkpoint_period.

        Args:
            save_fn: Function to call to actually save data to the driver
                staging path. The files in the driver staging path will be
                uploaded to the storage path.
            force: Forces an experiment checkpoint and launches a sync to storage.
                This happens regardless of checkpoint_period
            wait: Waits for the sync up to complete before returning.
        """
        driver_staging_path = self._storage.experiment_driver_staging_path

        force = force or self._should_force_sync_up

        now = time.monotonic()
        if now - self._last_save_time < self._checkpoint_period and not force:
            return

        # Checkpoint
        checkpoint_time_start = time.monotonic()

        # NOTE: This context manager is for Datasets captured in a trial config.
        # This is the case when *tuning over datasets*.
        # If the datasets have already been full executed, then serializing
        # block refs means that this checkpoint is not usable in a new Ray cluster.
        # This context will serialize the dataset execution plan instead, if available.
        with out_of_band_serialize_dataset():
            save_fn()

        def wait_for_sync():
            try:
                self._storage.syncer.wait()
            except Exception:
                logger.error(
                    "Saving experiment state to storage at "
                    f"'{self._storage.experiment_fs_path}' failed with exception: ",
                    exc_info=True,
                )

        if force:
            start_time = time.monotonic()
            wait_for_sync()
            wait_time = time.monotonic() - start_time
            if wait_time > self._slow_sync_threshold:
                logger.warning(
                    "Saving the experiment state (which holds a global view "
                    "of trial statuses and is used to restore the experiment) "
                    f"took ~{wait_time:.2f} seconds, which may be a performance "
                    "bottleneck.\n"
                    f"{_SLOW_SYNC_WARNING.format(threshold=self._slow_sync_threshold)}"
                )

        time_since_last_sync = (
            time.monotonic() - self._last_sync_time
            if self._last_sync_time is not None
            else None
        )
        launched_sync = self._storage.syncer.sync_up(
            driver_staging_path, self._storage.experiment_fs_path
        )
        if launched_sync:
            if (
                time_since_last_sync is not None
                and time_since_last_sync < self._excessive_sync_threshold
                and self._should_force_sync_up
            ):
                logger.warning(
                    "Experiment state snapshotting has been triggered multiple "
                    f"times in the last {self._excessive_sync_threshold} seconds "
                    "and may become a bottleneck. "
                    "A snapshot is forced if `CheckpointConfig(num_to_keep)` is set, "
                    "and a trial has checkpointed >= `num_to_keep` times "
                    "since the last snapshot.\n"
                    "You may want to consider increasing the "
                    "`CheckpointConfig(num_to_keep)` or decreasing the frequency of "
                    "saving checkpoints.\n"
                    "You can suppress this warning by setting the environment variable "
                    "TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S "
                    "to a smaller value than the current threshold "
                    f"({self._excessive_sync_threshold}). "
                    "Set it to 0 to completely suppress this warning."
                )

            self._last_sync_time = time.monotonic()

            # We just synced, so reset the force flag
            self._trial_num_checkpoints_since_last_sync.clear()
            self._should_force_sync_up = False
        else:
            if (
                time_since_last_sync is not None
                and time_since_last_sync > self._slow_sync_threshold
            ):
                logger.warning(
                    "Saving the experiment state (which holds a global view "
                    "of trial statuses and is used to restore the experiment) "
                    f"has already taken {time_since_last_sync:.2f} seconds, "
                    "which may cause consistency issues upon restoration if your "
                    "driver script ungracefully exits.\n"
                    f"{_SLOW_SYNC_WARNING.format(threshold=self._slow_sync_threshold)}"
                )

        if wait:
            wait_for_sync()

        checkpoint_time_taken = time.monotonic() - checkpoint_time_start

        # Adjust dynamic checkpointing
        self._update_auto_checkpoint_time(time_taken=checkpoint_time_taken)

        # Finish
        self._last_save_time = time.monotonic()

    def sync_down_experiment_state(self) -> None:
        fs = self._storage.storage_filesystem
        filepaths = _list_at_fs_path(fs=fs, fs_path=self._storage.experiment_fs_path)
        # TODO(ekl) we should refactor our restore code to read the necessary data
        # directly from the storage context. As a temporary hack, restore all the
        # serialized files from the root dir where other modules expect them to be.
        matches = [
            path
            for path in filepaths
            if path.endswith(".json") or path.endswith(".pkl")
        ]
        for relpath in matches:
            fs_path = Path(self._storage.experiment_fs_path, relpath).as_posix()
            local_path = Path(
                self._storage.experiment_driver_staging_path, relpath
            ).as_posix()
            _download_from_fs_path(fs=fs, fs_path=fs_path, local_path=local_path)
        logger.debug(
            f"Copied {matches} from:\n(fs, path) = "
            f"({self._storage.storage_filesystem.type_name}, "
            f"{self._storage.experiment_fs_path})\n"
            f"-> {self._storage.experiment_driver_staging_path}"
        )

    def on_trial_checkpoint(self, trial: Trial):
        if not self._sync_every_n_trial_checkpoints:
            return

        self._trial_num_checkpoints_since_last_sync[trial] += 1

        if (
            self._trial_num_checkpoints_since_last_sync[trial]
            >= self._sync_every_n_trial_checkpoints
        ):
            self._should_force_sync_up = True
