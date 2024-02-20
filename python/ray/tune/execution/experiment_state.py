from collections import Counter
from pathlib import Path
from typing import Callable, Dict, Optional, Union

import logging
import os
import time

from ray.train._internal.storage import (
    StorageContext,
    get_fs_and_path,
    _download_from_fs_path,
    _list_at_fs_path,
)
from ray.tune.experiment import Trial
from ray.tune.impl.out_of_band_serialize_dataset import out_of_band_serialize_dataset

logger = logging.getLogger(__name__)


_EXPERIMENT_SYNC_TIMEOUT_MESSAGE = (
    "If this warning keeps showing up, consider diagnosing the "
    "reason behind the hanging sync operation, or increase the "
    "`sync_timeout` in `SyncConfig`."
)

_DRIVER_SYNC_EXCLUDE_PATTERNS = ["*/checkpoint_*"]


def _experiment_checkpoint_exists(experiment_dir: str) -> bool:
    return bool(_find_newest_experiment_checkpoint(experiment_dir=experiment_dir))


def _find_newest_experiment_checkpoint(experiment_dir: str) -> Optional[str]:
    """Returns file name of most recently created experiment checkpoint.

    Args:
        experiment_dir: Local or remote path to the experiment directory
            containing at least one experiment checkpoint file.

    Returns:
        str: The local or remote path to the latest experiment checkpoint file
            based on timestamp. None if no experiment checkpoints were found.
    """
    from ray.tune.analysis import ExperimentAnalysis

    fs, path = get_fs_and_path(experiment_dir)
    return ExperimentAnalysis._find_newest_experiment_checkpoint(
        fs=fs, experiment_fs_path=path
    )


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

    If ``sync_every_n_trial_checkpoints`` is not None, syncing
    to cloud will be forced if any trial has checkpointed more times than
    ``sync_every_n_trial_checkpoints`` since last sync.

    """

    def __init__(
        self,
        *,
        storage: Optional[StorageContext],
        checkpoint_period: Union[int, float, str],
        sync_every_n_trial_checkpoints: Optional[int] = None,
    ):
        self._storage = storage

        # Last save + sync time
        self._last_save_time = 0.0
        self._last_sync_time = 0.0

        # Dynamic checkpointing period
        self._auto_checkpoint_enabled = checkpoint_period == "auto"
        if self._auto_checkpoint_enabled:
            self._checkpoint_period = 10.0  # Initial value
        else:
            self._checkpoint_period = float(checkpoint_period)

        # Upload triggered by trial checkpoints
        self._sync_every_n_trial_checkpoints = sync_every_n_trial_checkpoints
        self._trial_num_checkpoints_since_last_sync: Dict[Trial, int] = Counter()

        self._slow_sync_threshold = float(
            os.environ.get(
                "TUNE_WARN_SLOW_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S", "30"
            )
        )

        self._excessive_sync_threshold = float(
            os.environ.get(
                "TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S", "5"
            )
        )
        self._should_force_cloud_sync = False

    @property
    def auto_checkpoint_enabled(self):
        return self._auto_checkpoint_enabled

    def _update_auto_checkpoint_time(self, time_taken: float):
        if self._auto_checkpoint_enabled:
            # Multiplying this time by 19 means we spend ~5% of the time
            # writing global checkpoints and 95% of the time processing trials
            self._checkpoint_period = max(10.0, time_taken * 19)
            logger.debug(
                f"Global experiment checkpointing took "
                f"{time_taken:.2f} seconds. "
                f"Adjusting checkpoint period to "
                f"{self._checkpoint_period:.2f} seconds."
            )

    def on_trial_checkpoint(self, trial: Trial):
        if not self._sync_every_n_trial_checkpoints:
            return

        self._trial_num_checkpoints_since_last_sync[trial] += 1

        if (
            self._trial_num_checkpoints_since_last_sync[trial]
            >= self._sync_every_n_trial_checkpoints
        ):
            self._should_force_cloud_sync = True

    def checkpoint(
        self,
        save_fn: Callable[[], None],
        force: bool = False,
        wait: bool = False,
    ):
        """Saves execution state to the local experiment directory.
        Overwrites the current session checkpoint, which starts when self
        is instantiated. Throttle depends on self._checkpoint_period.

        Also, automatically saves the search algorithm to the local
        checkpoint dir.

        Args:
            save_fn: Function to call to actually save data. Should expect
                one string argument specifying the directory to save to.
            force: Forces a checkpoint despite checkpoint_period.
            wait: Wait until sync to cloud has finished.

        """
        experiment_local_path = self._storage.experiment_local_path
        if not experiment_local_path:
            return

        force = force or self._should_force_cloud_sync

        now = time.time()
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

        # Sync to cloud
        self.sync_up(force=force, wait=wait)

        checkpoint_time_taken = time.monotonic() - checkpoint_time_start

        # Adjust dynamic checkpointing
        self._update_auto_checkpoint_time(time_taken=checkpoint_time_taken)

        # Finish
        self._last_save_time = time.time()
        return experiment_local_path

    def sync_up(self, force: bool = False, wait: bool = False) -> bool:
        syncer = self._storage.syncer

        if not syncer:
            return False

        # Always exclude checkpoints in the new persistence path.
        # TODO(justinvyu, krfricke): Ideally, this excludes all trial directories.
        # But for now, this is needed to upload driver artifacts that live in the
        # trial directory.
        exclude = _DRIVER_SYNC_EXCLUDE_PATTERNS
        experiment_local_path = self._storage.experiment_local_path
        experiment_fs_path = self._storage.experiment_fs_path

        if force:
            # Wait until previous sync command finished
            try:
                syncer.wait()
            except TimeoutError as e:
                logger.warning(
                    "The previous sync of the experiment directory to the cloud "
                    f"timed out with the error: {str(e)}\nSyncing will be retried. "
                    + _EXPERIMENT_SYNC_TIMEOUT_MESSAGE
                )
            except Exception as e:
                logger.warning(
                    "The previous sync of the experiment directory to the cloud "
                    f"failed with the error: {str(e)}\nSyncing will be retried."
                )
            synced = syncer.sync_up(
                local_dir=experiment_local_path,
                remote_dir=experiment_fs_path,
                exclude=exclude,
            )
        else:
            synced = syncer.sync_up_if_needed(
                local_dir=experiment_local_path,
                remote_dir=experiment_fs_path,
                exclude=exclude,
            )

        start_time = time.monotonic()
        if wait:
            try:
                syncer.wait()
            except Exception as e:
                raise RuntimeError(
                    "Uploading the experiment directory from the driver "
                    f"(local path: {experiment_local_path}) to the the cloud "
                    f"(remote path: {experiment_fs_path}) failed. "
                    "Please check the error message above."
                ) from e

        now = time.monotonic()
        sync_time_taken = now - start_time

        if sync_time_taken > self._slow_sync_threshold:
            try:
                import fsspec
            except Exception:
                fsspec = None

            fsspec_msg = ""
            if fsspec is None:
                fsspec_msg = (
                    "If your data is small, try installing fsspec "
                    "(`pip install fsspec`) for more efficient local file parsing. "
                )

            logger.warning(
                "Syncing the experiment checkpoint to cloud took a long time with "
                f"{sync_time_taken:.2f} seconds. This can be due to a large number "
                f"of trials, large logfiles, or throttling from the "
                f"remote storage provider for too frequent syncs. {fsspec_msg}"
                f"If your `CheckpointConfig.num_to_keep` is a low number, this can "
                f"trigger frequent syncing, in which case you should increase it. "
            )

        if not synced:
            return False

        self._should_force_cloud_sync = False
        self._trial_num_checkpoints_since_last_sync.clear()

        if now - self._last_sync_time < self._excessive_sync_threshold:
            logger.warning(
                "Experiment checkpoint syncing has been triggered multiple "
                f"times in the last {self._excessive_sync_threshold} seconds. "
                "A sync will be triggered whenever a trial has checkpointed "
                "more than `num_to_keep` times since last sync or if "
                f"{syncer.sync_period} seconds have passed since last "
                "sync. If you have set `num_to_keep` in your `CheckpointConfig`, "
                "consider increasing the checkpoint frequency or keeping more "
                "checkpoints. You can supress this warning by changing the "
                "`TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S` "
                "environment variable."
            )
        self._last_sync_time = now
        return True

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
            local_path = Path(self._storage.experiment_local_path, relpath).as_posix()
            _download_from_fs_path(fs=fs, fs_path=fs_path, local_path=local_path)
        logger.debug(
            f"Copied {matches} from:\n(fs, path) = "
            f"({self._storage.storage_filesystem.type_name}, "
            f"{self._storage.experiment_fs_path})\n"
            f"-> {self._storage.experiment_local_path}"
        )

    def resume(self) -> bool:
        """Checks whether to resume experiment.

        The experiment can be resumed if a metadata file uploaded from a
        previous run can be found at the specified experiment directory on storage.
        If experiment should be resumed, this method will pull the necessary
        experiment state from storage.

        Returns:
            can_restore: Whether the experiment can be restored.
        """
        experiment_local_path = self._storage.experiment_local_path
        experiment_fs_path = self._storage.experiment_fs_path

        syncer = self._storage.syncer

        # syncer is not None when the local path != storage path
        if syncer:
            logger.info(
                f"Trying to find and download experiment checkpoint from: "
                f"{experiment_fs_path}"
            )
            try:
                self.sync_down_experiment_state()
            except Exception:
                logger.exception(
                    "Got error when trying to sync down experiment state from "
                    f"{experiment_fs_path}\n"
                    "Please check this error message for potential "
                    "access problems - if a directory was not found, "
                    "that is expected at this stage when you're starting "
                    "a new experiment."
                )
                return False

        latest_experiment_checkpoint_path = _find_newest_experiment_checkpoint(
            experiment_local_path
        )
        if latest_experiment_checkpoint_path is None:
            logger.warning(
                f"No experiment metadata was found at {experiment_fs_path}. "
                "Starting a new run..."
            )
            return False

        logger.info(
            f"The run will now start from the experiment state found in: "
            f"{latest_experiment_checkpoint_path}"
        )
        return True
