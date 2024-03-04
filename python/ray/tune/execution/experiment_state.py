from collections import Counter
import fnmatch
from pathlib import Path
from typing import Callable, Dict, Optional, Union
import logging
import os
import time

import pyarrow.fs

from ray.train._internal.storage import (
    StorageContext,
    get_fs_and_path,
    _download_from_fs_path,
    _list_at_fs_path,
    _upload_to_fs_path,
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

    If ``sync_every_n_trial_checkpoints`` is not None, syncing
    to cloud will be forced if any trial has checkpointed more times than
    ``sync_every_n_trial_checkpoints`` since last sync.

    """

    def __init__(
        self,
        *,
        storage: Optional[StorageContext],
        checkpoint_period: Union[int, float, str],
    ):
        self._storage = storage

        self._last_save_time = 0.0

        # Dynamic checkpointing period
        self._auto_checkpoint_enabled = checkpoint_period == "auto"
        if self._auto_checkpoint_enabled:
            self._checkpoint_period = 10.0  # Initial value
        else:
            self._checkpoint_period = float(checkpoint_period)

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
                f"Global experiment checkpointing took "
                f"{time_taken:.2f} seconds. "
                f"Adjusting checkpoint period to "
                f"{self._checkpoint_period:.2f} seconds."
            )

    def sync_up_experiment_state(
        self,
        save_fn: Callable[[], None],
        force: bool = False,
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
            force: Forces a checkpoint despite checkpoint_period.
        """
        driver_staging_path = self._storage.experiment_driver_staging_path

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

        _upload_to_fs_path(
            local_path=driver_staging_path,
            fs=self._storage.storage_filesystem,
            fs_path=self._storage.experiment_fs_path,
        )

        checkpoint_time_taken = time.monotonic() - checkpoint_time_start

        # Adjust dynamic checkpointing
        self._update_auto_checkpoint_time(time_taken=checkpoint_time_taken)

        if checkpoint_time_taken > self._slow_sync_threshold:
            logger.warning(
                "Checkpointing the experiment state (which holds a global view "
                "of trial statuses and is used to restore the experiment) took"
                f"{checkpoint_time_taken:.2f} seconds, which may be a bottleneck.\n"
                "This could be due to a large number of trials, "
                "large logfiles from lots of reported metrics, or throttling from the "
                "remote storage if uploading too frequently.\n"
                "You can increase the number of seconds between experiment syncs "
                "by setting the environment variable TUNE_GLOBAL_CHECKPOINT_S. "
                "For example, TUNE_GLOBAL_CHECKPOINT_S=60 will checkpoint trial "
                "states every 60 seconds."
            )

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
