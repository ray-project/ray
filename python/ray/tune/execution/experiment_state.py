from collections import Counter
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple, Union

import pyarrow
import click
import logging
import os
import time
import warnings

from ray.air._internal.remote_storage import list_at_uri
from ray.air._internal.uri_utils import _join_path_or_uri
from ray.train._internal.storage import _use_storage_context, StorageContext
from ray.tune.experiment import Trial
from ray.tune.impl.out_of_band_serialize_dataset import out_of_band_serialize_dataset
from ray.train._internal.syncer import SyncConfig, get_node_to_storage_syncer

logger = logging.getLogger(__name__)


VALID_RESUME_TYPES = [True, "LOCAL", "REMOTE", "PROMPT", "ERRORED_ONLY", "AUTO"]

_EXPERIMENT_SYNC_TIMEOUT_MESSAGE = (
    "If this warning keeps showing up, consider diagnosing the "
    "reason behind the hanging sync operation, or increase the "
    "`sync_timeout` in `SyncConfig`."
)

_DRIVER_SYNC_EXCLUDE_PATTERNS = ["*/checkpoint_*"]


@dataclass
class _ResumeConfig:
    resume_unfinished: bool = True
    resume_errored: bool = False
    restart_errored: bool = False


def _resume_str_to_config(resume_str: str) -> Tuple[str, _ResumeConfig]:
    if resume_str is True:
        resume_str = "LOCAL"
    elif resume_str == "ERRORED_ONLY":
        warnings.warn(
            "Passing `resume='ERRORED_ONLY'` to tune.run() is deprecated and "
            "will be removed in the future. Please pass e.g. "
            "`resume='LOCAL+RESTART_ERRORED_ONLY'` instead."
        )
        resume_str = "LOCAL+RESTART_ERRORED_ONLY"

    # Parse resume string, e.g. AUTO+ERRORED
    resume_config = _ResumeConfig()
    resume_settings = resume_str.split("+")
    resume_str = resume_settings[0]

    for setting in resume_settings:
        if setting == "ERRORED":
            resume_config.resume_errored = True
        elif setting == "RESTART_ERRORED":
            resume_config.restart_errored = True
        elif setting == "ERRORED_ONLY":
            resume_config.resume_unfinished = False
            resume_config.restart_errored = False
            resume_config.resume_errored = True
        elif setting == "RESTART_ERRORED_ONLY":
            resume_config.resume_unfinished = False
            resume_config.restart_errored = True
            resume_config.resume_errored = False

    assert resume_str in VALID_RESUME_TYPES, "resume={} is not one of {}".format(
        resume_str, VALID_RESUME_TYPES
    )
    return resume_str, resume_config


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

    def construct(file: str) -> str:
        return _join_path_or_uri(experiment_dir, file)

    candidate_paths = [
        construct(file)
        for file in list_at_uri(experiment_dir)
        if file.startswith("experiment_state") and file.endswith(".json")
    ]
    if not candidate_paths:
        return None

    return max(candidate_paths)


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
        checkpoint_period: Union[int, float, str],
        # TODO(justinvyu): Make `storage` required
        storage: Optional[StorageContext] = None,
        sync_every_n_trial_checkpoints: Optional[int] = None,
        # TODO(justinvyu): Remove these args.
        sync_config: SyncConfig = None,
        local_checkpoint_dir: str = None,
        remote_checkpoint_dir: str = None,
    ):
        self._storage = storage
        if _use_storage_context():
            assert storage

            self._legacy_local_checkpoint_dir = None
            self._legacy_remote_checkpoint_dir = None
            self._legacy_sync_config = None
            self._legacy_syncer = None
        else:
            # Checkpoint directories
            self._legacy_local_checkpoint_dir = local_checkpoint_dir
            self._legacy_remote_checkpoint_dir = remote_checkpoint_dir

            # Synch to/from cloud
            self._legacy_sync_config = sync_config or SyncConfig()
            # Resolves syncer="auto" to an actual syncer if needed
            self._legacy_syncer = get_node_to_storage_syncer(
                self._legacy_sync_config, self._legacy_remote_checkpoint_dir
            )

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
                "TUNE_WARN_EXCESSIVE_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S", "30"
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
        """Saves execution state to `self._legacy_local_checkpoint_dir`.

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
        experiment_local_path = (
            self._storage.experiment_local_path
            if _use_storage_context()
            else self._legacy_local_checkpoint_dir
        )
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
        # self._legacy_remote_checkpoint_dir can be empty in tests, but shouldn't
        # be empty when using in end-to-end tune.
        # Todo (krfricke): We may want to not store directories in this manager
        # but instead always pass them from the trial runner.
        syncer = self._storage.syncer if _use_storage_context() else self._legacy_syncer

        if not syncer:  # or not self._legacy_remote_checkpoint_dir:
            return False

        if _use_storage_context():
            # Always exclude checkpoints in the new persistence path.
            # TODO(justinvyu, krfricke): Ideally, this excludes all trial directories.
            # But for now, this is needed to upload driver artifacts that live in the
            # trial directory.
            exclude = _DRIVER_SYNC_EXCLUDE_PATTERNS
            experiment_local_path = self._storage.experiment_local_path
            experiment_fs_path = self._storage.experiment_fs_path
        else:
            if bool(self._legacy_remote_checkpoint_dir):
                # If an upload dir is given, trainable actors upload checkpoints
                # themselves. Then the driver does not need to sync checkpoints.
                exclude = _DRIVER_SYNC_EXCLUDE_PATTERNS
            else:
                # Otherwise, we sync the full trial dir.
                exclude = None
            experiment_local_path = self._legacy_local_checkpoint_dir
            experiment_fs_path = self._legacy_remote_checkpoint_dir

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
        file_infos = fs.get_file_info(
            pyarrow.fs.FileSelector(self._storage.experiment_fs_path)
        )
        # TODO(ekl) we should refactor our restore code to read the necessary data
        # directly from the storage context. As a temporary hack, restore all the
        # serialized files from the root dir where other modules expect them to be.
        matches = []
        for file_info in file_infos:
            name = os.path.basename(file_info.path)
            if name.endswith(".json") or name.endswith(".pkl"):
                matches.append(name)
        for name in matches:
            remote_path = os.path.join(self._storage.experiment_fs_path, name)
            local_path = os.path.join(self._storage.experiment_local_path, name)
            pyarrow.fs.copy_files(
                remote_path,
                local_path,
                source_filesystem=self._storage.storage_filesystem,
            )
        logger.debug(f"Copied {matches} from:\n{remote_path}\n-> {local_path}")

    def sync_down(self, force: bool = False, wait: bool = False) -> bool:
        assert not _use_storage_context()
        if not self._legacy_syncer or not self._legacy_remote_checkpoint_dir:
            return False

        if bool(self._legacy_remote_checkpoint_dir):
            # If an upload dir is given, trainable actors upload checkpoints
            # themselves. Then the driver does not need to sync checkpoints.
            exclude = ["*/checkpoint_*"]
        else:
            # Otherwise, we sync the full trial dir.
            exclude = None
        syncer = self._legacy_syncer
        experiment_local_path = self._legacy_local_checkpoint_dir
        experiment_fs_path = self._legacy_remote_checkpoint_dir

        if force:
            # Wait until previous sync command finished
            try:
                syncer.wait()
            except TimeoutError as e:
                logger.warning(
                    "The previous sync of the experiment directory from the cloud "
                    f"timed out with the error: {str(e)}\nSyncing will be retried. "
                    + _EXPERIMENT_SYNC_TIMEOUT_MESSAGE
                )
            except Exception as e:
                logger.warning(
                    "The previous sync of the experiment directory from the cloud "
                    f"failed with the error: {str(e)}\nSyncing will be retried. "
                )

            synced = syncer.sync_down(
                remote_dir=experiment_fs_path,
                local_dir=experiment_local_path,
                exclude=exclude,
            )
        else:
            synced = syncer.sync_down_if_needed(
                remote_dir=experiment_fs_path,
                local_dir=experiment_local_path,
                exclude=exclude,
            )

        if wait:
            try:
                syncer.wait()
            except Exception as e:
                raise RuntimeError(
                    "Downloading the remote experiment directory from the cloud "
                    f"(remote path: {experiment_fs_path}) to the driver "
                    f"(local path: {experiment_local_path}) failed. "
                    "Please check the error message above. "
                    "If you expected an experiment to already exist, check if "
                    "you supplied the correct restoration path."
                ) from e

        return synced

    def _resume_auto(self) -> bool:
        if _use_storage_context():
            experiment_local_path = self._storage.experiment_local_path
            experiment_fs_path = self._storage.experiment_fs_path
            syncer = self._storage.syncer
        else:
            experiment_local_path = self._legacy_local_checkpoint_dir
            experiment_fs_path = self._legacy_remote_checkpoint_dir
            syncer = self._legacy_syncer

        if experiment_fs_path and syncer:
            logger.info(
                f"Trying to find and download experiment checkpoint at "
                f"{experiment_fs_path}"
            )
            try:
                if _use_storage_context():
                    self.sync_down_experiment_state()
                else:
                    # Todo: This syncs the entire experiment including trial
                    # checkpoints. We should exclude these in the future.
                    syncer.sync_down_if_needed(
                        remote_dir=experiment_fs_path,
                        local_dir=experiment_local_path,
                    )
                    syncer.wait()
            except Exception:
                logger.exception(
                    "Got error when trying to sync down.\n"
                    "Please check this error message for potential "
                    "access problems - if a directory was not found, "
                    "that is expected at this stage when you're starting "
                    "a new experiment."
                )
                logger.info(
                    "No remote checkpoint was found or an error occurred "
                    "when trying to download the experiment checkpoint. "
                    "Please check the previous warning message for more "
                    "details. "
                    "Ray Tune will now start a new experiment."
                )
                return False
            if not _experiment_checkpoint_exists(experiment_local_path):
                logger.warning(
                    "A remote checkpoint was fetched, but no checkpoint "
                    "data was found. This can happen when e.g. the cloud "
                    "bucket exists but does not contain any data. "
                    "Ray Tune will start a new, fresh run."
                )
                return False
            logger.info(
                "A remote experiment checkpoint was found and will be "
                "used to restore the previous experiment state."
            )
            return True
        elif not _experiment_checkpoint_exists(experiment_local_path):
            logger.info(
                "No local checkpoint was found. "
                "Ray Tune will now start a new experiment."
            )
            return False
        logger.info(
            "A local experiment checkpoint was found and will be used "
            "to restore the previous experiment state."
        )
        return True

    def resume(self, resume_type: Union[str, bool]) -> Optional[_ResumeConfig]:
        """Checks whether to resume experiment.

        If experiment should be resumed, this method may sync down experiment state
        from the cloud and then return a ResumeConfig mapping to the resume type.

        Args:
            resume_type: One of ["REMOTE", "LOCAL", "PROMPT", "AUTO"]. Can
                be suffixed with one or more of ["+ERRORED", "+ERRORED_ONLY",
                "+RESTART_ERRORED", "+RESTART_ERRORED_ONLY"]

        Returns:
            _ResumeConfig if resume is successful. None otherwise.
        """
        if not resume_type:
            return None

        resume_type, resume_config = _resume_str_to_config(resume_type)

        if _use_storage_context():
            experiment_local_path = self._storage.experiment_local_path
            experiment_fs_path = self._storage.experiment_fs_path
        else:
            # Not clear if we need this assertion, since we should always have a
            # local checkpoint dir.
            assert self._legacy_local_checkpoint_dir or (
                self._legacy_remote_checkpoint_dir and self._legacy_syncer
            )
            experiment_local_path = self._legacy_local_checkpoint_dir
            experiment_fs_path = self._legacy_remote_checkpoint_dir

        if resume_type == "AUTO":
            if self._resume_auto():
                return resume_config
            # Else
            return None

        if resume_type in ["LOCAL", "PROMPT"]:
            if not _experiment_checkpoint_exists(experiment_local_path):
                raise ValueError(
                    f"You called resume ({resume_type}) when no checkpoint "
                    f"exists in local directory "
                    f"({experiment_local_path}). If you want to start "
                    f'a new experiment, use `resume="AUTO"` or '
                    f"`resume=None`. If you expected an experiment to "
                    f"already exist, check if you supplied the correct "
                    f"`local_dir` to `train.RunConfig()`."
                )
            elif resume_type == "PROMPT":
                if click.confirm(
                    f"Resume from local directory? " f"({experiment_local_path})"
                ):
                    return resume_config

        if resume_type in ["REMOTE", "PROMPT"]:
            if resume_type == "PROMPT" and not click.confirm(
                f"Try downloading from remote directory? " f"({experiment_fs_path})"
            ):
                return None
            if not experiment_fs_path or not self._legacy_syncer:
                raise ValueError(
                    "Called resume from remote without remote directory or "
                    "without valid syncer. "
                    "Fix this by passing a `SyncConfig` object with "
                    "`upload_dir` set to `Tuner(sync_config=...)`."
                )

            # Try syncing down the upload directory.
            logger.info(
                f"Downloading experiment checkpoint from " f"{experiment_fs_path}"
            )
            if _use_storage_context():
                self.sync_down_experiment_state()
            else:
                self.sync_down(force=True, wait=True)

            if not _experiment_checkpoint_exists(experiment_local_path):
                raise ValueError(
                    "Called resume when no checkpoint exists "
                    "in remote or local directory."
                )
        return resume_config
