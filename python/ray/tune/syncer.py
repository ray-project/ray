import abc
import threading
from typing import (
    Callable,
    Dict,
    List,
    TYPE_CHECKING,
    Union,
    Optional,
)

import logging
import os
import time
from dataclasses import dataclass

import ray
from ray.air._internal.remote_storage import (
    get_fs_and_path,
    fs_hint,
    upload_to_uri,
    download_from_uri,
    delete_at_uri,
)
from ray.tune import TuneError
from ray.tune.callback import Callback
from ray.tune.checkpoint_manager import _TuneCheckpoint
from ray.tune.result import NODE_IP
from ray.tune.utils.file_transfer import sync_dir_between_nodes
from ray.util.annotations import PublicAPI, DeveloperAPI

if TYPE_CHECKING:
    from ray.tune.trial import Trial

logger = logging.getLogger(__name__)

# Syncing period for syncing checkpoints between nodes or to cloud.
DEFAULT_SYNC_PERIOD = 300

CLOUD_CHECKPOINTING_URL = (
    "https://docs.ray.io/en/master/tune/user-guide.html#using-cloud-storage"
)
_log_sync_warned = False
_syncers = {}


def validate_upload_dir(sync_config: "SyncConfig"):
    if sync_config.upload_dir:
        exc = None
        try:
            fs, _ = get_fs_and_path(sync_config.upload_dir)
        except ImportError as e:
            fs = None
            exc = e
        if not fs:
            raise ValueError(
                f"Could not identify external storage filesystem for "
                f"upload dir `{sync_config.upload_dir}`. "
                f"Hint: {fs_hint(sync_config.upload_dir)}"
            ) from exc


@PublicAPI
@dataclass
class SyncConfig:
    """Configuration object for syncing.

    If an ``upload_dir`` is specified, both experiment and trial checkpoints
    will be stored on remote (cloud) storage. Synchronization then only
    happens via this remote storage.

    Args:
        upload_dir: Optional URI to sync training results and checkpoints
            to (e.g. ``s3://bucket``, ``gs://bucket`` or ``hdfs://path``).
            Specifying this will enable cloud-based checkpointing.
        sync_process: Syncer class to use for synchronizing checkpoints to/from
            cloud storage. If set to ``None``, no syncing will take place.
            Defaults to ``"auto"`` (auto detect).
        sync_on_checkpoint: Force sync-down of trial checkpoint to
            driver (only non cloud-storage).
            If set to False, checkpoint syncing from worker to driver
            is asynchronous and best-effort. This does not affect persistent
            storage syncing. Defaults to True.
        sync_period: Syncing period for syncing between nodes.

    """

    upload_dir: Optional[str] = None
    sync_process: Optional[Union[str, "Syncer"]] = "auto"

    sync_on_checkpoint: bool = True
    sync_period: int = DEFAULT_SYNC_PERIOD


@DeveloperAPI
class Syncer(abc.ABC):
    def __init__(self, sync_period: float = 300.0):
        self.sync_period = sync_period
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")

    @abc.abstractmethod
    def sync_up(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def sync_down(
        self, remote_dir: str, local_dir: str, exclude: Optional[List] = None
    ) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, remote_dir: str) -> bool:
        raise NotImplementedError

    def sync_up_if_needed(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        """Syncs up if time since last sync up is greater than sync_period.

        Args:
            sync_period: Time period between subsequent syncs.
            exclude: Regex pattern of files to exclude, e.g.
                ``[".*/checkpoint_.*]`` to exclude trial checkpoints.
        """
        if time.time() - self.last_sync_up_time > self.sync_period:
            result = self.sync_up(
                local_dir=local_dir, remote_dir=remote_dir, exclude=exclude
            )
            self.last_sync_up_time = time.time()
            return result

    def sync_down_if_needed(
        self, remote_dir: str, local_dir: str, exclude: Optional[List] = None
    ):
        """Syncs down if time since last sync down is greater than sync_period.

        Args:
            sync_period: Time period between subsequent syncs.
            exclude: Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.
        """
        if time.time() - self.last_sync_down_time > self.sync_period:
            result = self.sync_down(
                remote_dir=remote_dir, local_dir=local_dir, exclude=exclude
            )
            self.last_sync_down_time = time.time()
            return result

    def wait_or_retry(self, max_retries: int = 3, backoff_s: int = 5):
        return self.wait()

    def wait(self):
        pass

    def reset(self):
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")

    def close(self):
        pass


class _BackgroundProcess:
    def __init__(self, fn: Callable):
        self._fn = fn
        self._process = None

    @property
    def is_running(self):
        return bool(self._process)

    def start(self, *args, **kwargs):
        if self.is_running:
            return False

        self._process = threading.Thread(target=self._fn, args=args, kwargs=kwargs)
        self._process.start()

    def wait(self):
        if not self._process:
            return True

        self._process.join()
        return True


class _DefaultSyncer(Syncer):
    def __init__(self, sync_period: float = 300.0):
        super(_DefaultSyncer, self).__init__(sync_period=sync_period)
        self._sync_process = None
        self._current_command = None

    def sync_up(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        if self._sync_process:
            return False

        self._current_command = (
            upload_to_uri,
            dict(local_path=local_dir, uri=remote_dir, exclude=exclude),
        )
        self.retry()
        return True

    def sync_down(
        self, remote_dir: str, local_dir: str, exclude: Optional[List] = None
    ) -> bool:
        if self._sync_process:
            return False

        self._current_command = (
            download_from_uri,
            dict(uri=remote_dir, local_path=remote_dir),
        )
        self.retry()

        return True

    def delete(self, remote_dir: str) -> bool:
        if self._sync_process:
            return False

        self._current_command = (delete_at_uri, dict(uri=remote_dir))
        self.retry()
        return True

    def retry(self):
        if not self._current_command:
            return

        cmd, kwargs = self._current_command

        self._sync_process = _BackgroundProcess(cmd)
        self._sync_process.start(**kwargs)

    def wait_or_retry(self, max_retries: int = 3, backoff_s: int = 5):
        assert max_retries > 0
        for _ in range(max_retries - 1):
            try:
                self.wait()
            except TuneError as e:
                logger.error(
                    f"Caught sync error: {e}. "
                    f"Retrying after sleeping for {backoff_s} seconds..."
                )
                time.sleep(backoff_s)
                self.retry()
                continue
            self._current_command = None
            return
        self._current_command = None
        raise TuneError(f"Failed sync even after {max_retries} retries.")

    def wait(self):
        if self._sync_process:
            self._sync_process.wait()
            self._sync_process = None
            self._current_command = None


def get_node_to_storage_syncer(sync_config: SyncConfig):
    return _DefaultSyncer(sync_period=sync_config.sync_period)


@DeveloperAPI
class SyncerCallback(Callback):
    def __init__(self, enabled: bool = True, sync_period: float = DEFAULT_SYNC_PERIOD):
        self._enabled = enabled
        self._sync_processes: Dict[str, _BackgroundProcess] = {}
        self._sync_times: Dict[str, float] = {}
        self._sync_period = sync_period

    def _get_trial_sync_process(self, trial: "Trial"):
        return self._sync_processes.setdefault(
            trial.trial_id, _BackgroundProcess(sync_dir_between_nodes)
        )

    def _remove_trial_sync_process(self, trial: "Trial"):
        self._sync_processes.pop(trial.trial_id, None)

    def _should_sync(self, trial: "Trial"):
        last_sync_time = self._sync_times.setdefault(trial.trial_id, float("-inf"))
        return time.time() - last_sync_time > self._sync_period

    def _mark_as_synced(self, trial: "Trial"):
        self._sync_times[trial.trial_id] = time.time()

    def _sync_trial_dir(
        self, trial: "Trial", force: bool = False, wait: bool = True
    ) -> bool:
        if not self._enabled or trial.uses_cloud_checkpointing:
            return False

        sync_process = self._get_trial_sync_process(trial)

        if not force and not self._should_sync(trial):
            return False

        if NODE_IP in trial.last_result:
            source_ip = trial.last_result[NODE_IP]
        else:
            source_ip = ray.get(trial.runner.get_current_ip.remote())

        try:
            sync_process.wait()
            sync_process.start(
                source_ip=source_ip,
                source_path=trial.logdir,
                target_ip=ray.util.get_node_ip_address(),
                target_path=trial.logdir,
            )
            if wait:
                sync_process.wait()
        except TuneError as e:
            # Errors occurring during this wait are not fatal for this
            # checkpoint, so it should just be logged.
            logger.error(
                f"Trial {trial}: An error occurred during the "
                f"checkpoint syncing: {e}"
            )

    def on_trial_result(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        result: Dict,
        **info,
    ):
        self._sync_trial_dir(trial, force=False, wait=False)

    def on_trial_complete(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self._sync_trial_dir(trial, force=True, wait=True)
        self._remove_trial_sync_process(trial)

    def on_checkpoint(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        checkpoint: _TuneCheckpoint,
        **info,
    ):
        if checkpoint.storage == _TuneCheckpoint.MEMORY:
            return
        self._sync_trial_dir(trial, force=trial.sync_on_checkpoint, wait=True)

        if trial.uses_cloud_checkpointing:
            return

        if not os.path.exists(checkpoint.value):
            raise TuneError(
                f"Trial {trial}: Checkpoint path {checkpoint.value} not "
                "found after successful sync down. "
                "Are you running on a Kubernetes or "
                "managed cluster? rsync will not function "
                "due to a lack of SSH functionality. "
                "You'll need to use cloud-checkpointing "
                "if that's the case, see instructions "
                f"here: {CLOUD_CHECKPOINTING_URL}."
            )
