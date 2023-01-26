import abc
from functools import partial
import threading
from typing import (
    Any,
    Callable,
    Dict,
    List,
    TYPE_CHECKING,
    Union,
    Optional,
    Tuple,
)

import logging
import os
import time
from dataclasses import dataclass

import ray
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.air._internal.remote_storage import (
    fs_hint,
    upload_to_uri,
    download_from_uri,
    delete_at_uri,
    is_non_local_path_uri,
)
from ray.exceptions import RayActorError
from ray.tune import TuneError
from ray.tune.callback import Callback
from ray.tune.utils.file_transfer import sync_dir_between_nodes
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.widgets import Template

if TYPE_CHECKING:
    from ray.tune.experiment import Trial

logger = logging.getLogger(__name__)

# Syncing period for syncing checkpoints between nodes or to cloud.
DEFAULT_SYNC_PERIOD = 300

# Default sync timeout after which syncing processes are aborted
DEFAULT_SYNC_TIMEOUT = 1800

_EXCLUDE_FROM_SYNC = [
    "./checkpoint_-00001",
    "./checkpoint_tmp*",
    "./save_to_object*",
    "./rank_*",
]


@PublicAPI
@dataclass
class SyncConfig:
    """Configuration object for Tune syncing.

    See :ref:`tune-persisted-experiment-data` for an overview of what data is
    synchronized.

    If an ``upload_dir`` is specified, both experiment and trial checkpoints
    will be stored on remote (cloud) storage. Synchronization then only
    happens via uploading/downloading from this remote storage -- no syncing will
    happen between nodes.

    There are a few scenarios where syncing takes place:

    (1) The Tune driver (on the head node) syncing the experiment directory to the cloud
        (which includes experiment state such as searcher state, the list of trials
        and their statuses, and trial metadata)
    (2) Workers directly syncing trial checkpoints to the cloud
    (3) Workers syncing their trial directories to the head node
        (this is the default option when no cloud storage is used)

    See :ref:`tune-storage-options` for more details and examples.

    Args:
        upload_dir: Optional URI to sync training results and checkpoints
            to (e.g. ``s3://bucket``, ``gs://bucket`` or ``hdfs://path``).
            Specifying this will enable cloud-based checkpointing.
        syncer: If ``upload_dir`` is specified, then this config accepts a custom
            syncer subclassing :class:`~ray.tune.syncer.Syncer` which will be
            used to synchronize checkpoints to/from cloud storage.
            If no ``upload_dir`` is specified, this config can be set to ``None``,
            which disables the default worker-to-head-node syncing.
            Defaults to ``"auto"`` (auto detect), which assigns a default syncer
            that uses pyarrow to handle cloud storage syncing when ``upload_dir``
            is provided.
        sync_period: Minimum time in seconds to wait between two sync operations.
            A smaller ``sync_period`` will have more up-to-date data at the sync
            location but introduces more syncing overhead.
            Defaults to 5 minutes.
            **Note**: This applies to (1) and (3). Trial checkpoints are uploaded
            to the cloud synchronously on every checkpoint.
        sync_timeout: Maximum time in seconds to wait for a sync process
            to finish running. This is used to catch hanging sync operations
            so that experiment execution can continue and the syncs can be retried.
            Defaults to 30 minutes.
            **Note**: Currently, this timeout only affects cloud syncing: (1) and (2).
        sync_on_checkpoint: If *True*, a sync from a worker's remote trial directory
            to the head node will be forced on every trial checkpoint, regardless
            of the ``sync_period``.
            Defaults to True.
            **Note**: This is ignored if ``upload_dir`` is specified, since this
            only applies to worker-to-head-node syncing (3).
    """

    upload_dir: Optional[str] = None
    syncer: Optional[Union[str, "Syncer"]] = "auto"
    sync_period: int = DEFAULT_SYNC_PERIOD
    sync_timeout: int = DEFAULT_SYNC_TIMEOUT

    sync_on_checkpoint: bool = True

    def __post_init__(self):
        if self.upload_dir and self.syncer is None:
            raise ValueError(
                "`upload_dir` enables syncing to cloud storage, but `syncer=None` "
                "disables syncing. Either remove the `upload_dir`, "
                "or set `syncer` to 'auto' or a custom syncer."
            )
        if not self.upload_dir and isinstance(self.syncer, Syncer):
            raise ValueError("Must specify an `upload_dir` to use a custom `syncer`.")

    def _repr_html_(self) -> str:
        """Generate an HTML representation of the SyncConfig.

        Note that self.syncer is omitted here; seems to have some overlap
        with existing configuration settings here in the SyncConfig class.
        """
        try:
            from tabulate import tabulate
        except ImportError:
            return (
                "Tabulate isn't installed. Run "
                "`pip install tabulate` for rich notebook output."
            )

        return Template("scrollableTable.html.j2").render(
            table=tabulate(
                {
                    "Setting": [
                        "Upload directory",
                        "Sync on checkpoint",
                        "Sync period",
                    ],
                    "Value": [
                        self.upload_dir,
                        self.sync_on_checkpoint,
                        self.sync_period,
                    ],
                },
                tablefmt="html",
                showindex=False,
                headers="keys",
            ),
            max_height="none",
        )

    def validate_upload_dir(self) -> bool:
        """Checks if ``upload_dir`` is supported by ``syncer``.

        Returns True if ``upload_dir`` is valid, otherwise raises
        ``ValueError``.

        Args:
            upload_dir: Path to validate.
        """
        if isinstance(self.syncer, Syncer):
            return self.syncer.validate_upload_dir(self.upload_dir)
        else:
            return Syncer.validate_upload_dir(self.upload_dir)


class _BackgroundProcess:
    def __init__(self, fn: Callable):
        self._fn = fn
        self._process = None
        self._result = {}
        self._start_time = float("-inf")

    @property
    def is_running(self):
        return self._process and self._process.is_alive()

    @property
    def start_time(self):
        return self._start_time

    def start(self, *args, **kwargs):
        if self.is_running:
            return False

        self._result = {}

        def entrypoint():
            try:
                result = self._fn(*args, **kwargs)
            except Exception as e:
                self._result["exception"] = e
                return

            self._result["result"] = result

        self._process = threading.Thread(target=entrypoint)
        self._process.daemon = True
        self._process.start()
        self._start_time = time.time()

    def wait(self, timeout: Optional[float] = None) -> Any:
        """Waits for the backgrond process to finish running. Waits until the
        background process has run for at least `timeout` seconds, counting from
        the time when the process was started."""
        if not self._process:
            return None

        time_remaining = None
        if timeout:
            elapsed = time.time() - self.start_time
            time_remaining = max(timeout - elapsed, 0)

        self._process.join(timeout=time_remaining)

        if self._process.is_alive():
            self._process = None
            raise TimeoutError(
                f"{getattr(self._fn, '__name__', str(self._fn))} did not finish "
                f"running within the timeout of {timeout} seconds."
            )

        self._process = None

        exception = self._result.get("exception")
        if exception:
            raise exception

        result = self._result.get("result")

        self._result = {}
        return result


@DeveloperAPI
class Syncer(abc.ABC):
    """Syncer class for synchronizing data between Ray nodes and remote (cloud) storage.

    This class handles data transfer for two cases:

    1. Synchronizing data such as experiment checkpoints from the driver to
       cloud storage.
    2. Synchronizing data such as trial checkpoints from remote trainables to
       cloud storage.

    Synchronizing tasks are usually asynchronous and can be awaited using ``wait()``.
    The base class implements a ``wait_or_retry()`` API that will retry a failed
    sync command.

    The base class also exposes an API to only kick off syncs every ``sync_period``
    seconds.

    Args:
        sync_period: The minimum time in seconds between sync operations, as
            used by ``sync_up/down_if_needed``.
        sync_timeout: The maximum time to wait for a sync process to finish before
            issuing a new sync operation. Ex: should be used by ``wait`` if launching
            asynchronous sync tasks.
    """

    def __init__(
        self,
        sync_period: float = DEFAULT_SYNC_PERIOD,
        sync_timeout: float = DEFAULT_SYNC_TIMEOUT,
    ):
        self.sync_period = sync_period
        self.sync_timeout = sync_timeout
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")

    @abc.abstractmethod
    def sync_up(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        """Synchronize local directory to remote directory.

        This function can spawn an asynchronous process that can be awaited in
        ``wait()``.

        Args:
            local_dir: Local directory to sync from.
            remote_dir: Remote directory to sync up to. This is an URI
                (``protocol://remote/path``).
            exclude: Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.

        Returns:
            True if sync process has been spawned, False otherwise.

        """
        raise NotImplementedError

    @abc.abstractmethod
    def sync_down(
        self, remote_dir: str, local_dir: str, exclude: Optional[List] = None
    ) -> bool:
        """Synchronize remote directory to local directory.

        This function can spawn an asynchronous process that can be awaited in
        ``wait()``.

        Args:
            remote_dir: Remote directory to sync down from. This is an URI
                (``protocol://remote/path``).
            local_dir: Local directory to sync to.
            exclude: Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.

        Returns:
            True if sync process has been spawned, False otherwise.

        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, remote_dir: str) -> bool:
        """Delete directory on remote storage.

        This function can spawn an asynchronous process that can be awaited in
        ``wait()``.

        Args:
            remote_dir: Remote directory to delete. This is an URI
                (``protocol://remote/path``).

        Returns:
            True if sync process has been spawned, False otherwise.

        """
        raise NotImplementedError

    def retry(self):
        """Retry the last sync up, sync down, or delete command.

        You should implement this method if you spawn asynchronous syncing
        processes.
        """
        pass

    def wait(self):
        """Wait for asynchronous sync command to finish.

        You should implement this method if you spawn asynchronous syncing
        processes. This method should timeout after the asynchronous command
        has run for `sync_timeout` seconds and raise a `TimeoutError`.
        """
        pass

    def sync_up_if_needed(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        """Syncs up if time since last sync up is greater than sync_period.

        Args:
            local_dir: Local directory to sync from.
            remote_dir: Remote directory to sync up to. This is an URI
                (``protocol://remote/path``).
            exclude: Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.
        """
        now = time.time()
        if now - self.last_sync_up_time >= self.sync_period:
            result = self.sync_up(
                local_dir=local_dir, remote_dir=remote_dir, exclude=exclude
            )
            self.last_sync_up_time = now
            return result

    def sync_down_if_needed(
        self, remote_dir: str, local_dir: str, exclude: Optional[List] = None
    ):
        """Syncs down if time since last sync down is greater than sync_period.

        Args:
            remote_dir: Remote directory to sync down from. This is an URI
                (``protocol://remote/path``).
            local_dir: Local directory to sync to.
            exclude: Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.
        """
        now = time.time()
        if now - self.last_sync_down_time >= self.sync_period:
            result = self.sync_down(
                remote_dir=remote_dir, local_dir=local_dir, exclude=exclude
            )
            self.last_sync_down_time = now
            return result

    def wait_or_retry(self, max_retries: int = 3, backoff_s: int = 5):
        assert max_retries > 0
        last_error = None
        for _ in range(max_retries - 1):
            try:
                self.wait()
            except Exception as e:
                logger.error(
                    f"Caught sync error: {e}. "
                    f"Retrying after sleeping for {backoff_s} seconds..."
                )
                last_error = e
                time.sleep(backoff_s)
                self.retry()
                continue
            return
        raise TuneError(
            f"Failed sync even after {max_retries} retries."
        ) from last_error

    def reset(self):
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")

    def close(self):
        pass

    def _repr_html_(self) -> str:
        return

    @classmethod
    def validate_upload_dir(cls, upload_dir: str) -> bool:
        """Checks if ``upload_dir`` is supported by the Syncer.

        Returns True if ``upload_dir`` is valid, otherwise raises
        ``ValueError``.

        Args:
            upload_dir: Path to validate.
        """
        if not upload_dir:
            return True

        if upload_dir.startswith("file://"):
            return True

        if not is_non_local_path_uri(upload_dir):
            raise ValueError(
                f"Could not identify external storage filesystem for "
                f"upload dir `{upload_dir}`. "
                f"Hint: {fs_hint(upload_dir)}"
            )


class _BackgroundSyncer(Syncer):
    """Syncer using a background process for asynchronous file transfer."""

    def __init__(
        self,
        sync_period: float = DEFAULT_SYNC_PERIOD,
        sync_timeout: float = DEFAULT_SYNC_TIMEOUT,
    ):
        super(_BackgroundSyncer, self).__init__(
            sync_period=sync_period, sync_timeout=sync_timeout
        )
        self._sync_process = None
        self._current_cmd = None

    def _should_continue_existing_sync(self):
        """Returns whether a previous sync is still running within the timeout."""
        return (
            self._sync_process
            and self._sync_process.is_running
            and time.time() - self._sync_process.start_time < self.sync_timeout
        )

    def sync_up(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        if self._should_continue_existing_sync():
            logger.warning(
                f"Last sync still in progress, "
                f"skipping sync up of {local_dir} to {remote_dir}"
            )
            return False
        elif self._sync_process:
            try:
                self.wait()
            except Exception as e:
                logger.warning(f"Last sync command failed: {e}")

        self._current_cmd = self._sync_up_command(
            local_path=local_dir, uri=remote_dir, exclude=exclude
        )
        self.retry()

        return True

    def _sync_up_command(
        self, local_path: str, uri: str, exclude: Optional[List] = None
    ) -> Tuple[Callable, Dict]:
        raise NotImplementedError

    def sync_down(
        self, remote_dir: str, local_dir: str, exclude: Optional[List] = None
    ) -> bool:
        if self._should_continue_existing_sync():
            logger.warning(
                f"Last sync still in progress, "
                f"skipping sync down of {remote_dir} to {local_dir}"
            )
            return False
        elif self._sync_process:
            try:
                self.wait()
            except Exception as e:
                logger.warning(f"Last sync command failed: {e}")

        self._current_cmd = self._sync_down_command(
            uri=remote_dir, local_path=local_dir
        )
        self.retry()

        return True

    def _sync_down_command(self, uri: str, local_path: str) -> Tuple[Callable, Dict]:
        raise NotImplementedError

    def delete(self, remote_dir: str) -> bool:
        if self._should_continue_existing_sync():
            logger.warning(
                f"Last sync still in progress, skipping deletion of {remote_dir}"
            )
            return False
        elif self._sync_process:
            try:
                self.wait()
            except Exception as e:
                logger.warning(f"Last sync command failed: {e}")

        self._current_cmd = self._delete_command(uri=remote_dir)
        self.retry()

        return True

    def _delete_command(self, uri: str) -> Tuple[Callable, Dict]:
        raise NotImplementedError

    def wait(self):
        if self._sync_process:
            try:
                self._sync_process.wait(timeout=self.sync_timeout)
            except Exception as e:
                # Let `TimeoutError` pass through, to be handled separately
                # from errors thrown by the sync operation
                if isinstance(e, TimeoutError):
                    raise e
                raise TuneError(f"Sync process failed: {e}") from e
            finally:
                self._sync_process = None

    def retry(self):
        if not self._current_cmd:
            raise TuneError("No sync command set, cannot retry.")
        cmd, kwargs = self._current_cmd
        self._sync_process = _BackgroundProcess(cmd)
        self._sync_process.start(**kwargs)

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_sync_process"] = None
        return state


class _DefaultSyncer(_BackgroundSyncer):
    """Default syncer between local storage and remote URI."""

    def _sync_up_command(
        self, local_path: str, uri: str, exclude: Optional[List] = None
    ) -> Tuple[Callable, Dict]:
        return (
            upload_to_uri,
            dict(local_path=local_path, uri=uri, exclude=exclude),
        )

    def _sync_down_command(self, uri: str, local_path: str) -> Tuple[Callable, Dict]:
        return (
            download_from_uri,
            dict(uri=uri, local_path=local_path),
        )

    def _delete_command(self, uri: str) -> Tuple[Callable, Dict]:
        return delete_at_uri, dict(uri=uri)


@DeveloperAPI
def get_node_to_storage_syncer(sync_config: SyncConfig) -> Optional[Syncer]:
    """"""
    if sync_config.syncer is None:
        return None

    if not sync_config.upload_dir:
        return None

    if sync_config.syncer == "auto":
        return _DefaultSyncer(
            sync_period=sync_config.sync_period, sync_timeout=sync_config.sync_timeout
        )

    if isinstance(sync_config.syncer, Syncer):
        return sync_config.syncer

    raise ValueError(
        f"Unknown syncer type passed in SyncConfig: {type(sync_config.syncer)}. "
        f"Note that custom sync functions and templates have been deprecated. "
        f"Instead you can implement you own `Syncer` class. "
        f"Please leave a comment on GitHub if you run into any issues with this: "
        f"https://github.com/ray-project/ray/issues"
    )


@DeveloperAPI
class SyncerCallback(Callback):
    """Callback to synchronize trial directories on a worker node with the driver."""

    def __init__(self, enabled: bool = True, sync_period: float = DEFAULT_SYNC_PERIOD):
        self._enabled = enabled
        self._sync_processes: Dict[str, _BackgroundProcess] = {}
        self._sync_times: Dict[str, float] = {}
        self._sync_period = sync_period
        self._trial_ips = {}

    def _get_trial_sync_process(self, trial: "Trial"):
        return self._sync_processes.setdefault(
            trial.trial_id,
            _BackgroundProcess(partial(sync_dir_between_nodes, max_size_bytes=None)),
        )

    def _remove_trial_sync_process(self, trial: "Trial"):
        self._sync_processes.pop(trial.trial_id, None)

    def _should_sync(self, trial: "Trial"):
        last_sync_time = self._sync_times.setdefault(trial.trial_id, float("-inf"))
        return time.time() - last_sync_time >= self._sync_period

    def _mark_as_synced(self, trial: "Trial"):
        self._sync_times[trial.trial_id] = time.time()

    def _local_trial_logdir(self, trial: "Trial"):
        return trial.logdir

    def _remote_trial_logdir(self, trial: "Trial"):
        return trial.logdir

    def _sync_trial_dir(
        self, trial: "Trial", force: bool = False, wait: bool = True
    ) -> bool:
        if not self._enabled or trial.uses_cloud_checkpointing:
            return False

        sync_process = self._get_trial_sync_process(trial)

        # Always run if force=True
        # Otherwise, only run if we should sync (considering sync period)
        # and if there is no sync currently still running.
        if not force and (not self._should_sync(trial) or sync_process.is_running):
            return False

        source_ip = self._trial_ips.get(trial.trial_id, None)

        if not source_ip:
            try:
                source_ip = trial.get_runner_ip()
            except RayActorError as e:
                logger.error(
                    f"Trial {trial}: An error occurred when trying to get the "
                    f"node ip where this trial is running: {e}"
                )

            # If it still does not exist, the runner is terminated.
            if not source_ip:
                return False

        self._trial_ips[trial.trial_id] = source_ip

        try:
            sync_process.wait()
        except TuneError as e:
            # Errors occurring during this wait are not fatal for this
            # checkpoint, so it should just be logged.
            logger.error(
                f"Trial {trial}: An error occurred during the "
                f"checkpoint syncing of the previous checkpoint: {e}"
            )
        sync_process.start(
            source_ip=source_ip,
            source_path=self._remote_trial_logdir(trial),
            target_ip=ray.util.get_node_ip_address(),
            target_path=self._local_trial_logdir(trial),
            exclude=_EXCLUDE_FROM_SYNC,
        )
        self._sync_times[trial.trial_id] = time.time()
        if wait:
            try:
                sync_process.wait()
            except TuneError as e:
                # Errors occurring during this wait are not fatal for this
                # checkpoint, so it should just be logged.
                logger.error(
                    f"Trial {trial}: An error occurred during the "
                    f"checkpoint syncing of the current checkpoint: {e}"
                )
        return True

    def on_trial_start(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self._trial_ips.pop(trial.trial_id, None)

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
        self._trial_ips.pop(trial.trial_id, None)

    def on_trial_error(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self._remove_trial_sync_process(trial)
        self._trial_ips.pop(trial.trial_id, None)

    def on_checkpoint(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        checkpoint: _TrackedCheckpoint,
        **info,
    ):
        if checkpoint.storage_mode == CheckpointStorage.MEMORY:
            return

        if self._sync_trial_dir(
            trial, force=trial.sync_on_checkpoint, wait=True
        ) and not os.path.exists(checkpoint.dir_or_data):
            raise TuneError(
                f"Trial {trial}: Checkpoint path {checkpoint.dir_or_data} not "
                "found after successful sync down."
            )

    def wait_for_all(self):
        failed_syncs = {}
        for trial, sync_process in self._sync_processes.items():
            try:
                sync_process.wait()
            except Exception as e:
                failed_syncs[trial] = e

        if failed_syncs:
            sync_str = "\n".join(
                [f"  {trial}: {e}" for trial, e in failed_syncs.items()]
            )
            raise TuneError(
                f"At least one trial failed to sync down when waiting for all "
                f"trials to sync: \n{sync_str}"
            )

    def __getstate__(self):
        state = self.__dict__.copy()
        for remove in ["_sync_times", "_sync_processes", "_trial_ips"]:
            state.pop(remove, None)
        return state
