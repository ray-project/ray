import abc
import logging
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray.train.constants import _DEPRECATED_VALUE
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.widgets import Template

logger = logging.getLogger(__name__)

# Syncing period for syncing checkpoints between nodes or to cloud.
DEFAULT_SYNC_PERIOD = 300

# Default sync timeout after which syncing processes are aborted
DEFAULT_SYNC_TIMEOUT = 1800


@PublicAPI(stability="stable")
@dataclass
class SyncConfig:
    """Configuration object for Train/Tune file syncing to `RunConfig(storage_path)`.

    In Ray Train/Tune, here is where syncing (mainly uploading) happens:

    The experiment driver (on the head node) syncs the experiment directory to storage
    (which includes experiment state such as searcher state, the list of trials
    and their statuses, and trial metadata).

    It's also possible to sync artifacts from the trial directory to storage
    by setting `sync_artifacts=True`.
    For a Ray Tune run with many trials, each trial will upload its trial directory
    to storage, which includes arbitrary files that you dumped during the run.
    For a Ray Train run doing distributed training, each remote worker will similarly
    upload its trial directory to storage.

    See :ref:`persistent-storage-guide` for more details and examples.

    Args:
        sync_period: Minimum time in seconds to wait between two sync operations.
            A smaller ``sync_period`` will have the data in storage updated more often
            but introduces more syncing overhead. Defaults to 5 minutes.
        sync_timeout: Maximum time in seconds to wait for a sync process
            to finish running. A sync operation will run for at most this long
            before raising a `TimeoutError`. Defaults to 30 minutes.
        sync_artifacts: [Beta] Whether or not to sync artifacts that are saved to the
            trial directory (accessed via `train.get_context().get_trial_dir()`)
            to the persistent storage configured via `train.RunConfig(storage_path)`.
            The trial or remote worker will try to launch an artifact syncing
            operation every time `train.report` happens, subject to `sync_period`
            and `sync_artifacts_on_checkpoint`.
            Defaults to False -- no artifacts are persisted by default.
        sync_artifacts_on_checkpoint: If True, trial/worker artifacts are
            forcefully synced on every reported checkpoint.
            This only has an effect if `sync_artifacts` is True.
            Defaults to True.
    """

    sync_period: int = DEFAULT_SYNC_PERIOD
    sync_timeout: int = DEFAULT_SYNC_TIMEOUT
    sync_artifacts: bool = False
    sync_artifacts_on_checkpoint: bool = True
    upload_dir: Optional[str] = _DEPRECATED_VALUE
    syncer: Optional[Union[str, "Syncer"]] = _DEPRECATED_VALUE
    sync_on_checkpoint: bool = _DEPRECATED_VALUE

    # TODO(justinvyu): [Deprecated] Remove in 2.11.
    def _deprecation_warning(self, attr_name: str, extra_msg: str):
        if getattr(self, attr_name) != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                f"`SyncConfig({attr_name})` is a deprecated configuration "
                "Please remove it from your `SyncConfig`. "
                f"{extra_msg}"
            )

    def __post_init__(self):
        for attr_name, extra_msg in [
            (
                "upload_dir",
                "\nPlease specify `ray.train.RunConfig(storage_path)` instead.",
            ),
            (
                "syncer",
                "\nPlease implement custom syncing logic with a custom "
                "`pyarrow.fs.FileSystem` instead, and pass it into "
                "`ray.train.RunConfig(storage_filesystem)`. "
                "See here: https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html#custom-storage",  # noqa: E501
            ),
            ("sync_on_checkpoint", ""),
        ]:
            self._deprecation_warning(attr_name, extra_msg)

    def _repr_html_(self) -> str:
        """Generate an HTML representation of the SyncConfig."""
        return Template("scrollableTable.html.j2").render(
            table=tabulate(
                {
                    "Setting": ["Sync period", "Sync timeout"],
                    "Value": [self.sync_period, self.sync_timeout],
                },
                tablefmt="html",
                showindex=False,
                headers="keys",
            ),
            max_height="none",
        )


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
        """Waits for the background process to finish running. Waits until the
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

    1. Synchronizing data such as experiment state snapshots from the driver to
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

    def wait(self, timeout: Optional[float] = None):
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

    def wait_or_retry(self, max_retries: int = 2, backoff_s: int = 5):
        assert max_retries > 0
        last_error_traceback = None
        for i in range(max_retries + 1):
            try:
                self.wait()
            except Exception as e:
                attempts_remaining = max_retries - i

                # If we're out of retries, then save the full traceback of the last
                # error and show it when raising an exception.
                if attempts_remaining == 0:
                    last_error_traceback = traceback.format_exc()
                    break

                logger.error(
                    f"The latest sync operation failed with the following error: "
                    f"{repr(e)}\n"
                    f"Retrying {attempts_remaining} more time(s) after sleeping "
                    f"for {backoff_s} seconds..."
                )
                time.sleep(backoff_s)
                self.retry()
                continue
            # Succeeded!
            return
        raise RuntimeError(
            f"Failed sync even after {max_retries} retries. "
            f"The latest sync failed with the following error:\n{last_error_traceback}"
        )

    def reset(self):
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")

    def close(self):
        pass

    def _repr_html_(self) -> str:
        return


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

    def _launch_sync_process(self, sync_command: Tuple[Callable, Dict]):
        """Waits for the previous sync process to finish,
        then launches a new process that runs the given command."""
        if self._sync_process:
            try:
                self.wait()
            except Exception:
                logger.warning(
                    f"Last sync command failed with the following error:\n"
                    f"{traceback.format_exc()}"
                )

        self._current_cmd = sync_command
        self.retry()

    def sync_up(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        if self._should_continue_existing_sync():
            logger.debug(
                f"Last sync still in progress, "
                f"skipping sync up of {local_dir} to {remote_dir}"
            )
            return False

        sync_up_cmd = self._sync_up_command(
            local_path=local_dir, uri=remote_dir, exclude=exclude
        )
        self._launch_sync_process(sync_up_cmd)

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

        sync_down_cmd = self._sync_down_command(uri=remote_dir, local_path=local_dir)
        self._launch_sync_process(sync_down_cmd)

        return True

    def _sync_down_command(self, uri: str, local_path: str) -> Tuple[Callable, Dict]:
        raise NotImplementedError

    def delete(self, remote_dir: str) -> bool:
        if self._should_continue_existing_sync():
            logger.warning(
                f"Last sync still in progress, skipping deletion of {remote_dir}"
            )
            return False

        delete_cmd = self._delete_command(uri=remote_dir)
        self._launch_sync_process(delete_cmd)

        return True

    def _delete_command(self, uri: str) -> Tuple[Callable, Dict]:
        raise NotImplementedError

    def wait(self, timeout: Optional[float] = None):
        if self._sync_process:
            try:
                self._sync_process.wait(timeout=timeout or self.sync_timeout)
            except Exception as e:
                raise e
            finally:
                # Regardless of whether the sync process succeeded within the timeout,
                # clear the sync process so a new one can be created.
                self._sync_process = None

    def retry(self):
        if not self._current_cmd:
            raise RuntimeError("No sync command set, cannot retry.")
        cmd, kwargs = self._current_cmd
        self._sync_process = _BackgroundProcess(cmd)
        self._sync_process.start(**kwargs)

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_sync_process"] = None
        return state
