import abc
import urllib.parse
import threading
import traceback
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Union,
    Optional,
    Tuple,
)

import logging
import time
from dataclasses import dataclass


try:
    import fsspec
except Exception:
    fsspec = None

try:
    import s3fs
except Exception:
    s3fs = None

from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray.air._internal.remote_storage import (
    fs_hint,
    upload_to_uri,
    download_from_uri,
    delete_at_uri,
    is_non_local_path_uri,
)
from ray.air.constants import (
    LAZY_CHECKPOINT_MARKER_FILE,
    REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE,
)
# from ray.tune import TuneError
from ray.util import log_once
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.widgets import Template


logger = logging.getLogger(__name__)

# Syncing period for syncing checkpoints between nodes or to cloud.
DEFAULT_SYNC_PERIOD = 300

# Default sync timeout after which syncing processes are aborted
DEFAULT_SYNC_TIMEOUT = 1800

# Trigger first node-to-node sync only after this many iterations arrived
_DEFAULT_NODE_SYNCING_MIN_ITER_THRESHOLD = 2
# ... or until at least this much time (in seconds) passed
_DEFAULT_NODE_SYNCING_MIN_TIME_S_THRESHOLD = 10.0


_EXCLUDE_FROM_SYNC = [
    "./checkpoint_-00001",
    "./checkpoint_tmp*",
    "./save_to_object*",
    "./rank_*",
    f"./{LAZY_CHECKPOINT_MARKER_FILE}",
]


class _HeadNodeSyncDeprecationWarning(DeprecationWarning):
    """Error raised when trying to rely on deprecated head node syncing when
    checkpointing across multiple nodes."""

    pass


_SYNC_TO_HEAD_DEPRECATION_MESSAGE = (
    "Ray AIR no longer supports the synchronization of checkpoints and other "
    "artifacts from worker nodes to the head node. This means that the "
    "checkpoints and artifacts saved by trials scheduled on worker nodes will not be "
    "accessible during the run (e.g., resuming from a checkpoint "
    "after a failure) or after the run "
    "(e.g., loading the checkpoint of a trial that ran on an already "
    "terminated worker node).\n\n"
    "To fix this issue, configure AIR to use either:\n"
    "(1) Cloud storage: `RunConfig(storage_path='s3://your/bucket')`\n"
    "(2) A network filesystem mounted on all nodes: "
    "`RunConfig(storage_path='/mnt/path/to/nfs_storage')`\n"
    "See this Github issue for more details on transitioning to cloud storage/NFS "
    "as well as an explanation on why this functionality is "
    "being removed: https://github.com/ray-project/ray/issues/37177\n"
    "If you are already using NFS, you can ignore this warning message.\n\n"
    "Other temporary workarounds:\n"
    "- If you want to avoid errors/warnings and continue running with "
    "syncing explicitly turned off, set `RunConfig(SyncConfig(syncer=None))`\n"
    "- Or, to re-enable the head node syncing behavior, set the "
    f"environment variable {REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE}=1\n"
    "  - **Note that this functionality will tentatively be hard-deprecated in "
    "Ray 2.7.** See the linked issue for the latest information."
)


@PublicAPI
@dataclass
class SyncConfig:
    """Configuration object for Tune syncing.

    See :ref:`tune-persisted-experiment-data` for an overview of what data is
    synchronized.

    If a remote ``RunConfig(storage_path)`` is specified, both experiment and trial
    checkpoints will be stored on remote (cloud) storage. Synchronization then only
    happens via uploading/downloading from this remote storage.

    There are a few scenarios where syncing takes place:

    (1) The Tune driver (on the head node) syncing the experiment directory to the cloud
        (which includes experiment state such as searcher state, the list of trials
        and their statuses, and trial metadata)
    (2) Workers directly syncing trial checkpoints to the cloud
    (3) Workers syncing their trial directories to the head node (Deprecated)
    (4) Workers syncing artifacts (which include all files saved in the trial directory
        *except* for checkpoints) directly to the cloud.

    .. warning::
        When running on multiple nodes, using the local filesystem of the head node as
        the persistent storage location is *deprecated*.
        If you save trial checkpoints and run on a multi-node cluster,
        Tune will raise an error by default, if NFS or cloud storage is not setup.
        See `this issue <https://github.com/ray-project/ray/issues/37177>`_
        for more information, including temporary workarounds
        as well as the deprecation and removal schedule.

    See :ref:`tune-storage-options` for more details and examples.

    Args:
        upload_dir: This config is deprecated in favor of ``RunConfig(storage_path)``.
        syncer: If a cloud ``storage_path`` is configured, then this config accepts a
            custom syncer subclassing :class:`~ray.tune.syncer.Syncer` which will be
            used to synchronize checkpoints to/from cloud storage.
            Defaults to ``"auto"`` (auto detect), which defaults to use ``pyarrow.fs``.
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
        sync_artifacts: Whether or not to sync artifacts that are saved to the
            trial directory (accessed via `session.get_trial_dir()`) to the cloud.
            Artifact syncing happens at the same frequency as trial checkpoint syncing.
            **Note**: This is scenario (4).
        sync_on_checkpoint: This config is deprecated.
            If *True*, a sync from a worker's remote trial directory
            to the head node will be forced on every trial checkpoint, regardless
            of the ``sync_period``. Defaults to True.
            **Note**: This is ignored if ``upload_dir`` is specified, since this
            only applies to worker-to-head-node syncing (3).
    """

    upload_dir: Optional[str] = None
    syncer: Optional[Union[str, "Syncer"]] = "auto"
    sync_period: int = DEFAULT_SYNC_PERIOD
    sync_timeout: int = DEFAULT_SYNC_TIMEOUT
    sync_artifacts: bool = True

    sync_on_checkpoint: bool = True

    def __post_init__(self):
        if self.upload_dir and self.syncer is None:
            raise ValueError(
                "`upload_dir` enables syncing to cloud storage, but `syncer=None` "
                "disables syncing. Either remove the `upload_dir`, "
                "or set `syncer` to 'auto' or a custom syncer."
            )

    def _repr_html_(self) -> str:
        """Generate an HTML representation of the SyncConfig.

        Note that self.syncer is omitted here; seems to have some overlap
        with existing configuration settings here in the SyncConfig class.
        """
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

    def validate_upload_dir(self, upload_dir: Optional[str] = None) -> bool:
        """Checks if ``upload_dir`` is supported by ``syncer``.

        Returns True if ``upload_dir`` is valid, otherwise raises
        ``ValueError``.

        The ``upload_dir`` attribute of ``SyncConfig`` is depreacted and will be
        removed in the futures. This method also accepts a ``upload_dir`` argument
        that will be checked for validity instead, if set.

        Args:
            upload_dir: Path to validate.

        """
        upload_dir = upload_dir or self.upload_dir
        if upload_dir and self.syncer is None:
            raise ValueError(
                "`upload_dir` enables syncing to cloud storage, but `syncer=None` "
                "disables syncing. Either remove the `upload_dir`, "
                "or set `syncer` to 'auto' or a custom syncer."
            )
        if not upload_dir and isinstance(self.syncer, Syncer):
            raise ValueError("Must specify an `upload_dir` to use a custom `syncer`.")

        parsed = urllib.parse.urlparse(upload_dir)
        # Todo: Only warn for pyarrow versions that are affected by
        # https://github.com/apache/arrow/issues/32372#issuecomment-1421097792
        if (
            parsed.scheme
            and not s3fs
            and parsed.scheme.startswith("s3")
            and log_once("fsspec_missing")
        ):
            logger.warning(
                "You are using S3 for remote storage, but you don't have `s3fs` "
                "installed. Due to a bug in PyArrow, this can lead to significant "
                "slowdowns. To avoid this, install s3fs with "
                "`pip install fsspec s3fs`."
            )
        elif not fsspec and log_once("fsspec_missing"):
            logger.warning(
                "You are using remote storage, but you don't have `fsspec` "
                "installed. This can lead to inefficient syncing behavior. "
                "To avoid this, install fsspec with "
                "`pip install fsspec`. Depending on your remote storage provider, "
                "consider installing the respective fsspec-package "
                "(see https://github.com/fsspec)."
            )

        if isinstance(self.syncer, Syncer):
            return self.syncer.validate_upload_dir(upload_dir or self.upload_dir)
        else:
            return Syncer.validate_upload_dir(upload_dir or self.upload_dir)


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
        raise TuneError(
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
            logger.warning(
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
        from ray.train._internal.storage import _use_storage_context

        assert not _use_storage_context(), "Should never be used in this mode."

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

    def wait(self):
        if self._sync_process:
            try:
                self._sync_process.wait(timeout=self.sync_timeout)
            except Exception as e:
                raise e
            finally:
                # Regardless of whether the sync process succeeded within the timeout,
                # clear the sync process so a new one can be created.
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
    """Default syncer between local and remote storage, using `pyarrow.fs.copy_files`"""

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
def get_node_to_storage_syncer(
    sync_config: SyncConfig, upload_dir: Optional[str] = None
) -> Optional[Syncer]:
    """"""
    if sync_config.syncer is None:
        return None

    if not sync_config.upload_dir and not upload_dir:
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
