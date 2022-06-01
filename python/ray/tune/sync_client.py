import abc
import distutils
import distutils.spawn
import inspect
import logging
import pathlib
import subprocess
import tempfile
import time
import types

from typing import Optional, List, Callable, Union, Tuple

from shlex import quote

import ray
from ray.tune.error import TuneError
from ray.tune.utils.file_transfer import sync_dir_between_nodes, delete_on_node
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.ml.utils.remote_storage import (
    S3_PREFIX,
    GS_PREFIX,
    HDFS_PREFIX,
    ALLOWED_REMOTE_PREFIXES,
)

logger = logging.getLogger(__name__)


noop_template = ": {target}"  # noop in bash


def noop(*args):
    return


def get_sync_client(
    sync_function: Optional[Union[str, Callable]],
    delete_function: Optional[Union[str, Callable]] = None,
) -> Optional["SyncClient"]:
    """Returns a sync client.

    Args:
        sync_function: Sync function.
        delete_function: Delete function. Must be
            the same type as sync_function if it is provided.

    Raises:
        ValueError if sync_function or delete_function are malformed.
    """
    if sync_function is None:
        return None
    if delete_function and type(sync_function) != type(delete_function):
        raise ValueError("Sync and delete functions must be of same type.")
    if isinstance(sync_function, types.FunctionType):
        delete_function = delete_function or noop
        client_cls = FunctionBasedClient
    elif isinstance(sync_function, str):
        delete_function = delete_function or noop_template
        client_cls = CommandBasedClient
    else:
        raise ValueError(
            "Sync function {} must be string or function".format(sync_function)
        )
    return client_cls(sync_function, sync_function, delete_function)


def get_cloud_sync_client(remote_path: str) -> "CommandBasedClient":
    """Returns a CommandBasedClient that can sync to/from remote storage.

    Args:
        remote_path: Path to remote storage (S3, GS or HDFS).

    Raises:
        ValueError if malformed remote_dir.
    """
    if remote_path.startswith(S3_PREFIX):
        if not distutils.spawn.find_executable("aws"):
            raise ValueError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX)
            )
        sync_up_template = (
            "aws s3 sync {source} {target} "
            "--exact-timestamps --only-show-errors {options}"
        )
        sync_down_template = sync_up_template
        delete_template = "aws s3 rm {target} --recursive --only-show-errors {options}"
        exclude_template = "--exclude '{pattern}'"
    elif remote_path.startswith(GS_PREFIX):
        if not distutils.spawn.find_executable("gsutil"):
            raise ValueError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GS_PREFIX)
            )
        sync_up_template = "gsutil rsync -r {options} {source} {target}"
        sync_down_template = sync_up_template
        delete_template = "gsutil rm -r {options} {target}"
        exclude_template = "-x '{regex_pattern}'"
    elif remote_path.startswith(HDFS_PREFIX):
        if not distutils.spawn.find_executable("hdfs"):
            raise ValueError(
                "Upload uri starting with '{}' requires hdfs tool"
                " to be installed".format(HDFS_PREFIX)
            )
        sync_up_template = "hdfs dfs -put -f {source} {target}"
        sync_down_template = "hdfs dfs -get -f {source} {target}"
        delete_template = "hdfs dfs -rm -r {target}"
        exclude_template = None
    else:
        raise ValueError(
            f"Upload uri must start with one of: {ALLOWED_REMOTE_PREFIXES} "
            f"(is: `{remote_path}`)"
        )
    return CommandBasedClient(
        sync_up_template, sync_down_template, delete_template, exclude_template
    )


@PublicAPI(stability="beta")
class SyncClient(abc.ABC):
    """Client interface for interacting with remote storage options."""

    def sync_up(self, source: str, target: str, exclude: Optional[List] = None):
        """Syncs up from source to target.

        Args:
            source: Source path.
            target: Target path.
            exclude: Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.

        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def sync_down(self, source: str, target: str, exclude: Optional[List] = None):
        """Syncs down from source to target.

        Args:
            source: Source path.
            target: Target path.
            exclude: Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.

        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def delete(self, target: str):
        """Deletes target.

        Args:
            target: Target path.

        Returns:
            True if delete initiation successful, False otherwise.
        """
        raise NotImplementedError

    def wait(self):
        """Waits for current sync to complete, if asynchronously started."""
        pass

    def wait_or_retry(self, max_retries: int = 3, backoff_s: int = 5):
        """Wait for current sync to complete or retries on error."""
        pass

    def reset(self):
        """Resets state."""
        pass

    def close(self):
        """Clean up hook."""
        pass


def _is_legacy_sync_fn(func) -> bool:
    sig = inspect.signature(func)
    try:
        sig.bind_partial(None, None, None)
        return False
    except TypeError:
        return True


@DeveloperAPI
class FunctionBasedClient(SyncClient):
    def __init__(self, sync_up_func, sync_down_func, delete_func=None):
        self.sync_up_func = sync_up_func
        self._sync_up_legacy = _is_legacy_sync_fn(sync_up_func)

        self.sync_down_func = sync_down_func
        self._sync_down_legacy = _is_legacy_sync_fn(sync_up_func)

        if self._sync_up_legacy or self._sync_down_legacy:
            raise DeprecationWarning(
                "Your sync functions currently only accepts two params "
                "(a `source` and a `target`). In the future, we will "
                "pass an additional `exclude` parameter. Please adjust "
                "your sync function accordingly."
            )

        self.delete_func = delete_func or noop

    def sync_up(self, source, target, exclude: Optional[List] = None):
        if self._sync_up_legacy:
            self.sync_up_func(source, target)
        else:
            self.sync_up_func(source, target, exclude)
        return True

    def sync_down(self, source, target, exclude: Optional[List] = None):
        if self._sync_down_legacy:
            self.sync_down_func(source, target)
        else:
            self.sync_down_func(source, target, exclude)
        return True

    def delete(self, target):
        self.delete_func(target)
        return True


NOOP = FunctionBasedClient(noop, noop)


@DeveloperAPI
class CommandBasedClient(SyncClient):
    """Syncs between two directories with the given command.

    If a sync is already in-flight when calling ``sync_down`` or
    ``sync_up``, a warning will be printed and the new sync command is
    ignored. To force a new sync, either use ``wait()``
    (or ``wait_or_retry()``) to wait until the previous sync has finished,
    or call ``reset()`` to detach from the previous sync. Note that this
    will not kill the previous sync command, so it may still be executed.

    Arguments:
        sync_up_template: A runnable string template; needs to
            include replacement fields ``{source}``, ``{target}``, and
            ``{options}``.
        sync_down_template: A runnable string template; needs to
            include replacement fields ``{source}``, ``{target}``, and
            ``{options}``.
        delete_template: A runnable string template; needs
            to include replacement field ``{target}``. Noop by default.
        exclude_template: A pattern with possible
            replacement fields ``{pattern}`` and ``{regex_pattern}``.
            Will replace ``{options}}`` in the sync up/down templates
            if files/directories to exclude are passed.

    """

    def __init__(
        self,
        sync_up_template: str,
        sync_down_template: str,
        delete_template: Optional[str] = noop_template,
        exclude_template: Optional[str] = None,
    ):
        self._validate_sync_string(sync_up_template)
        self._validate_sync_string(sync_down_template)
        self._validate_exclude_template(exclude_template)
        self.sync_up_template = sync_up_template
        self.sync_down_template = sync_down_template
        self.delete_template = delete_template
        self.exclude_template = exclude_template
        self.logfile = None
        self._closed = False
        self.cmd_process = None
        # Keep track of last command for retry
        self._last_cmd = None

    def set_logdir(self, logdir: str):
        """Sets the directory to log sync execution output in.

        Args:
            logdir: Log directory.
        """
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync_out", dir=logdir, suffix=".log", delete=False
        )
        self._closed = False

    def _get_logfile(self):
        if self._closed:
            raise RuntimeError(
                "[internalerror] The client has been closed. "
                "Please report this stacktrace + your cluster configuration "
                "on Github!"
            )
        else:
            return self.logfile

    def _start_process(self, cmd: str) -> subprocess.Popen:
        return subprocess.Popen(
            cmd, shell=True, stderr=subprocess.PIPE, stdout=self._get_logfile()
        )

    def sync_up(self, source, target, exclude: Optional[List] = None):
        return self._execute(self.sync_up_template, source, target, exclude)

    def sync_down(self, source, target, exclude: Optional[List] = None):
        # Just in case some command line sync client expects that local
        # directory exists.
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)
        return self._execute(self.sync_down_template, source, target, exclude)

    def delete(self, target):
        if self.is_running:
            logger.warning(
                f"Last sync client cmd still in progress, "
                f"skipping deletion of {target}"
            )
            return False
        final_cmd = self.delete_template.format(target=quote(target), options="")
        logger.debug("Running delete: {}".format(final_cmd))
        self._last_cmd = final_cmd
        self.cmd_process = self._start_process(final_cmd)
        return True

    def wait(self):
        if self.cmd_process:
            _, error_msg = self.cmd_process.communicate()
            error_msg = error_msg.decode("ascii")
            code = self.cmd_process.returncode
            args = self.cmd_process.args
            self.cmd_process = None
            if code != 0:
                raise TuneError(
                    "Sync error. Ran command: {}\n"
                    "Error message ({}): {}".format(args, code, error_msg)
                )

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
                self.cmd_process = self._start_process(self._last_cmd)
                continue
            return
        self.cmd_process = None
        raise TuneError(f"Failed sync even after {max_retries} retries.")

    def reset(self):
        if self.is_running:
            logger.warning("Sync process still running but resetting anyways.")
        self.cmd_process = None
        self._last_cmd = None

    def close(self):
        if self.logfile:
            logger.debug(f"Closing the logfile: {str(self.logfile)}")
            self.logfile.close()
            self.logfile = None
            self._closed = True

    @property
    def is_running(self):
        """Returns whether a sync or delete process is running."""
        if self.cmd_process:
            self.cmd_process.poll()
            return self.cmd_process.returncode is None
        return False

    def _execute(self, sync_template, source, target, exclude: Optional[List] = None):
        """Executes sync_template on source and target."""
        if self.is_running:
            logger.warning(
                f"Last sync client cmd still in progress, "
                f"skipping sync from {source} to {target}."
            )
            return False

        if exclude and self.exclude_template:
            options = []
            if "{pattern}" in self.exclude_template:
                for excl in exclude:
                    options.append(self.exclude_template.format(pattern=excl))
            elif "{regex_pattern}" in self.exclude_template:
                # This is obviously not a great way to convert to regex,
                # but it will do for the moment. Todo: Improve.
                def _to_regex(pattern: str) -> str:
                    return f"({pattern.replace('*', '.*')})"

                regex_pattern = "|".join(_to_regex(excl) for excl in exclude)
                options.append(
                    self.exclude_template.format(regex_pattern=regex_pattern)
                )
            option_str = " ".join(options)
        else:
            option_str = ""

        final_cmd = sync_template.format(
            source=quote(source), target=quote(target), options=option_str
        )
        logger.debug("Running sync: {}".format(final_cmd))
        self._last_cmd = final_cmd
        self.cmd_process = self._start_process(final_cmd)
        return True

    @staticmethod
    def _validate_sync_string(sync_string):
        if not isinstance(sync_string, str):
            raise ValueError("{} is not a string.".format(sync_string))
        if "{source}" not in sync_string:
            raise ValueError("Sync template missing `{source}`: " f"{sync_string}.")
        if "{target}" not in sync_string:
            raise ValueError("Sync template missing `{target}`: " f"{sync_string}.")

    @staticmethod
    def _validate_exclude_template(exclude_template):
        if exclude_template:
            if (
                "{pattern}" not in exclude_template
                and "{regex_pattern}" not in exclude_template
            ):
                raise ValueError(
                    "Neither `{pattern}` nor `{regex_pattern}` found in "
                    f"exclude string `{exclude_template}`"
                )


@DeveloperAPI
class RemoteTaskClient(SyncClient):
    """Sync client that uses remote tasks to synchronize two directories.

    This client expects tuples of (ip, path) for remote sources/targets
    in sync_down/sync_up.

    To avoid unnecessary syncing, the sync client will collect the existing
    files with their respective mtimes and sizes on the (possibly remote)
    target directory. Only files that are not in the target directory or
    differ to those in the target directory by size or mtime will be
    transferred. This is similar to most cloud
    synchronization implementations (e.g. aws s3 sync).

    If a sync is already in-flight when calling ``sync_down`` or
    ``sync_up``, a warning will be printed and the new sync command is
    ignored. To force a new sync, either use ``wait()``
    (or ``wait_or_retry()``) to wait until the previous sync has finished,
    or call ``reset()`` to detach from the previous sync. Note that this
    will not kill the previous sync command, so it may still be executed.
    """

    def __init__(self, _store_remotes: bool = False):
        # Used for testing
        self._store_remotes = _store_remotes
        self._stored_pack_actor_ref = None
        self._stored_files_stats_future = None

        self._sync_future = None

        self._last_source_tuple = None
        self._last_target_tuple = None

        self._max_size_bytes = None  # No file size limit

    def _sync_still_running(self) -> bool:
        if not self._sync_future:
            return False

        ready, not_ready = ray.wait([self._sync_future], timeout=0.0)
        if self._sync_future in ready:
            self.wait()
            return False
        return True

    def sync_down(
        self, source: Tuple[str, str], target: str, exclude: Optional[List] = None
    ) -> bool:
        if self._sync_still_running():
            logger.warning(
                f"Last remote task sync still in progress, "
                f"skipping sync from {source} to {target}."
            )
            return False

        source_ip, source_path = source
        target_ip = ray.util.get_node_ip_address()

        self._last_source_tuple = source_ip, source_path
        self._last_target_tuple = target_ip, target

        return self._execute_sync(self._last_source_tuple, self._last_target_tuple)

    def sync_up(
        self, source: str, target: Tuple[str, str], exclude: Optional[List] = None
    ) -> bool:
        if self._sync_still_running():
            logger.warning(
                f"Last remote task sync still in progress, "
                f"skipping sync from {source} to {target}."
            )
            return False

        source_ip = ray.util.get_node_ip_address()
        target_ip, target_path = target

        self._last_source_tuple = source_ip, source
        self._last_target_tuple = target_ip, target_path

        return self._execute_sync(self._last_source_tuple, self._last_target_tuple)

    def _sync_function(self, *args, **kwargs):
        return sync_dir_between_nodes(*args, **kwargs)

    def _execute_sync(
        self,
        source_tuple: Tuple[str, str],
        target_tuple: Tuple[str, str],
    ) -> bool:
        source_ip, source_path = source_tuple
        target_ip, target_path = target_tuple

        self._sync_future, pack_actor, files_stats = self._sync_function(
            source_ip=source_ip,
            source_path=source_path,
            target_ip=target_ip,
            target_path=target_path,
            return_futures=True,
            max_size_bytes=self._max_size_bytes,
        )

        if self._store_remotes:
            self._stored_pack_actor_ref = pack_actor
            self._stored_files_stats = files_stats

        return True

    def delete(self, target: str):
        if not self._last_target_tuple:
            logger.warning(
                f"Could not delete path {target} as the target node is not known."
            )
            return

        node_ip = self._last_target_tuple[0]

        try:
            delete_on_node(node_ip=node_ip, path=target)
        except Exception as e:
            logger.warning(
                f"Could not delete path {target} on remote node {node_ip}: {e}"
            )

    def wait(self):
        if self._sync_future:
            try:
                ray.get(self._sync_future)
            except Exception as e:
                raise TuneError(
                    f"Remote task sync failed from "
                    f"{self._last_source_tuple} to "
                    f"{self._last_target_tuple}: {e}"
                ) from e
            self._sync_future = None
            self._stored_pack_actor_ref = None
            self._stored_files_stats_future = None

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

                self._execute_sync(
                    self._last_source_tuple,
                    self._last_target_tuple,
                )
                continue
            return
        self._sync_future = None
        self._stored_pack_actor_ref = None
        self._stored_files_stats_future = None
        raise TuneError(f"Failed sync even after {max_retries} retries.")

    def reset(self):
        if self._sync_future:
            logger.warning("Sync process still running but resetting anyways.")
        self._sync_future = None
        self._last_source_tuple = None
        self._last_target_tuple = None
        self._stored_pack_actor_ref = None
        self._stored_files_stats_future = None

    def close(self):
        self._sync_future = None  # Avoid warning
        self.reset()
