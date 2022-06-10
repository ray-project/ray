import abc
import logging
import time

from typing import Optional, List, Tuple

import ray
from ray.tune.error import TuneError
from ray.tune.utils.file_transfer import sync_dir_between_nodes, delete_on_node
from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)


noop_template = ": {target}"  # noop in bash


def noop(*args):
    return


@Deprecated
class SyncClient(abc.ABC):
    """Client interface for interacting with remote storage options."""

    def __init__(self):
        raise DeprecationWarning(
            "SyncClient has been deprecated. Please implement a "
            "`ray.tune.syncer.Syncer` instead."
        )


@Deprecated
class FunctionBasedClient(SyncClient):
    def __init__(self, sync_up_func, sync_down_func, delete_func=None):
        raise DeprecationWarning(
            "FunctionBasedClient has been deprecated. Please implement a "
            "`ray.tune.syncer.Syncer` instead."
        )


NOOP = FunctionBasedClient(noop, noop)


@Deprecated
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
        raise DeprecationWarning(
            "CommandBasedClient has been deprecated. Please implement a "
            "`ray.tune.syncer.Syncer` instead."
        )


@Deprecated
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
