from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils
import logging
import os
import subprocess
import tempfile
import time
import types

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

from ray.tune.error import TuneError
from ray.tune.log_sync import log_sync_template, NodeSyncMixin

logger = logging.getLogger(__name__)

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX)
SYNC_PERIOD = 300

_syncers = {}


def wait_for_sync():
    for syncer in _syncers.values():
        syncer.wait()


class SyncClient(object):
    def sync_up(self, source, target):
        """Sync up from source to target.

        Args:
            source (str): Source path.
            target (str): Target path.

        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def sync_down(self, source, target):
        """Sync down from source to target.

        Args:
            source (str): Source path.
            target (str): Target path.

        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def wait(self):
        """Wait for current sync to complete, if asynchronously started."""
        pass

    def reset(self):
        """Resets state."""
        pass


class FunctionBasedClient(SyncClient):
    def __init__(self, sync_up_func, sync_down_func):
        self.sync_up_func = sync_up_func
        self.sync_down_func = sync_down_func

    def sync_up(self, source, target):
        self.sync_up_func(source, target)
        return True

    def sync_down(self, source, target):
        self.sync_down_func(source, target)
        return True


class CommandBasedClient(SyncClient):
    def __init__(self, sync_up_template, sync_down_template):
        """Syncs between two directories with the given command.

        Arguments:
            sync_up_template (str): A runnable string template; needs to
                include replacement fields '{source}' and '{target}'.
            sync_down_template (str): A runnable string template; needs to
                include replacement fields '{source}' and '{target}'.
        """
        if not isinstance(sync_up_template, str):
            raise ValueError("{} is not a string.".format(sync_up_template))
        if not isinstance(sync_down_template, str):
            raise ValueError("{} is not a string.".format(sync_down_template))
        self._validate_sync_string(sync_up_template)
        self._validate_sync_string(sync_down_template)
        self.sync_up_template = sync_up_template
        self.sync_down_template = sync_down_template
        self.logfile = None
        self.sync_process = None

    def set_logdir(self, logdir):
        """Sets the directory to log sync execution output in.

        Args:
            logdir: Log directory.
        """
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync", dir=logdir, suffix=".log", delete=False)

    def sync_up(self, source, target):
        return self.execute(self.sync_up_template, source, target)

    def sync_down(self, source, target):
        return self.execute(self.sync_down_template, source, target)

    def execute(self, sync_template, source, target):
        """Executes sync_template on source and target."""
        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                logger.warning("Last sync is still in progress, skipping.")
                return False
        final_cmd = sync_template.format(
            source=quote(source), target=quote(target))
        logger.debug("Running sync: {}".format(final_cmd))
        self.sync_process = subprocess.Popen(
            final_cmd, shell=True, stderr=subprocess.PIPE, stdout=self.logfile)
        return True

    def wait(self):
        if self.sync_process:
            _, error_msg = self.sync_process.communicate()
            error_msg = error_msg.decode("ascii")
            code = self.sync_process.returncode
            self.sync_process = None
            if code != 0:
                raise TuneError("Sync error ({}): {}".format(code, error_msg))

    def reset(self):
        if self.sync_process:
            logger.warning("Sync process still running but resetting anyways.")
            self.sync_process = None

    @staticmethod
    def _validate_sync_string(sync_string):
        if "{source}" not in sync_string:
            raise ValueError("Sync template missing '{source}'.")
        if "{target}" not in sync_string:
            raise ValueError("Sync template missing '{target}'.")


NOOP = FunctionBasedClient(lambda s, t: None, lambda s, t: None)


class Syncer(object):
    def __init__(self, local_dir, remote_dir, sync_client=NOOP):
        """Syncs between two directories with the sync_function.

        Arguments:
            local_dir (str): Directory to sync. Uniquely identifies the syncer.
            remote_dir (str): Remote directory to sync with.
            sync_client (SyncClient): Client for syncing between local_dir and
                remote_dir. Defaults to a Noop.
        """
        self._local_dir = (os.path.join(local_dir, "")
                           if local_dir else local_dir)
        self._remote_dir = remote_dir
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")
        self.sync_client = sync_client

    def sync_up_if_needed(self):
        if time.time() - self.last_sync_up_time > SYNC_PERIOD:
            self.sync_up()

    def sync_down_if_needed(self):
        if time.time() - self.last_sync_down_time > SYNC_PERIOD:
            self.sync_down()

    def sync_up(self):
        """Attempts to start the sync-up to the remote path.

        Returns:
            Whether the sync (if feasible) was successfully started.
        """
        result = False
        if self.validate_hosts(self._local_dir, self._remote_path):
            try:
                result = self.sync_client.sync_up(self._local_dir,
                                                  self._remote_path)
                self.last_sync_up_time = time.time()
            except Exception:
                logger.exception("Sync execution failed.")
        return result

    def sync_down(self):
        """Attempts to start the sync-down from the remote path.

        Returns:
             Whether the sync (if feasible) was successfully started.
        """
        result = False
        if self.validate_hosts(self._local_dir, self._remote_path):
            try:
                result = self.sync_client.sync_down(self._remote_path,
                                                    self._local_dir)
                self.last_sync_down_time = time.time()
            except Exception:
                logger.exception("Sync execution failed.")
        return result

    def validate_hosts(self, source, target):
        if not (source and target):
            logger.debug("Source or target is empty, skipping log sync for "
                         "{}".format(self._local_dir))
            return False
        return True

    def wait(self):
        """Waits for the sync client to complete the current sync."""
        self.sync_client.wait()

    def reset(self):
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")
        self.sync_client.reset()

    @property
    def _remote_path(self):
        return self._remote_dir


def get_cloud_syncer(local_dir, remote_dir=None, sync_function=None):
    """Returns a Syncer.

    This syncer is in charge of syncing the local_dir with upload_dir.

    Args:
        local_dir (str): Source directory for syncing.
        remote_dir (str): Target directory for syncing. If not provided, a
            no-op Syncer is returned.
        sync_function (func | str): Function for syncing the local_dir to
            remote_dir. If string, then it must be a string template for
            syncer to run. If not provided, it defaults
            to standard S3 or gsutil sync commands.
    """
    key = (local_dir, remote_dir)

    if key in _syncers:
        return _syncers[key]

    if not remote_dir:
        _syncers[key] = Syncer(local_dir, remote_dir, NOOP)
        return _syncers[key]

    client = _get_sync_client(sync_function)

    if client:
        _syncers[key] = Syncer(local_dir, remote_dir, client)
        return _syncers[key]

    if remote_dir.startswith(S3_PREFIX):
        if not distutils.spawn.find_executable("aws"):
            raise TuneError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX))
        template = "aws s3 sync {source} {target}"
        s3_client = CommandBasedClient(template, template)
        _syncers[key] = Syncer(local_dir, remote_dir, s3_client)
    elif remote_dir.startswith(GS_PREFIX):
        if not distutils.spawn.find_executable("gsutil"):
            raise TuneError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GS_PREFIX))
        template = "gsutil rsync -r {source} {target}"
        gs_client = CommandBasedClient(template, template)
        _syncers[key] = Syncer(local_dir, remote_dir, gs_client)
    else:
        raise TuneError("Upload uri must start with one of: {}"
                        "".format(ALLOWED_REMOTE_PREFIXES))

    return _syncers[key]


def get_log_syncer(local_dir, remote_dir=None, sync_function=None):
    """Returns a log Syncer.

    Args:
        local_dir (str): Source directory for syncing.
        remote_dir (str): Target directory for syncing. If not provided, a
            no-op Syncer is returned.
        sync_function (func|str): Function for syncing the local_dir to
            remote_dir. If string, then it must be a string template for
            syncer to run. If not provided, it defaults rsync.
    """
    key = (local_dir, remote_dir)
    if key in _syncers:
        return _syncers[key]
    elif not remote_dir:
        sync_client = NOOP
    elif sync_function:
        sync_client = _get_sync_client(sync_function)
    else:
        sync_up = log_sync_template()
        sync_down = log_sync_template(options="--remove-source-files")
        if sync_up and sync_down:
            sync_client = CommandBasedClient(sync_up, sync_down)
            sync_client.set_logdir(local_dir)
        else:
            sync_client = NOOP

    class MixedSyncer(NodeSyncMixin, Syncer):
        def __init__(self, *args, **kwargs):
            Syncer.__init__(self, *args, **kwargs)
            NodeSyncMixin.__init__(self)

    _syncers[key] = MixedSyncer(local_dir, remote_dir, sync_client)
    return _syncers[key]


def _get_sync_client(sync_function):
    if not sync_function:
        return None
    if isinstance(sync_function, types.FunctionType):
        return FunctionBasedClient(sync_function, sync_function)
    elif isinstance(sync_function, str):
        return CommandBasedClient(sync_function, sync_function)
    else:
        raise ValueError("Sync function {} must be string or function".format(
            sync_function))
