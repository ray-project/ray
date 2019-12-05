from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils
import distutils.spawn
import logging
import os
import time
import types

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

from ray import services
from ray.tune.cluster_info import get_ssh_key, get_ssh_user
from ray.tune.sync_client import CommandBasedClient, FunctionBasedClient, NOOP

logger = logging.getLogger(__name__)

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX)
SYNC_PERIOD = 300

_log_sync_warned = False
_syncers = {}


def wait_for_sync():
    for syncer in _syncers.values():
        syncer.wait()


def log_sync_template(options=""):
    """Template enabling syncs between driver and worker when possible.

    Requires ray cluster to be started with the autoscaler. Also requires
    rsync to be installed.

    Args:
        options (str): Addtional rsync options.

    Returns:
        Sync template with source and target parameters. None if rsync
        unavailable.
    """
    if not distutils.spawn.find_executable("rsync"):
        logger.error("Log sync requires rsync to be installed.")
        return None
    global _log_sync_warned
    ssh_key = get_ssh_key()
    if ssh_key is None:
        if not _log_sync_warned:
            logger.debug("Log sync requires cluster to be setup with "
                         "`ray up`.")
            _log_sync_warned = True
        return None

    rsh = "ssh -i {ssh_key} -o ConnectTimeout=120s -o StrictHostKeyChecking=no"
    rsh = rsh.format(ssh_key=quote(ssh_key))
    template = "rsync {options} -savz -e {rsh} {{source}} {{target}}"
    return template.format(options=options, rsh=quote(rsh))


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

    def sync_up_if_needed(self, relative_subdir=None):
        if time.time() - self.last_sync_up_time > SYNC_PERIOD:
            self.sync_up(relative_subdir)

    def sync_down_if_needed(self, relative_subdir=None):
        if time.time() - self.last_sync_down_time > SYNC_PERIOD:
            self.sync_down(relative_subdir)

    def sync_up(self, relative_subdir=None):
        """Attempts to start the sync-up to the remote path.

        Returns:
            Whether the sync (if feasible) was successfully started.
        """
        result = False
        relative_subdir = relative_subdir or ""
        # Add a trailing slash in case client requires it (eg: rsync).
        relative_subdir = os.path.join(relative_subdir, "")
        if self.validate_hosts(self._local_dir, self._remote_path):
            try:
                local_path = os.path.join(self._local_dir, relative_subdir)
                remote_path = os.path.join(self._remote_path, relative_subdir)
                logger.debug("Syncing: %s to %s", local_path, remote_path)
                result = self.sync_client.sync_up(local_path, remote_path)
                self.last_sync_up_time = time.time()
            except Exception:
                logger.exception("Sync execution failed.")
        return result

    def sync_down(self, relative_subdir=None):
        """Attempts to start the sync-down from the remote path.

        Returns:
             Whether the sync (if feasible) was successfully started.
        """
        result = False
        relative_subdir = os.path.join(relative_subdir, "") or ""
        if self.validate_hosts(self._local_dir, self._remote_path):
            try:
                local_path = os.path.join(self._local_dir, relative_subdir)
                remote_path = os.path.join(self._remote_path, relative_subdir)
                logger.debug("Syncing: %s to %s", remote_path, local_path)
                result = self.sync_client.sync_down(remote_path, local_path)
                self.last_sync_down_time = time.time()
            except Exception:
                logger.exception("Sync execution failed.")
        return result

    def validate_hosts(self, source, target):
        if not (source and target):
            logger.debug("Source or target is empty, skipping sync for "
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


class NodeSyncer(Syncer):
    """Syncer for syncing files to/from a remote dir to a local dir."""

    def __init__(self, local_dir, remote_dir, sync_client):
        self.local_ip = services.get_node_ip_address()
        self.worker_ip = None
        super(NodeSyncer, self).__init__(local_dir, remote_dir, sync_client)

    def set_worker_ip(self, worker_ip):
        """Sets the worker IP to sync logs from."""
        self.worker_ip = worker_ip

    def has_remote_target(self):
        """Returns whether the Syncer has a remote target."""
        if not self.worker_ip:
            logger.debug("Worker IP unknown, skipping sync for %s",
                         self._local_dir)
            return False
        if self.worker_ip == self.local_ip:
            logger.debug("Worker IP is local IP, skipping sync for %s",
                         self._local_dir)
            return False
        return True

    def sync_up_if_needed(self, relative_subdir=None):
        if not self.has_remote_target():
            return True
        return super(NodeSyncer, self).sync_up_if_needed(relative_subdir)

    def sync_down_if_needed(self, relative_subdir=None):
        if not self.has_remote_target():
            return True
        return super(NodeSyncer, self).sync_down_if_needed(relative_subdir)

    def sync_up_to_new_location(self, worker_ip, relative_subdir=None):
        if worker_ip != self.worker_ip:
            logger.debug("Setting new worker IP to %s", worker_ip)
            self.set_worker_ip(worker_ip)
            self.reset()
            if not self.sync_up(relative_subdir):
                logger.warning(
                    "Sync up to new location skipped. This should not occur.")
        else:
            logger.warning("Sync attempted to same IP %s.", worker_ip)

    def sync_up(self, relative_subdir=None):
        if not self.has_remote_target():
            return True
        return super(NodeSyncer, self).sync_up(relative_subdir)

    def sync_down(self, relative_subdir=None):
        if not self.has_remote_target():
            return True
        return super(NodeSyncer, self).sync_down(relative_subdir)

    @property
    def _remote_path(self):
        ssh_user = get_ssh_user()
        global _log_sync_warned
        if not self.has_remote_target():
            return None
        if ssh_user is None:
            if not _log_sync_warned:
                logger.error("Syncer requires cluster to be setup with "
                             "`ray up`.")
                _log_sync_warned = True
            return None
        return "{}@{}:{}/".format(ssh_user, self.worker_ip, self._remote_dir)


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

    Raises:
        ValueError if malformed remote_dir.
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
            raise ValueError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX))
        template = "aws s3 sync {source} {target}"
        s3_client = CommandBasedClient(template, template)
        _syncers[key] = Syncer(local_dir, remote_dir, s3_client)
    elif remote_dir.startswith(GS_PREFIX):
        if not distutils.spawn.find_executable("gsutil"):
            raise ValueError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GS_PREFIX))
        template = "gsutil rsync -r {source} {target}"
        gs_client = CommandBasedClient(template, template)
        _syncers[key] = Syncer(local_dir, remote_dir, gs_client)
    else:
        raise ValueError("Upload uri must start with one of: {}"
                         "".format(ALLOWED_REMOTE_PREFIXES))

    return _syncers[key]


def get_syncer(local_dir, remote_dir=None, sync_function=None):
    """Returns a NodeSyncer between two directories.

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
    # TODO(ujvl): Refactor initialization: remove dir parameters.
    _syncers[key] = NodeSyncer(local_dir, remote_dir, sync_client)
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
