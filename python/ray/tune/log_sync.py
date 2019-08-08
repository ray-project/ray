from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils.spawn
import logging

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

import ray
from ray.tune.cluster_info import get_ssh_key, get_ssh_user

logger = logging.getLogger(__name__)
_log_sync_warned = False


def log_sync_template(exclude_patterns=()):
    """Syncs the local_dir between driver and worker if possible.

    Requires ray cluster to be started with the autoscaler. Also requires
    rsync to be installed.

    Args:
        exclude_patterns: an iterable of exclude patterns to exclude from rsync
        syncing. Each element adds an `--exclude [pattern]` flag for rsync
        command.
    """

    if not distutils.spawn.find_executable("rsync"):
        logger.error("Log sync requires rsync to be installed.")
        return
    global _log_sync_warned
    ssh_key = get_ssh_key()
    if ssh_key is None:
        if not _log_sync_warned:
            logger.error("Log sync requires cluster to be setup with "
                         "`ray up`.")
            _log_sync_warned = True
        return

    log_sync_command = " ".join((
        "rsync",
        "-savz",
        *[
            "--exclude {}".format(quote(exclude_pattern))
            for exclude_pattern in exclude_patterns
        ],
        "-e",
        quote(
            " ".join((
                "ssh",
                "-i {}".format(quote(ssh_key)),
                "-o ConnectTimeout=120s",
                "-o StrictHostKeyChecking=no",
            ))),
        "{source}",
        "{target}",
    ))

    return log_sync_command


class NodeSyncMixin():
    """Mixin for syncing files to/from a remote dir to a local dir."""

    def __init__(self):
        assert hasattr(self, "_remote_dir"), "Mixin not mixed with Syncer."
        self.local_ip = ray.services.get_node_ip_address()
        self.worker_ip = None

    def set_worker_ip(self, worker_ip):
        """Set the worker ip to sync logs from."""
        self.worker_ip = worker_ip

    def _check_valid_worker_ip(self):
        if not self.worker_ip:
            logger.debug("Worker ip unknown, skipping log sync for {}".format(
                self._local_dir))
            return False
        if self.worker_ip == self.local_ip:
            logger.debug(
                "Worker ip is local ip, skipping log sync for {}".format(
                    self._local_dir))
            return False
        return True

    @property
    def _remote_path(self):
        ssh_user = get_ssh_user()
        global _log_sync_warned
        if not self._check_valid_worker_ip():
            return
        if ssh_user is None:
            if not _log_sync_warned:
                logger.error("Log sync requires cluster to be setup with "
                             "`ray up`.")
                _log_sync_warned = True
            return
        return "{}@{}:{}/".format(ssh_user, self.worker_ip, self._remote_dir)
