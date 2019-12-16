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


def log_sync_template(options=""):
    """Syncs the local_dir between driver and worker if possible.

    Requires ray cluster to be started with the autoscaler. Also requires
    rsync to be installed.

    Args:
        options (str): Addtional rsync options.

    Returns:
        Sync template with source and target parameters.
    """
    if not distutils.spawn.find_executable("rsync"):
        logger.error("Log sync requires rsync to be installed.")
        return
    global _log_sync_warned
    ssh_key = get_ssh_key()
    if ssh_key is None:
        if not _log_sync_warned:
            logger.debug("Log sync requires cluster to be setup with "
                         "`ray up`.")
            _log_sync_warned = True
        return

    rsh = "ssh -i {ssh_key} -o ConnectTimeout=120s -o StrictHostKeyChecking=no"
    rsh = rsh.format(ssh_key=quote(ssh_key))
    template = """rsync {options} -savz -e "{rsh}" {{source}} {{target}}"""
    return template.format(options=options, rsh=rsh)


class NodeSyncMixin(object):
    # TODO(ujvl): Refactor this code.
    """Mixin for syncing files to/from a remote dir to a local dir."""

    def __init__(self):
        assert hasattr(self, "_remote_dir"), "Mixin not mixed with Syncer."
        self.local_ip = ray.services.get_node_ip_address()
        self.worker_ip = None

    def set_worker_ip(self, worker_ip):
        """Set the worker ip to sync logs from."""
        self.worker_ip = worker_ip

    def has_remote_target(self):
        """Returns whether the Syncer has a remote target."""
        if not self.worker_ip:
            logger.debug("Worker IP unknown, skipping log sync for %s",
                         self._local_dir)
            return False
        if self.worker_ip == self.local_ip:
            logger.debug("Worker IP is local IP, skipping log sync for %s",
                         self._local_dir)
            return False
        return True

    def sync_up_if_needed(self):
        if not self.has_remote_target():
            return True
        super(NodeSyncMixin, self).sync_up()

    def sync_down_if_needed(self):
        if not self.has_remote_target():
            return True
        super(NodeSyncMixin, self).sync_down()

    def sync_down(self):
        if not self.has_remote_target():
            return True
        return super(NodeSyncMixin, self).sync_down()

    def sync_up(self):
        if not self.has_remote_target():
            return True
        return super(NodeSyncMixin, self).sync_up()

    @property
    def _remote_path(self):
        ssh_user = get_ssh_user()
        global _log_sync_warned
        if not self.has_remote_target():
            return
        if ssh_user is None:
            if not _log_sync_warned:
                logger.error("Log sync requires cluster to be setup with "
                             "`ray up`.")
                _log_sync_warned = True
            return
        return "{}@{}:{}/".format(ssh_user, self.worker_ip, self._remote_dir)
