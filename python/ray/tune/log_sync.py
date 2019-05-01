from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils.spawn
import logging
import os

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

import ray
from ray.tune.syncer import CommandSyncer
from ray.tune.cluster_info import get_ssh_key, get_ssh_user
from ray.tune.result import DEFAULT_RESULTS_DIR

logger = logging.getLogger(__name__)
_log_sync_warned = False

# Map from (local_dir, remote_dir) -> syncer
_syncers = {}


def get_log_syncer(local_dir, remote_dir=None, sync_function=None):
    key = (local_dir, remote_dir)
    if key not in _syncers:
        _syncers[key] = LogSyncer(local_dir, remote_dir, sync_function)

    return _syncers[key]


def wait_for_log_sync():
    for syncer in _syncers.values():
        syncer.wait()


class LogSyncer(CommandSyncer):
    """This syncs files to and from a remote directory to a local directory.

    Attributes:
        remote_path:
        local_dir:
    """

    def __init__(self, *args, **kwargs):
        super(LogSyncer, self).__init__(*args, **kwargs)

        # Resolve sync_function into template or function
        self.local_ip = ray.services.get_node_ip_address()
        self.worker_ip = None

    def set_worker_ip(self, worker_ip):
        """Set the worker ip to sync logs from."""
        self.worker_ip = worker_ip

    def get_remote_sync_template(self):
        """Syncs the local local_dir on driver to worker if possible.

        Requires ray cluster to be started with the autoscaler. Also requires
        rsync to be installed.

        TODO:
            Make this command more flexible with self.sync_func?
        """

        if not self.worker_ip:
            logger.info("Worker ip unknown, skipping log sync for {}".format(
                self.local_dir))
            return
        if self.worker_ip == self.local_ip:
            logger.debug(
                "Worker ip is local ip, skipping log sync for {}".format(
                    self.local_dir))
            return
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

        return ("""rsync -savz -e "ssh -i {ssh_key} -o ConnectTimeout=120s """
                """-o StrictHostKeyChecking=no" {{source}} {{target}}"""
                ).format(ssh_key=quote(ssh_key))

    @property
    def remote_path(self):
        ssh_user = get_ssh_user()
        global _log_sync_warned
        if ssh_user is None:
            if not _log_sync_warned:
                logger.error("Log sync requires cluster to be setup with "
                             "`ray up`.")
                _log_sync_warned = True
            return
        return '{}@{}:{}/'.format(ssh_user, self.worker_ip, self._remote_dir)
