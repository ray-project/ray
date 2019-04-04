from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils.spawn
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

import ray
from ray.tune.cluster_info import get_ssh_key, get_ssh_user
from ray.tune.error import TuneError
from ray.tune.result import DEFAULT_RESULTS_DIR
from ray.tune.suggest.variant_generator import function as tune_function

logger = logging.getLogger(__name__)
_log_sync_warned = False

# Map from (local_dir, remote_dir) -> syncer
_syncers = {}

### START TODO


def get_log_syncer(local_dir, remote_dir=None, sync_function=None):
    if remote_dir:
        if local_dir.startswith(DEFAULT_RESULTS_DIR + "/"):
            rel_path = os.path.relpath(local_dir, DEFAULT_RESULTS_DIR)
            remote_dir = os.path.join(remote_dir, rel_path)

    key = (local_dir, remote_dir)
    if key not in _syncers:
        _syncers[key] = LogSyncer(local_dir, remote_dir, sync_function)

    return _syncers[key]


### END TODO


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
        super(_LogSyncer, self).__init__(*args, **kwargs)

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
        ssh_key = get_ssh_key()
        if ssh_pass is None:
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
        if ssh_user is None:
            if not _log_sync_warned:
                logger.error("Log sync requires cluster to be setup with "
                             "`ray up`.")
                _log_sync_warned = True
            return
        return '{}@{}:{}/'.format(ssh_user, self.worker_ip, self.remote_dir)
