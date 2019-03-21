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

# Map from (logdir, remote_dir) -> syncer
_syncers = {}

S3_PREFIX = "s3://"
GCS_PREFIX = "gs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GCS_PREFIX)


def get_syncer(local_dir, remote_dir=None, sync_function=None):
    if remote_dir:
        if not sync_function and not any(
                remote_dir.startswith(prefix)
                for prefix in ALLOWED_REMOTE_PREFIXES):
            raise TuneError("Upload uri must start with one of: {}"
                            "".format(ALLOWED_REMOTE_PREFIXES))

        if (remote_dir.startswith(S3_PREFIX)
                and not distutils.spawn.find_executable("aws")):
            raise TuneError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX))
        elif (remote_dir.startswith(GCS_PREFIX)
              and not distutils.spawn.find_executable("gsutil")):
            raise TuneError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GCS_PREFIX))

        if local_dir.startswith(DEFAULT_RESULTS_DIR + "/"):
            rel_path = os.path.relpath(local_dir, DEFAULT_RESULTS_DIR)
            remote_dir = os.path.join(remote_dir, rel_path)

    key = (local_dir, remote_dir)
    if key not in _syncers:
        _syncers[key] = _LogSyncer(local_dir, remote_dir, sync_function)

    return _syncers[key]


def wait_for_log_sync():
    for syncer in _syncers.values():
        syncer.wait()


def validate_sync_function(sync_function):
    if sync_function is None:
        return
    elif isinstance(sync_function, str):
        assert "{remote_dir}" in sync_function, (
            "Sync template missing '{remote_dir}'.")
        assert "{local_dir}" in sync_function, (
            "Sync template missing '{local_dir}'.")
    elif not (isinstance(sync_function, types.FunctionType)
              or isinstance(sync_function, tune_function)):
        raise ValueError("Sync function {} must be string or function".format(
            sync_function))


class _LogSyncer(object):
    """Log syncer for tune.

    This syncs files from workers to the local node, and optionally also from
    the local node to a remote directory (e.g. S3).

    Arguments:
        logdir (str): Directory to sync from.
        upload_uri (str): Directory to sync to.
        sync_function (func|str): Function for syncing the local_dir to
            upload_dir. If string, then it must be a string template
            for syncer to run and needs to include replacement fields
            '{local_dir}' and '{remote_dir}'.
    """

    def __init__(self, local_dir, remote_dir=None, sync_function=None):
        super(_LogSyncer, self).__init__(local_dir, remote_dir)

        # Resolve sync_function into template or function
        self.local_ip = ray.services.get_node_ip_address()
        self.worker_ip = None
        logger.debug("Created LogSyncer for {} -> {}".format(
            local_dir, remote_dir))

    def set_worker_ip(self, worker_ip):
        """Set the worker ip to sync logs from."""
        self.worker_ip = worker_ip

    def sync_up(self):
        """Syncs the local logdir on driver to worker if possible.

        Requires ray cluster to be started with the autoscaler. Also requires
        rsync to be installed.
        """
        if self.worker_ip == self.local_ip:
            return
        ssh_key = get_ssh_key()
        ssh_user = get_ssh_user()
        global _log_sync_warned
        if ssh_key is None or ssh_user is None:
            if not _log_sync_warned:
                logger.error("Log sync requires cluster to be setup with "
                             "`ray up`.")
                _log_sync_warned = True
            return
        if not distutils.spawn.find_executable("rsync"):
            logger.error("Log sync requires rsync to be installed.")
            return
        source = '{}/'.format(self.local_dir)
        target = '{}@{}:{}/'.format(ssh_user, self.worker_ip, self.local_dir)
        final_cmd = (("""rsync -savz -e "ssh -i {} -o ConnectTimeout=120s """
                      """-o StrictHostKeyChecking=no" {} {}""").format(
                          quote(ssh_key), quote(source), quote(target)))
        logger.info("Syncing results to %s", str(self.worker_ip))
        sync_process = subprocess.Popen(
            final_cmd, shell=True, stdout=self.logfile)
        sync_process.wait()

    def sync_down(self, force=False):
        if not self.worker_ip:
            logger.debug("Worker ip unknown, skipping log sync for {}".format(
                self.local_dir))
            return

        super(_LogSyncer, self).sync_down(force=force)

    def get_remote_sync_cmd(self):
        if self.worker_ip == self.local_ip:
            worker_to_local_sync_cmd = None  # don't need to rsync
        else:
            ssh_key = get_ssh_key()
            ssh_user = get_ssh_user()
            global _log_sync_warned
            if ssh_key is None or ssh_user is None:
                if not _log_sync_warned:
                    logger.error("Log sync requires cluster to be setup with "
                                 "`ray up`.")
                    _log_sync_warned = True
                return
            if not distutils.spawn.find_executable("rsync"):
                logger.error("Log sync requires rsync to be installed.")
                return
            source = '{}@{}:{}/'.format(ssh_user, self.worker_ip,
                                        self.local_dir)
            target = '{}/'.format(self.local_dir)
            worker_to_local_sync_cmd = ((
                """rsync -savz -e "ssh -i {} -o ConnectTimeout=120s """
                """-o StrictHostKeyChecking=no" {} {}""").format(
                    quote(ssh_key), quote(source), quote(target)))

        return worker_to_local_sync_cmd
