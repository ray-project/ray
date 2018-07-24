from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils.spawn
import os
import subprocess
import time

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

import ray
from ray.tune.cluster_info import get_ssh_key, get_ssh_user
from ray.tune.error import TuneError
from ray.tune.result import DEFAULT_RESULTS_DIR

# Map from (logdir, remote_dir) -> syncer
_syncers = {}

S3_PREFIX = "s3://"
GCS_PREFIX = "gs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GCS_PREFIX)


def get_syncer(local_dir, remote_dir=None):
    if remote_dir:
        if not any(
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
        _syncers[key] = _LogSyncer(local_dir, remote_dir)

    return _syncers[key]


def wait_for_log_sync():
    for syncer in _syncers.values():
        syncer.wait()


class _LogSyncer(object):
    """Log syncer for tune.

    This syncs files from workers to the local node, and optionally also from
    the local node to a remote directory (e.g. S3)."""

    def __init__(self, local_dir, remote_dir=None):
        self.local_dir = local_dir
        self.remote_dir = remote_dir
        self.last_sync_time = 0
        self.sync_process = None
        self.local_ip = ray.services.get_node_ip_address()
        self.worker_ip = None
        print("Created LogSyncer for {} -> {}".format(local_dir, remote_dir))

    def set_worker_ip(self, worker_ip):
        """Set the worker ip to sync logs from."""

        self.worker_ip = worker_ip

    def sync_if_needed(self):
        if time.time() - self.last_sync_time > 300:
            self.sync_now()

    def sync_now(self, force=False):
        self.last_sync_time = time.time()
        if not self.worker_ip:
            print("Worker ip unknown, skipping log sync for {}".format(
                self.local_dir))
            return

        if self.worker_ip == self.local_ip:
            worker_to_local_sync_cmd = None  # don't need to rsync
        else:
            ssh_key = get_ssh_key()
            ssh_user = get_ssh_user()
            if ssh_key is None or ssh_user is None:
                print("Error: log sync requires cluster to be setup with "
                      "`ray create_or_update`.")
                return
            if not distutils.spawn.find_executable("rsync"):
                print("Error: log sync requires rsync to be installed.")
                return
            worker_to_local_sync_cmd = ((
                """rsync -avz -e "ssh -i {} -o ConnectTimeout=120s """
                """-o StrictHostKeyChecking=no" '{}@{}:{}/' '{}/'""").format(
                    quote(ssh_key), ssh_user, self.worker_ip,
                    quote(self.local_dir), quote(self.local_dir)))

        if self.remote_dir:
            if self.remote_dir.startswith(S3_PREFIX):
                local_to_remote_sync_cmd = ("aws s3 sync {} {}".format(
                    quote(self.local_dir), quote(self.remote_dir)))
            elif self.remote_dir.startswith(GCS_PREFIX):
                local_to_remote_sync_cmd = ("gsutil rsync -r {} {}".format(
                    quote(self.local_dir), quote(self.remote_dir)))
        else:
            local_to_remote_sync_cmd = None

        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                if force:
                    self.sync_process.kill()
                else:
                    print("Warning: last sync is still in progress, skipping")
                    return

        if worker_to_local_sync_cmd or local_to_remote_sync_cmd:
            final_cmd = ""
            if worker_to_local_sync_cmd:
                final_cmd += worker_to_local_sync_cmd
            if local_to_remote_sync_cmd:
                if final_cmd:
                    final_cmd += " && "
                final_cmd += local_to_remote_sync_cmd
            print("Running log sync: {}".format(final_cmd))
            self.sync_process = subprocess.Popen(final_cmd, shell=True)

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()
