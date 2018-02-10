from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils.spawn
import os
import subprocess
import time

from ray.tune.error import TuneError
from ray.tune.result import DEFAULT_RESULTS_DIR


# Map from (logdir, remote_dir) -> syncer
_syncers = {}


def get_syncer(local_dir, remote_dir):
    if not remote_dir.startswith("s3://"):
        raise TuneError("Upload uri must start with s3://")

    if not distutils.spawn.find_executable("aws"):
        raise TuneError("Upload uri requires awscli tool to be installed")

    if local_dir.startswith(DEFAULT_RESULTS_DIR + "/"):
        rel_path = os.path.relpath(local_dir, DEFAULT_RESULTS_DIR)
        remote_dir = os.path.join(remote_dir, rel_path)

    key = (local_dir, remote_dir)
    if key not in _syncers:
        _syncers[key] = _S3LogSyncer(local_dir, remote_dir)

    return _syncers[key]


def wait_for_log_sync():
    for syncer in _syncers.values():
        syncer.wait()


class _S3LogSyncer(object):
    def __init__(self, local_dir, remote_dir):
        self.local_dir = local_dir
        self.remote_dir = remote_dir
        self.last_sync_time = 0
        self.sync_process = None
        print("Created S3LogSyncer for {} -> {}".format(local_dir, remote_dir))

    def sync_if_needed(self):
        if time.time() - self.last_sync_time > 300:
            self.sync_now()

    def sync_now(self, force=False):
        print(
            "Syncing files from {} -> {}".format(
                self.local_dir, self.remote_dir))
        self.last_sync_time = time.time()
        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                if force:
                    self.sync_process.kill()
                else:
                    print("Warning: last sync is still in progress, skipping")
                    return
        self.sync_process = subprocess.Popen(
            ["aws", "s3", "sync", self.local_dir, self.remote_dir])

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()
