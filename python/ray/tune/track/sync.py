from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import distutils.spawn
import subprocess
import time

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote

from track.error import TrackError

S3_PREFIX = "s3://"
GCS_PREFIX = "gs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GCS_PREFIX)


def check_remote_util(remote_dir):
    if not any(
            remote_dir.startswith(prefix)
            for prefix in ALLOWED_REMOTE_PREFIXES):
        raise TrackError("Upload uri must start with one of: {}"
                         "".format(ALLOWED_REMOTE_PREFIXES))

    if (remote_dir.startswith(S3_PREFIX)
            and not distutils.spawn.find_executable("aws")):
        raise TrackError(
            "Upload uri starting with '{}' requires awscli tool"
            " to be installed".format(S3_PREFIX))
    elif (remote_dir.startswith(GCS_PREFIX)
          and not distutils.spawn.find_executable("gsutil")):
        raise TrackError(
            "Upload uri starting with '{}' requires gsutil tool"
            " to be installed".format(GCS_PREFIX))


class _LogSyncer(object):
    """Log syncer.

    This syncs files from the local node to a remote directory (e.g. S3)."""

    def __init__(self, local_dir, remote_dir=None, sync_period=None):
        self.local_dir = local_dir
        if remote_dir:
            check_remote_util(remote_dir)
        self.remote_dir = remote_dir
        self.last_sync_time = 0
        self.sync_period = sync_period or 300
        self.sync_process = None
        print("Created LogSyncer for {} -> {}".format(local_dir, remote_dir))

    def sync_if_needed(self):
        if time.time() - self.last_sync_time > self.sync_period:
            self.sync_now()

    def sync_now(self, force=False):
        self.last_sync_time = time.time()
        local_to_remote_sync_cmd = None
        if self.remote_dir:
            if self.remote_dir.startswith(S3_PREFIX):
                local_to_remote_sync_cmd = ("aws s3 sync {} {}".format(
                    quote(self.local_dir), quote(self.remote_dir)))
            elif self.remote_dir.startswith(GCS_PREFIX):
                local_to_remote_sync_cmd = ("gsutil rsync -r {} {}".format(
                    quote(self.local_dir), quote(self.remote_dir)))

        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                if force:
                    self.sync_process.kill()
                else:
                    print("Warning: last sync is still in progress, skipping")
                    return

        if local_to_remote_sync_cmd:
            final_cmd = local_to_remote_sync_cmd
            print("Running log sync: {}".format(final_cmd))
            self.sync_process = subprocess.Popen(final_cmd, shell=True)

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()


class SyncHook(object):
    def __init__(self, local_dir, remote_dir=None, sync_period=None):
        self._logsync = _LogSyncer(local_dir, remote_dir, sync_period)

    def on_result(self, *args, **kwargs):
        self._logsync.sync_if_needed()

    def close(self):
        self._logsync.sync_now(force=True)
        self._logsync.wait()
        print("Syncer closed.")
