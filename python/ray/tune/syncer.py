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

from ray.tune.sample import function as tune_function
from ray.tune.error import TuneError

logger = logging.getLogger(__name__)

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX)


def validate_sync_string(sync_string):
    if "{source}" not in sync_string:
        raise ValueError("Sync template missing '{source}'.")
    if "{target}" not in sync_string:
        raise ValueError("Sync template missing '{target}'.")


class BaseSyncer(object):
    def __init__(self, local_dir, remote_dir, sync_function=None):
        """
        Arguments:
            local_dir (str): Directory to sync. Uniquely identifies the syncer.
            remote_dir (str): Remote directory to sync with.
            sync_function (func): Function for syncing the local_dir to
                remote_dir. Defaults to a Noop.
        """
        self._local_dir = os.path.join(local_dir, "")
        self._remote_dir = remote_dir
        self.last_sync_up_time = 0
        self.last_sync_down_time = 0
        self._sync_function = sync_function or (lambda source, target: None)

    def sync_function(self, source, target):
        """Executes sync between source and target.

        Can be overwritten by subclasses for custom sync procedures.

        Args:
            source: Path to source file(s).
            target: Path to target file(s).
        """
        if self._sync_function:
            return self._sync_function(source, target)

    def sync(self, source, target):
        if not (source and target):
            logger.debug(
                "Source or target is empty, skipping log sync for {}".format(
                    self._local_dir))
            return

        try:
            self.sync_function(source, target)
            return True
        except Exception:
            logger.exception("Sync function failed.")

    def sync_up_if_needed(self):
        if time.time() - self.last_sync_up_time > 300:
            self.sync_up()

    def sync_down_if_needed(self):
        if time.time(
        ) - self.last_sync_down_time > 300:  # Maybe change in future
            self.sync_down()

    def sync_down(self, *args, **kwargs):
        self.sync(self._remote_path, self._local_dir, *args, **kwargs)
        self.last_sync_down_time = time.time()

    def sync_up(self, *args, **kwargs):
        self.sync(self._local_dir, self._remote_path, *args, **kwargs)
        self.last_sync_up_time = time.time()

    def close(self):
        pass

    def wait(self):
        pass

    @property
    def _remote_path(self):
        """Protected method for accessing remote_dir.

        Can be overridden in subclass for custom path.
        """
        return self._remote_dir


class CommandSyncer(BaseSyncer):
    def __init__(self, local_dir, remote_dir, sync_template=None):
        """
        Arguments:
            local_dir (str): Directory to sync.
            remote_dir (str): Remote directory to sync with.
            sync_template (str): A string template
                for syncer to run and needs to include replacement fields
                '{source}' and '{target}'. Returned when using
                `CommandSyncer.sync_template`, which can be overridden
                by subclass.
        """
        super(CommandSyncer, self).__init__(local_dir, remote_dir)
        if sync_template and isinstance(sync_template, str):
            validate_sync_string(sync_template)
        self._sync_template = sync_template
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync", dir=self._local_dir, suffix=".log", delete=False)

        self.sync_process = None

    def sync_function(self, source, target):
        self.last_sync_time = time.time()
        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                logger.warning("Last sync is still in progress, skipping.")
                return
        sync_template = self.sync_template
        final_cmd = sync_template.format(
            source=quote(source), target=quote(target))
        logger.debug("Running sync: {}".format(final_cmd))
        self.sync_process = subprocess.Popen(
            final_cmd, shell=True, stdout=self.logfile)
        return True

    @property
    def sync_template(self):
        """Template for executing a sync command.

        Can be overwritten by subclass for custom behavior.
        """
        return self._sync_template

    def close(self):
        self.logfile.close()

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()


def get_syncer(local_dir, remote_dir, sync_function=None):
    """This returns a Syncer depending on given args.

    Args:
        local_dir: Source directory for syncing.
        remote_dir: Target directory for syncing. If None,
            returns NoopSyncer.
        sync_function (func | str): Function for syncing the local_dir to
            remote_dir. If string, then it must be a string template for
            syncer to run. If not provided, it defaults
            to standard S3 or gsutil sync commands.
        """
    if not remote_dir:
        return BaseSyncer(None, None)

    if sync_function:
        if isinstance(sync_function, types.FunctionType) or isinstance(
                sync_function, tune_function):
            return BaseSyncer(local_dir, remote_dir, sync_function)
        elif isinstance(sync_function, str):
            return CommandSyncer(local_dir, remote_dir, sync_function)
        else:
            raise ValueError(
                "Sync function {} must be string or function".format(
                    sync_function))

    if remote_dir.startswith(S3_PREFIX):
        if not distutils.spawn.find_executable("aws"):
            raise TuneError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX))
        return CommandSyncer(local_dir, remote_dir,
                             "aws s3 sync {source} {target}")
    elif remote_dir.startswith(GS_PREFIX):
        if not distutils.spawn.find_executable("gsutil"):
            raise TuneError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GS_PREFIX))
        return CommandSyncer(local_dir, remote_dir,
                             "gsutil rsync -r {source} {target}")
    else:
        raise TuneError("Upload uri must start with one of: {}"
                        "".format(ALLOWED_REMOTE_PREFIXES))
