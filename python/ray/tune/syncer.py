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

from ray.tune.error import TuneError
from ray.tune.log_sync import log_sync_template, NodeSyncMixin

logger = logging.getLogger(__name__)

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX)
SYNC_PERIOD = 300

_syncers = {}


def validate_sync_string(sync_string):
    if "{source}" not in sync_string:
        raise ValueError("Sync template missing '{source}'.")
    if "{target}" not in sync_string:
        raise ValueError("Sync template missing '{target}'.")


def wait_for_sync():
    for syncer in _syncers.values():
        syncer.wait()


class BaseSyncer(object):
    def __init__(self, local_dir, remote_dir, sync_function=None):
        """Syncs between two directories with the sync_function.

        Arguments:
            local_dir (str): Directory to sync. Uniquely identifies the syncer.
            remote_dir (str): Remote directory to sync with.
            sync_function (func): Function for syncing the local_dir to
                remote_dir. Defaults to a Noop.
        """
        self._local_dir = (os.path.join(local_dir, "")
                           if local_dir else local_dir)
        self._remote_dir = remote_dir
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")
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
        if time.time() - self.last_sync_up_time > SYNC_PERIOD:
            self.sync_up()

    def sync_down_if_needed(self):
        if time.time() - self.last_sync_down_time > SYNC_PERIOD:
            self.sync_down()

    def sync_down(self, *args, **kwargs):
        self.sync(self._remote_path, self._local_dir, *args, **kwargs)
        self.last_sync_down_time = time.time()

    def sync_up(self, *args, **kwargs):
        self.sync(self._local_dir, self._remote_path, *args, **kwargs)
        self.last_sync_up_time = time.time()

    def reset(self):
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")

    def wait(self):
        pass

    @property
    def _remote_path(self):
        """Protected method for accessing remote_dir.

        Can be overridden in subclass for custom path.
        """
        return self._remote_dir


class CommandSyncer(BaseSyncer):
    def __init__(self, local_dir, remote_dir, sync_template):
        """Syncs between two directories with the given command.

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
        if not isinstance(sync_template, str):
            raise ValueError("{} is not a string.".format(sync_template))
        validate_sync_string(sync_template)
        self._sync_template = sync_template
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync",
            dir=self._local_dir,
            suffix=".log",
            delete=False)

        self.sync_process = None

    def sync_function(self, source, target):
        self.last_sync_time = time.time()
        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                logger.warning("Last sync is still in progress, skipping.")
                return
        final_cmd = self._sync_template.format(
            source=quote(source), target=quote(target))
        logger.debug("Running sync: {}".format(final_cmd))
        self.sync_process = subprocess.Popen(
            final_cmd, shell=True, stdout=self.logfile)
        return True

    def reset(self):
        if self.sync_process:
            logger.warning("Sync process still running but resetting anyways.")
            self.sync_process = None
        super(CommandSyncer, self).reset()

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()


def _get_sync_cls(sync_function):
    if not sync_function:
        return
    if isinstance(sync_function, types.FunctionType):
        return BaseSyncer
    elif isinstance(sync_function, str):
        return CommandSyncer
    else:
        raise ValueError("Sync function {} must be string or function".format(
            sync_function))


def get_syncer(local_dir, remote_dir=None, sync_function=None):
    """Returns a Syncer depending on given args.

    This syncer is in charge of syncing the local_dir with upload_dir.

    Args:
        local_dir: Source directory for syncing.
        remote_dir: Target directory for syncing. If None,
            returns BaseSyncer with a noop.
        sync_function (func | str): Function for syncing the local_dir to
            remote_dir. If string, then it must be a string template for
            syncer to run. If not provided, it defaults
            to standard S3 or gsutil sync commands.
        """
    key = (local_dir, remote_dir)

    if key in _syncers:
        return _syncers[key]

    if not remote_dir:
        _syncers[key] = BaseSyncer(local_dir, remote_dir)
        return _syncers[key]

    sync_cls = _get_sync_cls(sync_function)

    if sync_cls:
        _syncers[key] = sync_cls(local_dir, remote_dir, sync_function)
        return _syncers[key]

    if remote_dir.startswith(S3_PREFIX):
        if not distutils.spawn.find_executable("aws"):
            raise TuneError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX))
        _syncers[key] = CommandSyncer(local_dir, remote_dir,
                                      "aws s3 sync {source} {target}")
    elif remote_dir.startswith(GS_PREFIX):
        if not distutils.spawn.find_executable("gsutil"):
            raise TuneError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GS_PREFIX))
        _syncers[key] = CommandSyncer(local_dir, remote_dir,
                                      "gsutil rsync -r {source} {target}")
    else:
        raise TuneError("Upload uri must start with one of: {}"
                        "".format(ALLOWED_REMOTE_PREFIXES))

    return _syncers[key]


def get_log_syncer(local_dir, remote_dir=None, sync_function=None):
    """Returns a Syncer depending on given args.

    This syncer is in charge of syncing the local_dir with remote local_dir.

    Args:
        local_dir: Source directory for syncing.
        remote_dir: Target directory for syncing. If None,
            returns BaseSyncer with noop.
        sync_function (func | str): Function for syncing the local_dir to
            remote_dir. If string, then it must be a string template for
            syncer to run. If not provided, it defaults rsync.
        """
    key = (local_dir, remote_dir)

    if key in _syncers:
        return _syncers[key]

    sync_cls = None
    if sync_function:
        sync_cls = _get_sync_cls(sync_function)
    else:
        sync_cls = CommandSyncer
        sync_function = log_sync_template()

    if not remote_dir or sync_function is None:
        sync_cls = BaseSyncer

    class MixedSyncer(NodeSyncMixin, sync_cls):
        def __init__(self, *args, **kwargs):
            sync_cls.__init__(self, *args, **kwargs)
            NodeSyncMixin.__init__(self)

    _syncers[key] = MixedSyncer(local_dir, remote_dir, sync_function)
    return _syncers[key]
