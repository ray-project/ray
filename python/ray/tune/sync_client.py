from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import subprocess
import tempfile
import types

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote
from ray.tune.error import TuneError

logger = logging.getLogger(__name__)


def noop(*args):
    return


def get_sync_client(sync_function):
    if not sync_function:
        return None
    if isinstance(sync_function, types.FunctionType):
        return FunctionBasedClient(sync_function, sync_function, noop)
    elif isinstance(sync_function, str):
        return CommandBasedClient(sync_function, sync_function)
    else:
        raise ValueError("Sync function {} must be string or function".format(
            sync_function))


class SyncClient(object):
    def sync_up(self, source, target):
        """Sync up from source to target.
        Args:
            source (str): Source path.
            target (str): Target path.
        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def sync_down(self, source, target):
        """Sync down from source to target.
        Args:
            source (str): Source path.
            target (str): Target path.
        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def delete(self, target):
        """Deletes target.

        Args:
            target (str): Target path.

        Returns:
            True if delete initiation successful, False otherwise.
        """
        raise NotImplementedError

    def wait(self):
        """Wait for current sync to complete, if asynchronously started."""
        pass

    def reset(self):
        """Resets state."""
        pass


class FunctionBasedClient(SyncClient):
    def __init__(self, sync_up_func, sync_down_func, delete_func):
        self.sync_up_func = sync_up_func
        self.sync_down_func = sync_down_func
        self.delete_func = delete_func

    def sync_up(self, source, target):
        self.sync_up_func(source, target)
        return True

    def sync_down(self, source, target):
        self.sync_down_func(source, target)
        return True

    def delete(self, target):
        self.delete_func(target)
        return True


NOOP = FunctionBasedClient(noop, noop, noop)


class CommandBasedClient(SyncClient):
    def __init__(self, sync_up_template, sync_down_template):
        """Syncs between two directories with the given command.
        Arguments:
            sync_up_template (str): A runnable string template; needs to
                include replacement fields '{source}' and '{target}'.
            sync_down_template (str): A runnable string template; needs to
                include replacement fields '{source}' and '{target}'.
        """
        self._validate_sync_string(sync_up_template)
        self._validate_sync_string(sync_down_template)
        self.sync_up_template = sync_up_template
        self.sync_down_template = sync_down_template
        self.logfile = None
        self.sync_process = None

    def set_logdir(self, logdir):
        """Sets the directory to log sync execution output in.
        Args:
            logdir (str): Log directory.
        """
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync", dir=logdir, suffix=".log", delete=False)

    def sync_up(self, source, target):
        return self._execute(self.sync_up_template, source, target)

    def sync_down(self, source, target):
        return self._execute(self.sync_down_template, source, target)

    def delete(self, target):
        pass

    def wait(self):
        if self.sync_process:
            _, error_msg = self.sync_process.communicate()
            error_msg = error_msg.decode("ascii")
            code = self.sync_process.returncode
            self.sync_process = None
            if code != 0:
                raise TuneError("Sync error ({}): {}".format(code, error_msg))

    def reset(self):
        if self.sync_process:
            logger.warning("Sync process still running but resetting anyways.")
            self.sync_process = None

    def _execute(self, sync_template, source, target):
        """Executes sync_template on source and target."""
        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                logger.warning("Last sync is still in progress, skipping.")
                return False
        final_cmd = sync_template.format(
            source=quote(source), target=quote(target))
        logger.debug("Running sync: {}".format(final_cmd))
        self.sync_process = subprocess.Popen(
            final_cmd, shell=True, stderr=subprocess.PIPE, stdout=self.logfile)
        return True

    @staticmethod
    def _validate_sync_string(sync_string):
        if not isinstance(sync_string, str):
            raise ValueError("{} is not a string.".format(sync_string))
        if "{source}" not in sync_string:
            raise ValueError("Sync template missing '{source}'.")
        if "{target}" not in sync_string:
            raise ValueError("Sync template missing '{target}'.")
