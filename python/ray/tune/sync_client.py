import distutils
import distutils.spawn
import inspect
import logging
import pathlib
import subprocess
import tempfile
import types
import warnings

from typing import Optional, List

from shlex import quote

from ray.tune.error import TuneError
from ray.util.annotations import PublicAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
HDFS_PREFIX = "hdfs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX, HDFS_PREFIX)

noop_template = ": {target}"  # noop in bash


def noop(*args):
    return


def get_sync_client(sync_function, delete_function=None):
    """Returns a sync client.

    Args:
        sync_function (Optional[str|function]): Sync function.
        delete_function (Optional[str|function]): Delete function. Must be
            the same type as sync_function if it is provided.

    Raises:
        ValueError if sync_function or delete_function are malformed.
    """
    if sync_function is None:
        return None
    if delete_function and type(sync_function) != type(delete_function):
        raise ValueError("Sync and delete functions must be of same type.")
    if isinstance(sync_function, types.FunctionType):
        delete_function = delete_function or noop
        client_cls = FunctionBasedClient
    elif isinstance(sync_function, str):
        delete_function = delete_function or noop_template
        client_cls = CommandBasedClient
    else:
        raise ValueError("Sync function {} must be string or function".format(
            sync_function))
    return client_cls(sync_function, sync_function, delete_function)


def get_cloud_sync_client(remote_path):
    """Returns a CommandBasedClient that can sync to/from remote storage.

    Args:
        remote_path (str): Path to remote storage (S3, GS or HDFS).

    Raises:
        ValueError if malformed remote_dir.
    """
    if remote_path.startswith(S3_PREFIX):
        if not distutils.spawn.find_executable("aws"):
            raise ValueError(
                "Upload uri starting with '{}' requires awscli tool"
                " to be installed".format(S3_PREFIX))
        sync_up_template = ("aws s3 sync {source} {target} "
                            "--only-show-errors {options}")
        sync_down_template = sync_up_template
        delete_template = ("aws s3 rm {target} --recursive "
                           "--only-show-errors {options}")
        exclude_template = "--exclude '{pattern}'"
    elif remote_path.startswith(GS_PREFIX):
        if not distutils.spawn.find_executable("gsutil"):
            raise ValueError(
                "Upload uri starting with '{}' requires gsutil tool"
                " to be installed".format(GS_PREFIX))
        sync_up_template = "gsutil rsync -r {options} {source} {target}"
        sync_down_template = sync_up_template
        delete_template = "gsutil rm -r {options} {target}"
        exclude_template = "-x '{regex_pattern}'"
    elif remote_path.startswith(HDFS_PREFIX):
        if not distutils.spawn.find_executable("hdfs"):
            raise ValueError("Upload uri starting with '{}' requires hdfs tool"
                             " to be installed".format(HDFS_PREFIX))
        sync_up_template = "hdfs dfs -put -f {source} {target}"
        sync_down_template = "hdfs dfs -get -f {source} {target}"
        delete_template = "hdfs dfs -rm -r {target}"
        exclude_template = None
    else:
        raise ValueError(
            f"Upload uri must start with one of: {ALLOWED_REMOTE_PREFIXES} "
            f"(is: `{remote_path}`)")
    return CommandBasedClient(sync_up_template, sync_down_template,
                              delete_template, exclude_template)


@PublicAPI(stability="beta")
class SyncClient:
    """Client interface for interacting with remote storage options."""

    def sync_up(self, source, target, exclude: Optional[List] = None):
        """Syncs up from source to target.

        Args:
            source (str): Source path.
            target (str): Target path.
            exclude (List[str]): Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.

        Returns:
            True if sync initiation successful, False otherwise.
        """
        raise NotImplementedError

    def sync_down(self, source, target, exclude: Optional[List] = None):
        """Syncs down from source to target.

        Args:
            source (str): Source path.
            target (str): Target path.
            exclude (List[str]): Pattern of files to exclude, e.g.
                ``["*/checkpoint_*]`` to exclude trial checkpoints.

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
        """Waits for current sync to complete, if asynchronously started."""
        pass

    def reset(self):
        """Resets state."""
        pass

    def close(self):
        """Clean up hook."""
        pass


def _is_legacy_sync_fn(func) -> bool:
    sig = inspect.signature(func)
    try:
        sig.bind_partial(None, None, None)
        return False
    except TypeError:
        return True


class FunctionBasedClient(SyncClient):
    def __init__(self, sync_up_func, sync_down_func, delete_func=None):
        self.sync_up_func = sync_up_func
        self._sync_up_legacy = _is_legacy_sync_fn(sync_up_func)

        self.sync_down_func = sync_down_func
        self._sync_down_legacy = _is_legacy_sync_fn(sync_up_func)

        if self._sync_up_legacy or self._sync_down_legacy:
            if log_once("func_sync_up_legacy"):
                warnings.warn(
                    "Your sync functions currently only accepts two params "
                    "(a `source` and a `target`). In the future, we will "
                    "pass an additional `exclude` parameter. Please adjust "
                    "your sync function accordingly.")

        self.delete_func = delete_func or noop

    def sync_up(self, source, target, exclude: Optional[List] = None):
        if self._sync_up_legacy:
            self.sync_up_func(source, target)
        else:
            self.sync_up_func(source, target, exclude)
        return True

    def sync_down(self, source, target, exclude: Optional[List] = None):
        if self._sync_down_legacy:
            self.sync_down_func(source, target)
        else:
            self.sync_down_func(source, target, exclude)
        return True

    def delete(self, target):
        self.delete_func(target)
        return True


NOOP = FunctionBasedClient(noop, noop)


class CommandBasedClient(SyncClient):
    def __init__(self,
                 sync_up_template: str,
                 sync_down_template: str,
                 delete_template: Optional[str] = noop_template,
                 exclude_template: Optional[str] = None):
        """Syncs between two directories with the given command.

        Arguments:
            sync_up_template (str): A runnable string template; needs to
                include replacement fields ``{source}``, ``{target}``, and
                ``{options}``.
            sync_down_template (str): A runnable string template; needs to
                include replacement fields ``{source}``, ``{target}``, and
                ``{options}``.
            delete_template (Optional[str]): A runnable string template; needs
                to include replacement field ``{target}``. Noop by default.
            exclude_template (Optional[str]): A pattern with possible
                replacement fields ``{pattern}`` and ``{regex_pattern}``.
                Will replace ``{options}}`` in the sync up/down templates
                if files/directories to exclude are passed.
        """
        self._validate_sync_string(sync_up_template)
        self._validate_sync_string(sync_down_template)
        self._validate_exclude_template(exclude_template)
        self.sync_up_template = sync_up_template
        self.sync_down_template = sync_down_template
        self.delete_template = delete_template
        self.exclude_template = exclude_template
        self.logfile = None
        self._closed = False
        self.cmd_process = None

    def set_logdir(self, logdir):
        """Sets the directory to log sync execution output in.

        Args:
            logdir (str): Log directory.
        """
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync_out", dir=logdir, suffix=".log", delete=False)
        self._closed = False

    def _get_logfile(self):
        if self._closed:
            raise RuntimeError(
                "[internalerror] The client has been closed. "
                "Please report this stacktrace + your cluster configuration "
                "on Github!")
        else:
            return self.logfile

    def sync_up(self, source, target, exclude: Optional[List] = None):
        return self._execute(self.sync_up_template, source, target, exclude)

    def sync_down(self, source, target, exclude: Optional[List] = None):
        # Just in case some command line sync client expects that local
        # directory exists.
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)
        return self._execute(self.sync_down_template, source, target, exclude)

    def delete(self, target):
        if self.is_running:
            logger.warning("Last sync client cmd still in progress, skipping.")
            return False
        final_cmd = self.delete_template.format(
            target=quote(target), options="")
        logger.debug("Running delete: {}".format(final_cmd))
        self.cmd_process = subprocess.Popen(
            final_cmd,
            shell=True,
            stderr=subprocess.PIPE,
            stdout=self._get_logfile())
        return True

    def wait(self):
        if self.cmd_process:
            _, error_msg = self.cmd_process.communicate()
            error_msg = error_msg.decode("ascii")
            code = self.cmd_process.returncode
            args = self.cmd_process.args
            self.cmd_process = None
            if code != 0:
                raise TuneError("Sync error. Ran command: {}\n"
                                "Error message ({}): {}".format(
                                    args, code, error_msg))

    def reset(self):
        if self.is_running:
            logger.warning("Sync process still running but resetting anyways.")
        self.cmd_process = None

    def close(self):
        if self.logfile:
            logger.debug(f"Closing the logfile: {str(self.logfile)}")
            self.logfile.close()
            self.logfile = None
            self._closed = True

    @property
    def is_running(self):
        """Returns whether a sync or delete process is running."""
        if self.cmd_process:
            self.cmd_process.poll()
            return self.cmd_process.returncode is None
        return False

    def _execute(self,
                 sync_template,
                 source,
                 target,
                 exclude: Optional[List] = None):
        """Executes sync_template on source and target."""
        if self.is_running:
            logger.warning("Last sync client cmd still in progress, skipping.")
            return False

        if exclude and self.exclude_template:
            options = []
            if "{pattern}" in self.exclude_template:
                for excl in exclude:
                    options.append(self.exclude_template.format(pattern=excl))
            elif "{regex_pattern}" in self.exclude_template:
                # This is obviously not a great way to convert to regex,
                # but it will do for the moment. Todo: Improve.
                def _to_regex(pattern: str) -> str:
                    return f"({pattern.replace('*', '.*')})"

                regex_pattern = "|".join(_to_regex(excl) for excl in exclude)
                options.append(
                    self.exclude_template.format(regex_pattern=regex_pattern))
            option_str = " ".join(options)
        else:
            option_str = ""

        final_cmd = sync_template.format(
            source=quote(source), target=quote(target), options=option_str)
        logger.debug("Running sync: {}".format(final_cmd))
        self.cmd_process = subprocess.Popen(
            final_cmd,
            shell=True,
            stderr=subprocess.PIPE,
            stdout=self._get_logfile())
        return True

    @staticmethod
    def _validate_sync_string(sync_string):
        if not isinstance(sync_string, str):
            raise ValueError("{} is not a string.".format(sync_string))
        if "{source}" not in sync_string:
            raise ValueError("Sync template missing `{source}`: "
                             f"{sync_string}.")
        if "{target}" not in sync_string:
            raise ValueError("Sync template missing `{target}`: "
                             f"{sync_string}.")

    @staticmethod
    def _validate_exclude_template(exclude_template):
        if exclude_template:
            if ("{pattern}" not in exclude_template
                    and "{regex_pattern}" not in exclude_template):
                raise ValueError(
                    "Neither `{pattern}` nor `{regex_pattern}` found in "
                    f"exclude string `{exclude_template}`")
