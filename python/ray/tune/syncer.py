import time
import logger
import subprocess
from ray.tune import tune_function
import types
import tempfile
import os


### START TODO
S3_PREFIX = "s3://"
GS_PREFIX = "gs://"


def get_syncer(local_dir, remote_dir, *args, **kwargs):
    if remote_dir.startswith(S3_PREFIX):
        return CommandSyncer(local_dir, remote_dir, "aws s3 sync {source} {target}")
    elif remote_dir.startswith(GS_PREFIX)
        return CommandSyncer(local_dir, remote_dir, "gsutil rsync -r {source} {target}")
    # TODO: Find out where this switch logic occurs!
    if isinstance(sync_fn, types.FunctionType) or isinstance(
            sync_fn, tune_function):
        self.sync_fn = sync_fn
    elif isinstance(sync_fn, str):
        self.sync_cmd_tmpl = sync_fn
    else:
        raise ValueError(type(sync_fn))



def validate_sync_function(sync_function):
    if sync_function is None:
        return
    elif isinstance(sync_function, str):
        assert "{source}" in sync_function, (
            "Sync template missing '{source}'.")
        assert "{target}" in sync_function, (
            "Sync template missing '{target}'.")
    elif not (isinstance(sync_function, types.FunctionType)
              or isinstance(sync_function, tune_function)):
        raise ValueError("Sync function {} must be string or function".format(
            sync_function))


### END TODO


class BaseSyncer(object):
    def __init__(self, local_dir, remote_dir, sync_fn=None):
        """
        Arguments:
            local_dir (str): Directory to sync. Uniquely identifies the syncer.
            remote_dir (str): Remote directory to sync with.
            sync_fn (func): Function for syncing the local_dir to
                remote_dir.
        """
        self._local_dir = os.path.join(local_dir, '')
        self._remote_dir = remote_dir or self._local_dir  # TODO(rliaw) - double check
        self.last_sync_time = 0
        if sync_fn:  # TODO(rliaw): Check on this
            self.sync_fn = sync_fn

    def sync(self, source, target):
        self.last_sync_time = time.time()

        if not (source and target):
            logger.debug("Source or target is empty, skipping log sync for {}".format(
                self.local_dir))
            return

        try:
            self.sync_fn(source, target)
            return True
        except Exception:
            logger.exception("Sync function failed.")

    def sync_if_needed(self):
        if time.time() - self.last_sync_time > 300:
            self.sync_down()

    def sync_down(self, *args, **kwargs):
        self.sync(self.remote_path, self.local_dir, *args, **kwargs)

    def sync_up(self, *args, **kwargs):
        self.sync(self.local_dir, self.remote_path, *args, **kwargs)

    def close(self):
        pass

    def wait(self):
        pass

    @property
    def local_dir(self):
        return self._local_dir

    @property
    def remote_path(self):
        return self._remote_dir


class CommandSyncer(BaseSyncer):
    def __init__(self, local_dir, remote_dir, sync_cmd_tmpl=None):
        """
        Arguments:
            local_dir (str): Directory to sync.
            remote_dir (str): Remote directory to sync with.
            sync_cmd_tmpl (str): A string template
                for syncer to run and needs to include replacement fields
                '{local_dir}' and '{remote_dir}'.
        """
        super(CommandSyncer, self).__init(local_dir, remote_dir)
        self.sync_cmd_tmpl = sync_cmd_tmpl
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync", dir=self.local_dir, suffix=".log", delete=False)

        self.sync_process = None

    def sync_fn(self, source, target):
        self.last_sync_time = time.time()
        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                if force:
                    self.sync_process.kill()
                else:
                    logger.warning("Last sync is still in progress, skipping.")
                    return

        sync_template = self.get_remote_sync_template()
        final_cmd = sync_template.format(
            source=quote(source),
            target=quote(target))
        logger.debug("Running sync: {}".format(final_cmd))
        self.sync_process = subprocess.Popen(
            final_cmd, shell=True, stdout=self.logfile)
        return True

    def get_remote_sync_template(self):
        return self.sync_cmd_tmpl

    def close(self):
        self.logfile.close()

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()
