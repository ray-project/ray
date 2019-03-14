try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote
import os
import subprocess
import json

import pandas as pd

from . import constants
from .autodetect import dfl_local_dir
from .sync import S3_PREFIX, GCS_PREFIX, check_remote_util

class Project(object):
    """
    The project class manages all trials that have been run with the given
    log_dir and upload_dir. It gives pandas-dataframe access to trial metadata,
    metrics, and then path-based access to stored user artifacts for each trial.

    log_dir is created with the same defaults as in track.Trial
    """

    def __init__(self, log_dir=None, upload_dir=None):
        if log_dir is None:
            log_dir = dfl_local_dir()
        self.log_dir = log_dir
        if upload_dir:
            check_remote_util(upload_dir)
        self.upload_dir = upload_dir
        self._sync_metadata()
        self._ids = self._load_metadata()

    @property
    def ids(self):
        return self._ids

    def results(self, trial_ids):
        """
        Accepts a sequence of trial ids and returns a pandas dataframe
        with the schema

        trial_id, iteration?, *metric_schema_union

        where iteration is an optional column that specifies the iteration
        when a user logged a metric, if the user supplied one. The iteration
        column is added if any metric was logged with an iteration.
        Then, every metric name that was ever logged is a column in the
        metric_schema_union.
        """
        metadata_folder = os.path.join(self.log_dir, constants.METADATA_FOLDER)
        dfs = []
        # TODO: various file-creation corner cases like the result file not
        # always existing if stuff is not logged and etc should be ironed out
        # (would probably be easier if we had a centralized Sync class which
        # relied on some formal remote store semantics).
        for trial_id in trial_ids:
            # TODO constants should just contain the recipes for filename
            # construction instead of this multi-file implicit constraint
            result_file = os.path.join(
                metadata_folder, trial_id + "_" + constants.RESULT_SUFFIX)
            assert os.path.isfile(result_file), result_file
            dfs.append(pd.read_json(result_file, typ='frame', lines=True))
        df = pd.concat(dfs, axis=0, ignore_index=True, sort=False)
        return df


    def fetch_artifact(self, trial_id, prefix):
        """
        Verifies that all children of the artifact prefix path are
        available locally. Fetches them if not.

        Returns the local path to the given trial's artifacts at the
        specified prefix, which is always just

        {log_dir}/{trial_id}/{prefix}
        """
        # TODO: general windows concern: local prefix will be in
        # backslashes but remote dirs will be expecting /
        # TODO: having s3 logic split between project and sync.py
        # worries me
        local = os.path.join(self.log_dir, trial_id, prefix)
        if self.upload_dir:
            remote = '/'.join([self.upload_dir, trial_id, prefix])
            _remote_to_local_sync(remote, local)
        return local

    def _sync_metadata(self):
        local = os.path.join(self.log_dir, constants.METADATA_FOLDER)
        if self.upload_dir:
            remote = '/'.join([self.upload_dir, constants.METADATA_FOLDER])
            _remote_to_local_sync(remote, local)

    def _load_metadata(self):
        metadata_folder = os.path.join(self.log_dir, constants.METADATA_FOLDER)
        rows = []
        for trial_file in os.listdir(metadata_folder):
            if not trial_file.endswith(constants.CONFIG_SUFFIX):
                continue
            trial_file = os.path.join(metadata_folder, trial_file)
            rows.append(pd.read_json(trial_file, typ='frame', lines=True))
        return pd.concat(rows, axis=0, ignore_index=True, sort=False)

def _remote_to_local_sync(remote, local):
    # TODO: at some point look up whether sync will clobber newer
    # local files and do this more delicately
    if remote.startswith(S3_PREFIX):
        remote_to_local_sync_cmd = ("aws s3 sync {} {}".format(
            quote(remote), quote(local)))
    elif remote.startswith(GCS_PREFIX):
        remote_to_local_sync_cmd = ("gsutil rsync -r {} {}".format(
            quote(remote), quote(local)))
    else:
        raise ValueError('unhandled remote uri {}'.format(remote))
    print("Running log sync: {}".format(remote_to_local_sync_cmd))
    subprocess.check_call(remote_to_local_sync_cmd, shell=True)
