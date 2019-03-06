from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import glob
import json
import logging
import os
import sys
import subprocess
from datetime import datetime

import pandas as pd
from ray.tune.util import flatten_dict
from ray.tune.result import TRAINING_ITERATION, MEAN_ACCURACY, MEAN_LOSS
from ray.tune.trial import Trial
try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

logger = logging.getLogger(__name__)

DEFAULT_EXPERIMENT_INFO_KEYS = (
    "trainable_name",
    "experiment_tag",
    "trial_id",
    "status",
    "last_update_time",
)

DEFAULT_RESULT_KEYS = (TRAINING_ITERATION, MEAN_ACCURACY, MEAN_LOSS)

DEFAULT_PROJECT_INFO_KEYS = (
    "name",
    "timestamp",
    "total_trials",
    "running_trials",
    "terminated_trials",
    "error_trials",
)

TERMINAL_HEIGHT, TERMINAL_WIDTH = subprocess.check_output(['stty',
                                                           'size']).split()
TERMINAL_HEIGHT, TERMINAL_WIDTH = int(TERMINAL_HEIGHT), int(TERMINAL_WIDTH)


def _check_tabulate():
    if tabulate is None:
        raise Exception(
            "Tabulate not installed. Please run `pip install tabulate`.")


def print_format_output(dataframe):
    print_df = pd.DataFrame()
    dropped_cols = []
    empty_cols = []
    # column display priority is based on the info_keys passed in
    for i, col in enumerate(dataframe):
        if dataframe[col].isnull().all():
            # Don't add col to print_df if is fully empty
            empty_cols += [col]
            continue

        print_df[col] = dataframe[col]
        test_table = tabulate(print_df, headers="keys", tablefmt="psql")
        if str(test_table).index('\n') > TERMINAL_WIDTH:
            # Drop all columns beyond terminal width
            print_df.drop(col, axis=1, inplace=True)
            dropped_cols += list(dataframe.columns)[i:]
            break

    table = tabulate(
        print_df, headers="keys", tablefmt="psql", showindex="never")

    print(table)
    if dropped_cols:
        print("Dropped columns:", dropped_cols)
        print("Please increase your terminal size to view remaining columns.")
    if empty_cols:
        print("Empty columns:", empty_cols)

    return table, dropped_cols, empty_cols


def get_experiment_state(experiment_path, exit_on_fail=False):
    experiment_path = os.path.expanduser(experiment_path)
    experiment_state_paths = glob.glob(
        os.path.join(experiment_path, "experiment_state*.json"))
    if not experiment_state_paths:
        if exit_on_fail:
            print("No experiment state found!")
            sys.exit(0)
        else:
            return
    experiment_filename = max(list(experiment_state_paths))

    with open(experiment_filename) as f:
        experiment_state = json.load(f)
    return experiment_state


def list_trials(experiment_path,
                sort=None,
                info_keys=DEFAULT_EXPERIMENT_INFO_KEYS,
                result_keys=DEFAULT_RESULT_KEYS):
    """Lists trials in the directory subtree starting at the given path."""
    _check_tabulate()
    experiment_state = get_experiment_state(experiment_path, exit_on_fail=True)

    checkpoint_dicts = experiment_state["checkpoints"]
    checkpoint_dicts = [flatten_dict(g) for g in checkpoint_dicts]
    checkpoints_df = pd.DataFrame(checkpoint_dicts)

    result_keys = ["last_result:{}".format(k) for k in result_keys]
    col_keys = [
        k for k in list(info_keys) + result_keys if k in checkpoints_df
    ]
    checkpoints_df = checkpoints_df[col_keys]

    if "logdir" in checkpoints_df:
        # logdir often too verbose to view in table, so drop experiment_path
        checkpoints_df["logdir"] = checkpoints_df["logdir"].str.replace(
            experiment_path, '')

    if sort:
        if sort not in checkpoints_df:
            raise KeyError("Sort Index {} not in: {}".format(
                sort, list(checkpoints_df)))
        checkpoints_df = checkpoints_df.sort_values(by=sort)

    print_format_output(checkpoints_df)


def list_experiments(project_path,
                     sort=None,
                     info_keys=DEFAULT_PROJECT_INFO_KEYS):
    _check_tabulate()
    base, experiment_folders, _ = next(os.walk(project_path))

    experiment_data_collection = []
    for experiment_dir in experiment_dirs:
        experiment_state = get_experiment_state(
            os.path.join(base, experiment_dir))
        if not experiment_state:
            logger.debug("No experiment state found in %s", experiment_dir)
            continue

        checkpoints = pd.DataFrame(experiment_state["checkpoints"])
        runner_data = experiment_state["runner_data"]
        timestamp = experiment_state.get("timestamp")

        experiment_data = {
            "name": experiment_dir,
            "start_time": runner_data.get("_start_time"),
            "timestamp": datetime.fromtimestamp(timestamp)
            if timestamp else None,
            "total_trials": checkpoints.shape[0],
            "running_trials": (checkpoints["status"] == Trial.RUNNING).sum(),
            "terminated_trials": (
                checkpoints["status"] == Trial.TERMINATED).sum(),
            "error_trials": (checkpoints["status"] == Trial.ERROR).sum(),
        }
        experiment_data_collection.append(experiment_data)

    info_df = pd.DataFrame(experiment_data_collection)[list(info_keys)]

    if sort:
        info_df = info_df.sort_values(by=sort)

    print_format_output(info_df)
