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

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S (%A)"

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
    "total_trials",
    "running_trials",
    "terminated_trials",
    "error_trials",
    "last_updated",
)

try:
    TERM_HEIGHT, TERM_WIDTH = subprocess.check_output(["stty", "size"]).split()
    TERM_HEIGHT, TERM_WIDTH = int(TERM_HEIGHT), int(TERM_WIDTH)
except subprocess.CalledProcessError:
    TERM_HEIGHT, TERM_WIDTH = 100, 100

EDITOR = os.getenv("EDITOR", "vim")


def _check_tabulate():
    """Checks whether tabulate is installed."""
    if tabulate is None:
        raise ImportError(
            "Tabulate not installed. Please run `pip install tabulate`.")


def print_format_output(dataframe):
    """Prints output of given dataframe to fit into terminal.

    Returns:
        table (pd.DataFrame): Final outputted dataframe.
        dropped_cols (list): Columns dropped due to terminal size.
        empty_cols (list): Empty columns (dropped on default).
    """
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
        if str(test_table).index('\n') > TERM_WIDTH:
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


def _get_experiment_state(experiment_path, exit_on_fail=False):
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
                output=None,
                info_keys=DEFAULT_EXPERIMENT_INFO_KEYS,
                result_keys=DEFAULT_RESULT_KEYS):
    """Lists trials in the directory subtree starting at the given path.

    Args:
        experiment_path (str): Directory where trials are located.
            Corresponds to Experiment.local_dir/Experiment.name.
        sort (str): Key to sort by.
        output (str): Name of file where output is saved.
        info_keys (list): Keys that are displayed.
        result_keys (list): Keys of last result that are displayed.
    """
    _check_tabulate()
    experiment_state = _get_experiment_state(
        experiment_path, exit_on_fail=True)

    checkpoint_dicts = experiment_state["checkpoints"]
    checkpoint_dicts = [flatten_dict(g) for g in checkpoint_dicts]
    checkpoints_df = pd.DataFrame(checkpoint_dicts)

    result_keys = ["last_result:{}".format(k) for k in result_keys]
    col_keys = [
        k for k in list(info_keys) + result_keys if k in checkpoints_df
    ]
    checkpoints_df = checkpoints_df[col_keys]

    if "last_update_time" in checkpoints_df:
        with pd.option_context("mode.use_inf_as_null", True):
            datetime_series = checkpoints_df["last_update_time"].dropna()

        datetime_series = datetime_series.apply(
            lambda t: datetime.fromtimestamp(t).strftime(TIMESTAMP_FORMAT))
        checkpoints_df["last_update_time"] = datetime_series

    if "logdir" in checkpoints_df:
        # logdir often too verbose to view in table, so drop experiment_path
        checkpoints_df["logdir"] = checkpoints_df["logdir"].str.replace(
            experiment_path, '')

    if sort:
        if sort not in checkpoints_df:
            raise KeyError("Sort Index '{}' not in: {}".format(
                sort, list(checkpoints_df)))
        checkpoints_df = checkpoints_df.sort_values(by=sort)

    print_format_output(checkpoints_df)

    if output:
        experiment_path = os.path.expanduser(experiment_path)
        output_path = os.path.join(experiment_path, output)
        file_extension = os.path.splitext(output)[1].lower()
        if file_extension in (".p", ".pkl", ".pickle"):
            checkpoints_df.to_pickle(output_path)
        elif file_extension == ".csv":
            checkpoints_df.to_csv(output_path, index=False)
        else:
            raise ValueError("Unsupported filetype: {}".format(output))
        print("Output saved at:", output_path)


def list_experiments(project_path,
                     sort=None,
                     output=None,
                     info_keys=DEFAULT_PROJECT_INFO_KEYS):
    """Lists experiments in the directory subtree.

    Args:
        project_path (str): Directory where experiments are located.
            Corresponds to Experiment.local_dir.
        sort (str): Key to sort by.
        output (str): Name of file where output is saved.
        info_keys (list): Keys that are displayed.
    """
    _check_tabulate()
    base, experiment_folders, _ = next(os.walk(project_path))

    experiment_data_collection = []

    for experiment_dir in experiment_folders:
        experiment_state = _get_experiment_state(
            os.path.join(base, experiment_dir))
        if not experiment_state:
            logger.debug("No experiment state found in %s", experiment_dir)
            continue

        checkpoints = pd.DataFrame(experiment_state["checkpoints"])
        runner_data = experiment_state["runner_data"]

        # Format time-based values.
        time_values = {
            "start_time": runner_data.get("_start_time"),
            "last_updated": experiment_state.get("timestamp"),
        }

        formatted_time_values = {
            key: datetime.fromtimestamp(val).strftime(TIMESTAMP_FORMAT)
            if val else None
            for key, val in time_values.items()
        }

        experiment_data = {
            "name": experiment_dir,
            "total_trials": checkpoints.shape[0],
            "running_trials": (checkpoints["status"] == Trial.RUNNING).sum(),
            "terminated_trials": (
                checkpoints["status"] == Trial.TERMINATED).sum(),
            "error_trials": (checkpoints["status"] == Trial.ERROR).sum(),
        }
        experiment_data.update(formatted_time_values)
        experiment_data_collection.append(experiment_data)

    if not experiment_data_collection:
        print("No experiments found!")
        sys.exit(0)

    info_df = pd.DataFrame(experiment_data_collection)
    col_keys = [k for k in list(info_keys) if k in info_df]

    if not col_keys:
        print("None of keys {} in experiment data!".format(info_keys))
        sys.exit(0)

    info_df = info_df[col_keys]

    if sort:
        if sort not in info_df:
            raise KeyError("Sort Index '{}' not in: {}".format(
                sort, list(info_df)))
        info_df = info_df.sort_values(by=sort)

    print_format_output(info_df)

    if output:
        output_path = os.path.join(base, output)
        file_extension = os.path.splitext(output)[1].lower()
        if file_extension in (".p", ".pkl", ".pickle"):
            info_df.to_pickle(output_path)
        elif file_extension == ".csv":
            info_df.to_csv(output_path, index=False)
        else:
            raise ValueError("Unsupported filetype: {}".format(output))
        print("Output saved at:", output_path)


def add_note(path, filename="note.txt"):
    """Opens a txt file at the given path where user can add and save notes.

    Args:
        path (str): Directory where note will be saved.
        filename (str): Name of note. Defaults to "note.txt"
    """
    path = os.path.expanduser(path)
    assert os.path.isdir(path), "{} is not a valid directory.".format(path)

    filepath = os.path.join(path, filename)
    exists = os.path.isfile(filepath)

    try:
        subprocess.call([EDITOR, filepath])
    except Exception as exc:
        logger.error("Editing note failed!")
        raise exc
    if exists:
        print("Note updated at:", filepath)
    else:
        print("Note created at:", filepath)
