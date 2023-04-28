from typing import Optional, List

import click
import logging
import operator
import os
import shutil
import subprocess
from datetime import datetime

import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype
from ray.tune.result import (
    DEFAULT_EXPERIMENT_INFO_KEYS,
    DEFAULT_RESULT_KEYS,
    CONFIG_PREFIX,
)
from ray.tune.analysis import ExperimentAnalysis
from ray.tune import TuneError
from ray._private.thirdparty.tabulate.tabulate import tabulate

logger = logging.getLogger(__name__)

EDITOR = os.getenv("EDITOR", "vim")

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S (%A)"

DEFAULT_CLI_KEYS = DEFAULT_EXPERIMENT_INFO_KEYS + DEFAULT_RESULT_KEYS

DEFAULT_PROJECT_INFO_KEYS = (
    "name",
    "total_trials",
    "last_updated",
)

TERM_WIDTH, TERM_HEIGHT = shutil.get_terminal_size(fallback=(100, 100))

OPERATORS = {
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt,
}


def _check_tabulate():
    """Checks whether tabulate is installed."""
    if tabulate is None:
        raise ImportError("Tabulate not installed. Please run `pip install tabulate`.")


def print_format_output(dataframe):
    """Prints output of given dataframe to fit into terminal.

    Returns:
        table: Final outputted dataframe.
        dropped_cols: Columns dropped due to terminal size.
        empty_cols: Empty columns (dropped on default).
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
        if str(test_table).index("\n") > TERM_WIDTH:
            # Drop all columns beyond terminal width
            print_df.drop(col, axis=1, inplace=True)
            dropped_cols += list(dataframe.columns)[i:]
            break

    table = tabulate(print_df, headers="keys", tablefmt="psql", showindex="never")

    print(table)
    if dropped_cols:
        click.secho("Dropped columns: {}".format(dropped_cols), fg="yellow")
        click.secho("Please increase your terminal size to view remaining columns.")
    if empty_cols:
        click.secho("Empty columns: {}".format(empty_cols), fg="yellow")

    return table, dropped_cols, empty_cols


def list_trials(
    experiment_path: str,
    sort: Optional[List[str]] = None,
    output: Optional[str] = None,
    filter_op: Optional[str] = None,
    info_keys: Optional[List[str]] = None,
    limit: int = None,
    desc: bool = False,
):
    """Lists trials in the directory subtree starting at the given path.

    Args:
        experiment_path: Directory where trials are located.
            Like Experiment.local_dir/Experiment.name/experiment*.json.
        sort: Keys to sort by.
        output: Name of file where output is saved.
        filter_op: Filter operation in the format
            "<column> <operator> <value>".
        info_keys: Keys that are displayed.
        limit: Number of rows to display.
        desc: Sort ascending vs. descending.
    """
    _check_tabulate()

    try:
        checkpoints_df = ExperimentAnalysis(experiment_path).dataframe()  # last result
    except TuneError as e:
        raise click.ClickException("No trial data found!") from e

    config_prefix = CONFIG_PREFIX + "/"

    def key_filter(k):
        return k in DEFAULT_CLI_KEYS or k.startswith(config_prefix)

    col_keys = [k for k in checkpoints_df.columns if key_filter(k)]

    if info_keys:
        for k in info_keys:
            if k not in checkpoints_df.columns:
                raise click.ClickException(
                    "Provided key invalid: {}. "
                    "Available keys: {}.".format(k, checkpoints_df.columns)
                )
        col_keys = [k for k in checkpoints_df.columns if k in info_keys]

    if not col_keys:
        raise click.ClickException("No columns to output.")

    checkpoints_df = checkpoints_df[col_keys]
    if "last_update_time" in checkpoints_df:
        with pd.option_context("mode.use_inf_as_null", True):
            datetime_series = checkpoints_df["last_update_time"].dropna()

        datetime_series = datetime_series.apply(
            lambda t: datetime.fromtimestamp(t).strftime(TIMESTAMP_FORMAT)
        )
        checkpoints_df["last_update_time"] = datetime_series

    if "logdir" in checkpoints_df:
        # logdir often too long to view in table, so drop experiment_path
        checkpoints_df["logdir"] = checkpoints_df["logdir"].str.replace(
            experiment_path, ""
        )

    if filter_op:
        col, op, val = filter_op.split(" ")
        col_type = checkpoints_df[col].dtype
        if is_numeric_dtype(col_type):
            val = float(val)
        elif is_string_dtype(col_type):
            val = str(val)
        # TODO(Andrew): add support for datetime and boolean
        else:
            raise click.ClickException(
                "Unsupported dtype for {}: {}".format(val, col_type)
            )
        op = OPERATORS[op]
        filtered_index = op(checkpoints_df[col], val)
        checkpoints_df = checkpoints_df[filtered_index]

    if sort:
        for key in sort:
            if key not in checkpoints_df:
                raise click.ClickException(
                    "{} not in: {}".format(key, list(checkpoints_df))
                )
        ascending = not desc
        checkpoints_df = checkpoints_df.sort_values(by=sort, ascending=ascending)

    if limit:
        checkpoints_df = checkpoints_df[:limit]

    print_format_output(checkpoints_df)

    if output:
        file_extension = os.path.splitext(output)[1].lower()
        if file_extension in (".p", ".pkl", ".pickle"):
            checkpoints_df.to_pickle(output)
        elif file_extension == ".csv":
            checkpoints_df.to_csv(output, index=False)
        else:
            raise click.ClickException("Unsupported filetype: {}".format(output))
        click.secho("Output saved at {}".format(output), fg="green")


def list_experiments(
    project_path: str,
    sort: Optional[List[str]] = None,
    output: str = None,
    filter_op: str = None,
    info_keys: Optional[List[str]] = None,
    limit: int = None,
    desc: bool = False,
):
    """Lists experiments in the directory subtree.

    Args:
        project_path: Directory where experiments are located.
            Corresponds to Experiment.local_dir.
        sort: Keys to sort by.
        output: Name of file where output is saved.
        filter_op: Filter operation in the format
            "<column> <operator> <value>".
        info_keys: Keys that are displayed.
        limit: Number of rows to display.
        desc: Sort ascending vs. descending.
    """
    _check_tabulate()
    base, experiment_folders, _ = next(os.walk(project_path))

    experiment_data_collection = []

    for experiment_dir in experiment_folders:
        num_trials = sum(
            "result.json" in files
            for _, _, files in os.walk(os.path.join(base, experiment_dir))
        )

        experiment_data = {"name": experiment_dir, "total_trials": num_trials}
        experiment_data_collection.append(experiment_data)

    if not experiment_data_collection:
        raise click.ClickException("No experiments found!")

    info_df = pd.DataFrame(experiment_data_collection)
    if not info_keys:
        info_keys = DEFAULT_PROJECT_INFO_KEYS
    col_keys = [k for k in list(info_keys) if k in info_df]
    if not col_keys:
        raise click.ClickException(
            "None of keys {} in experiment data!".format(info_keys)
        )
    info_df = info_df[col_keys]

    if filter_op:
        col, op, val = filter_op.split(" ")
        col_type = info_df[col].dtype
        if is_numeric_dtype(col_type):
            val = float(val)
        elif is_string_dtype(col_type):
            val = str(val)
        # TODO(Andrew): add support for datetime and boolean
        else:
            raise click.ClickException(
                "Unsupported dtype for {}: {}".format(val, col_type)
            )
        op = OPERATORS[op]
        filtered_index = op(info_df[col], val)
        info_df = info_df[filtered_index]

    if sort:
        for key in sort:
            if key not in info_df:
                raise click.ClickException("{} not in: {}".format(key, list(info_df)))
        ascending = not desc
        info_df = info_df.sort_values(by=sort, ascending=ascending)

    if limit:
        info_df = info_df[:limit]

    print_format_output(info_df)

    if output:
        file_extension = os.path.splitext(output)[1].lower()
        if file_extension in (".p", ".pkl", ".pickle"):
            info_df.to_pickle(output)
        elif file_extension == ".csv":
            info_df.to_csv(output, index=False)
        else:
            raise click.ClickException("Unsupported filetype: {}".format(output))
        click.secho("Output saved at {}".format(output), fg="green")


def add_note(path: str, filename: str = "note.txt"):
    """Opens a txt file at the given path where user can add and save notes.

    Args:
        path: Directory where note will be saved.
        filename: Name of note. Defaults to "note.txt"
    """
    path = os.path.expanduser(path)
    assert os.path.isdir(path), "{} is not a valid directory.".format(path)

    filepath = os.path.join(path, filename)
    exists = os.path.isfile(filepath)

    try:
        subprocess.call([EDITOR, filepath])
    except Exception as exc:
        click.secho("Editing note failed: {}".format(str(exc)), fg="red")
    if exists:
        print("Note updated at:", filepath)
    else:
        print("Note created at:", filepath)
