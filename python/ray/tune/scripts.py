from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import glob
import json
import os
import subprocess
from datetime import datetime

import pandas as pd
from ray.tune.trial import Trial
from tabulate import tabulate


def _flatten_dict(dt):
    while any(type(v) is dict for v in dt.values()):
        remove = []
        add = {}
        for key, value in dt.items():
            if type(value) is dict:
                for subkey, v in value.items():
                    add[":".join([key, subkey])] = v
                remove.append(key)
        dt.update(add)
        for k in remove:
            del dt[k]
    return dt


@click.group()
def cli():
    pass


DEFAULT_EXPERIMENT_INFO_KEYS = ("trial_name", "trial_id", "status",
                                "num_failures", "logdir")

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


def _list_trials(experiment_path, sort,
                 info_keys=DEFAULT_EXPERIMENT_INFO_KEYS):
    experiment_path = os.path.expanduser(experiment_path)
    globs = glob.glob(os.path.join(experiment_path, "experiment_state*.json"))
    filename = max(list(globs))

    with open(filename) as f:
        experiment_state = json.load(f)

    checkpoints_df = pd.DataFrame(
        experiment_state["checkpoints"])[list(info_keys)]
    if "logdir" in checkpoints_df.columns:
        # logdir often too verbose to view in table, so drop experiment_path
        checkpoints_df["logdir"] = checkpoints_df["logdir"].str.replace(
            experiment_path, '')
    if sort:
        checkpoints_df = checkpoints_df.sort_values(by=sort)

    print_df = pd.DataFrame()
    columns = list(info_keys)
    dropped = []
    empty = []
    # column display priority is based on the info_keys passed in
    for i in range(len(columns)):
        print_df[columns[i]] = checkpoints_df[columns[i]]
        table = tabulate(print_df, headers="keys", tablefmt="psql")
        if checkpoints_df[columns[i]].isnull().all():
            empty.append(columns[i])
            print_df.drop(columns[i], axis=1, inplace=True)
        elif str(table).index('\n') > TERMINAL_WIDTH:
            print_df.drop(columns[i], axis=1, inplace=True)
            table = tabulate(print_df, headers="keys", tablefmt="psql")
            dropped += columns[i:]
            break

    print(table)
    if dropped:
        print("Dropped columns:", dropped)
        print("Please increase your terminal size to view remaining columns.")
    if empty:
        print("Empty columns:", empty)


@cli.command()
@click.argument("experiment_path", required=True, type=str)
@click.option(
    '--sort', default=None, type=str, help='Select which column to sort on.')
def list_trials(experiment_path, sort):
    _list_trials(experiment_path, sort)


def _list_experiments(project_path, sort, info_keys=DEFAULT_PROJECT_INFO_KEYS):
    base, experiment_paths, _ = list(os.walk(project_path))[0]  # clean this

    experiment_data_collection = []
    for experiment_path in experiment_paths:
        experiment_state_path = glob.glob(
            os.path.join(base, experiment_path, "experiment_state*.json"))

        if not experiment_state_path:
            # TODO(hartikainen): Print some warning?
            continue

        with open(experiment_state_path[0]) as f:
            experiment_state = json.load(f)

        checkpoints = pd.DataFrame(experiment_state["checkpoints"])
        runner_data = experiment_state["runner_data"]
        timestamp = experiment_state.get("timestamp")

        experiment_data = {
            "name": experiment_path,
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

    print_df = pd.DataFrame()
    columns = list(info_keys)
    dropped = []
    empty = []
    # column display priority is based on the info_keys passed in
    for i in range(len(columns)):
        print_df[columns[i]] = info_df[columns[i]]
        table = tabulate(print_df, headers="keys", tablefmt="psql")
        if info_df[columns[i]].isnull().all():
            empty.append(columns[i])
            print_df.drop(columns[i], axis=1, inplace=True)
        elif str(table).index('\n') > TERMINAL_WIDTH:
            print_df.drop(columns[i], axis=1, inplace=True)
            table = tabulate(print_df, headers="keys", tablefmt="psql")
            dropped += columns[i:]
            break

    print(table)
    if dropped:
        print("Dropped columns:", dropped)
        print("Please increase your terminal size to view remaining columns.")
    if empty:
        print("Empty columns:", empty)


@cli.command()
@click.argument("project_path", required=True, type=str)
@click.option(
    '--sort', default=None, type=str, help='Select which column to sort on.')
def list_experiments(project_path, sort):
    _list_experiments(project_path, sort)


cli.add_command(list_trials, name="ls")
cli.add_command(list_experiments, name="lsx")


def main():
    return cli()


if __name__ == "__main__":
    main()
