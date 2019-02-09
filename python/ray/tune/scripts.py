from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import glob
import json
import os
from datetime import datetime


import pandas as pd

from ray.tune.trial import Trial


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


DEFAULT_EXPERIMENT_INFO_KEYS = (
    "trial_name",
    "trial_id",
    "status",
    "num_failures",
    "logdir"
)

DEFAULT_PROJECT_INFO_KEYS = (
    "name",
    "timestamp",
    "total_trials",
    "running_trials",
    "terminated_trials",
    "error_trials",
)


def _list_trials(experiment_path, info_keys=DEFAULT_EXPERIMENT_INFO_KEYS):
    experiment_path = os.path.expanduser(experiment_path)
    globs = glob.glob(os.path.join(experiment_path, "experiment_state*.json"))
    filename = max(list(globs))

    with open(filename) as f:
        experiment_state = json.load(f)

    checkpoints = pd.DataFrame.from_records(experiment_state['checkpoints'])
    # TODO(hartikainen): The logdir is often too verbose to be viewed in a
    # table.
    checkpoints['logdir'] = checkpoints['logdir'].str.replace(
        experiment_path, '')

    print(checkpoints[list(info_keys)].to_string())


@cli.command()
@click.argument("experiment_path", required=True, type=str)
def list_trials(experiment_path):
    _list_trials(experiment_path)


def _list_experiments(project_path, info_keys=DEFAULT_PROJECT_INFO_KEYS):
    base, experiment_paths, _ = list(os.walk(project_path))[0]  # clean this

    experiment_data_collection = []
    for experiment_path in experiment_paths:
        experiment_state_path = glob.glob(os.path.join(
            base,
            experiment_path,
            "experiment_state*.json"))

        if not experiment_state_path:
            # TODO(hartikainen): Print some warning?
            continue

        with open(experiment_state_path[0]) as f:
            experiment_state = json.load(f)

        checkpoints = pd.DataFrame(experiment_state["checkpoints"])
        runner_data = experiment_state["runner_data"]
        timestamp = experiment_state["timestamp"]

        experiment_data = {
            "name": experiment_path,
            "start_time": runner_data["_start_time"],
            "timestamp": datetime.fromtimestamp(timestamp),
            "total_trials": checkpoints.shape[0],
            "running_trials": (checkpoints["status"] == Trial.RUNNING).sum(),
            "terminated_trials": (
                checkpoints["status"] == Trial.TERMINATED).sum(),
            "error_trials": (checkpoints["status"] == Trial.ERROR).sum(),
         }

        experiment_data_collection.append(experiment_data)

    info_dataframe = pd.DataFrame(experiment_data_collection)
    print(info_dataframe[list(info_keys)].to_string())


@cli.command()
@click.argument("project_path", required=True, type=str)
def list_experiments(project_path):
    _list_experiments(project_path)


cli.add_command(list_trials, name="ls")
cli.add_command(list_experiments, name="lsx")


def main():
    return cli()


if __name__ == "__main__":
    main()
