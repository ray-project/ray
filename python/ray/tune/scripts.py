from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import logging
import glob
import json
import os
import pandas as pd
# from ray.tune.trial_runner import TrialRunner
import sys
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

@cli.command()
@click.argument("experiment_path", required=True, type=str)
def list_trials(experiment_path):
    _list_trials(experiment_path)

def _list_trials(experiment_path):
    experiment_path = os.path.expanduser(experiment_path)
    print("start glob")
    globs = glob.glob(os.path.join(experiment_path, "experiment_state*.json"))
    print(globs)
    filename = max(list(globs))
    print("found")
    with open(filename) as f:
        experiment_state = json.load(f)

    checkpoints_df = pd.DataFrame(experiment_state["checkpoints"])[["trial_name", "trial_id", "status", "num_failures", "logdir"]]
    print(tabulate(checkpoints_df, headers="keys", tablefmt="psql"))

@cli.command()
@click.argument("project_path", required=True, type=str)
def list_experiments(project_path):
    _list_experiments(project_path)

def _list_experiments(project_path):
    base, experiment_paths, _ = list(os.walk(project_path))[0]  # clean this
    experiment_collection = {}
    for experiment_path in experiment_paths:
        experiment_state_path = glob.glob(os.path.join(base, experiment_path,  "experiment_state*.json"))
        if not experiment_state_path:
            continue
        else:
            with open(experiment_state_path[0]) as f:
                experiment_state = json.load(f)
                # import ipdb; ipdb.set_trace()
            experiment_collection[experiment_state_path[0]] = (pd.DataFrame(experiment_state["checkpoints"]), experiment_state["runner_data"], experiment_state["time_stamp"])

    # total_ = pd.concat(experiment_collection.values())

    from ray.tune.trial import Trial
    all_values = []
    for experiment_path, (df, data, timestamp) in experiment_collection.items():
        status = {}
        status["name"] = experiment_path
        status["timestamp"] = timestamp
        status["total_running"] = (df["status"] == Trial.RUNNING).sum()
        status["total_terminated"] = (df["status"] == Trial.TERMINATED).sum()
        status["total_errored"] = (df["status"] == Trial.ERROR).sum()
        status["total_trials"] = df.shape[0]
        all_values += [status]

    final_dataframe = pd.DataFrame(all_values)
    print(final_dataframe.to_string())



cli.add_command(list_trials, name="ls")
cli.add_command(list_experiments, name="lsx")


def main():
    return cli()


if __name__ == "__main__":
    main()
