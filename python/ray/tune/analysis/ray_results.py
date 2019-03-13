from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
from shutil import copyfile

import numpy as np
import pandas as pd


def get_result_path(trial_dir):
    return os.path.join(trial_dir, "progress.csv")


def get_result_backup_path(result_path):
    return f"{result_path}.backup"


def result_backed_up(trial_dir):
    result_path = get_result_path(trial_dir=trial_dir)
    result_backup_path = get_result_backup_path(result_path=result_path)

    return os.path.exists(result_backup_path)


def backup_result(trial_dir):
    result_path = get_result_path(trial_dir=trial_dir)
    result_backup_path = get_result_backup_path(result_path=result_path)
    copyfile(result_path, result_backup_path)


def prune_restore_dataframes(dataframes):
    if len(dataframes) < 2:
        return dataframes[0]

    dataframe1 = dataframes[0]
    dataframe2 = prune_restore_dataframes(dataframes[1:])

    return max(dataframe1, dataframe2, key=lambda d: d.shape[0])


def clean_dataframe(dataframe):
    restore_begins = tuple(
        np.where(dataframe['iterations_since_restore'] == 1)[0])
    restore_ends = (*restore_begins[1:], dataframe.shape[0])
    restore_dataframes_index = tuple(zip(restore_begins, restore_ends))
    restore_dataframes = tuple(
        dataframe[slice(*restore_dataframe_index)]
        for restore_dataframe_index in restore_dataframes_index)

    cleaned_dataframe = prune_restore_dataframes(restore_dataframes)

    return cleaned_dataframe


def clean_trial(trial_dir):
    result_path = get_result_path(trial_dir=trial_dir)
    result_backup_path = get_result_backup_path(result_path=result_path)

    print(result_backup_path)
    dataframe = pd.read_csv(result_backup_path)

    cleaned_dataframe = clean_dataframe(dataframe)

    cleaned_dataframe.to_csv(result_path, index=False)


def fix_ray_results(experiment_dir):
    trial_dirs = [
        os.path.join(experiment_dir, trial_dir)
        for trial_dir in next(os.walk(experiment_dir))[1]
    ]

    for trial_dir in trial_dirs:
        if not result_backed_up(trial_dir):
            backup_result(trial_dir)

        clean_trial(trial_dir)
