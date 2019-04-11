from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import numpy as np
import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype
from shutil import copyfile

from ray.tune.util import flatten_dict
from ray.tune.result import EXPR_PROGRESS_FILE, \
    EXPR_PARARM_FILE, EXPR_RESULT_FILE

logger = logging.getLogger(__name__)


def get_sorted_trials(trial_list, metric):
    return sorted(
        trial_list,
        key=lambda trial: trial.last_result.get(metric, 0),
        reverse=True)


def get_best_result(trial_list, metric):
    """Retrieve the last result from the best trial."""
    return {metric: get_best_trial(trial_list, metric).last_result[metric]}


def _parse_results(res_path):
    res_dict = {}
    try:
        with open(res_path) as f:
            # Get last line in file
            for line in f:
                pass
        res_dict = flatten_dict(json.loads(line.strip()))
    except Exception:
        logger.exception(
            "Importing {} failed...Perhaps empty?".format(res_path))
    return res_dict


def _parse_configs(cfg_path):
    with open(cfg_path) as f:
        cfg_dict = flatten_dict(json.load(f))
    return cfg_dict


def _resolve(directory, result_filename):
    try:
        result_path = os.path.join(directory, result_filename)
        res_dict = _parse_results(result_path)
        cfgp = os.path.join(directory, EXPR_PARARM_FILE)
        cfg_dict = _parse_configs(cfgp)
        cfg_dict.update(res_dict)
        return cfg_dict
    except Exception:
        return None


def load_results_to_df(directory, result_name=EXPR_RESULT_FILE):
    """ Loads results to pandas dataframe """
    exp_directories = [
        dirpath for dirpath, dirs, files in os.walk(directory) for f in files
        if f == result_name
    ]
    data = [_resolve(d, result_name) for d in exp_directories]
    data = [d for d in data if d is not None]
    return pd.DataFrame(data)


def generate_plotly_dim_dict(df, field):
    dim_dict = {}
    dim_dict["label"] = field
    column = df[field]
    if is_numeric_dtype(column):
        dim_dict["values"] = column
    elif is_string_dtype(column):
        texts = column.unique()
        dim_dict["values"] = [
            np.argwhere(texts == x).flatten()[0] for x in column
        ]
        dim_dict["tickvals"] = list(range(len(texts)))
        dim_dict["ticktext"] = texts
    else:
        raise Exception("Unidentifiable Type")

    return dim_dict


def get_result_path(trial_dir):
    return os.path.join(trial_dir, EXPR_PROGRESS_FILE)


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

    try:
        dataframe = pd.read_csv(result_backup_path)
    except pd.errors.EmptyDataError:  # trial has an empty CSV file
        return

    cleaned_dataframe = clean_dataframe(dataframe)

    cleaned_dataframe.to_csv(result_path, index=False)


def fix_ray_results(experiment_dir):
    if not os.path.exists(experiment_dir):
        raise ValueError(
            "could not find experiment directory {}".format(experiment_dir))

    trial_dirs = [
        os.path.join(experiment_dir, trial_dir)
        for trial_dir in next(os.walk(experiment_dir))[1]
    ]

    for trial_dir in trial_dirs:
        if not result_backed_up(trial_dir):
            backup_result(trial_dir)

        clean_trial(trial_dir)
