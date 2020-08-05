"""Utilities to load and cache data."""

import os
from typing import Callable, Dict
import numpy as np
from transformers import EvalPrediction
from transformers import glue_compute_metrics, glue_output_modes


def build_compute_metrics_fn(
        task_name: str) -> Callable[[EvalPrediction], Dict]:
    """Function from transformers/examples/text-classification/run_glue.py"""
    output_mode = glue_output_modes[task_name]

    def compute_metrics_fn(p: EvalPrediction):
        if output_mode == "classification":
            preds = np.argmax(p.predictions, axis=1)
        elif output_mode == "regression":
            preds = np.squeeze(p.predictions)
        metrics = glue_compute_metrics(task_name, preds, p.label_ids)
        return metrics

    return compute_metrics_fn


def download_data(task_name, data_dir="./data"):
    # Download RTE training data
    print("Downloading dataset.")
    import urllib
    import zipfile
    if task_name == "rte":
        url = "https://firebasestorage.googleapis.com/v0/b/" \
            "mtl-sentence-representations.appspot.com" \
            "/o/data%2FRTE.zip?alt=media" \
            "&token=5efa7e85-a0bb-4f19-8ea2-9e1840f077fb"
    else:
        raise ValueError("Unknown task: {}".format(task_name))
    data_file = os.path.join(data_dir, "{}.zip".format(task_name))
    if not os.path.exists(data_file):
        urllib.request.urlretrieve(url, data_file)
        with zipfile.ZipFile(data_file) as zip_ref:
            zip_ref.extractall(data_dir)
        print("Downloaded data for task {} to {}".format(task_name, data_dir))
    else:
        print("Data already exists. Using downloaded data for task {} from {}".
              format(task_name, data_dir))
