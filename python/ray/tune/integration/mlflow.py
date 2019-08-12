#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import json
import mlflow
from  mlflow.tracking import MlflowClient

from ray.tune.logger import Logger
from ray.tune.result import TRAINING_ITERATION

class MLFLowLogger(Logger):
    """MLFlow integration logger."""
    def _init(self):
        client = MlflowClient()
        run = client.create_run(self.config["mlflow_experiment_id"])
        self._run_id = run.info.run_id
        for key, value in self.config.items():
            client.log_param(self._run_id, key, value)
        self.client = client

    def on_result(self, result):
        for key, value in result.items():
            if not isinstance(value, float):
                continue
            self.client.log_metric(
                self._run_id, key, value, step=result.get(TRAINING_ITERATION))

    def close(self):
        self.client.set_terminated(self._run_id)

