# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Estimator: High level tools for working with models."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# pylint: disable=unused-import,line-too-long,wildcard-import
from tensorflow_estimator.python.estimator.canned.baseline import BaselineClassifier
from tensorflow_estimator.python.estimator.canned.baseline import BaselineEstimator
from tensorflow_estimator.python.estimator.canned.baseline import BaselineRegressor
from tensorflow_estimator.python.estimator.canned.boosted_trees import BoostedTreesClassifier
from tensorflow_estimator.python.estimator.canned.boosted_trees import BoostedTreesRegressor
from tensorflow_estimator.python.estimator.canned.dnn import dnn_logit_fn_builder
from tensorflow_estimator.python.estimator.canned.dnn import DNNClassifier
from tensorflow_estimator.python.estimator.canned.dnn import DNNEstimator
from tensorflow_estimator.python.estimator.canned.dnn import DNNRegressor
from tensorflow_estimator.python.estimator.canned.dnn_linear_combined import DNNLinearCombinedClassifier
from tensorflow_estimator.python.estimator.canned.dnn_linear_combined import DNNLinearCombinedEstimator
from tensorflow_estimator.python.estimator.canned.dnn_linear_combined import DNNLinearCombinedRegressor
from tensorflow_estimator.python.estimator.canned.linear import linear_logit_fn_builder
from tensorflow_estimator.python.estimator.canned.linear import LinearClassifier
from tensorflow_estimator.python.estimator.canned.linear import LinearEstimator
from tensorflow_estimator.python.estimator.canned.linear import LinearRegressor
from tensorflow_estimator.python.estimator.canned.parsing_utils import classifier_parse_example_spec
from tensorflow_estimator.python.estimator.canned.parsing_utils import regressor_parse_example_spec
from tensorflow_estimator.python.estimator.early_stopping import make_early_stopping_hook
from tensorflow_estimator.python.estimator.early_stopping import stop_if_higher_hook
from tensorflow_estimator.python.estimator.estimator import Estimator
from tensorflow_estimator.python.estimator.estimator import VocabInfo
from tensorflow_estimator.python.estimator.estimator import WarmStartSettings
from tensorflow_estimator.python.estimator.export import export_lib as export
from tensorflow_estimator.python.estimator.exporter import Exporter
from tensorflow_estimator.python.estimator.exporter import FinalExporter
from tensorflow_estimator.python.estimator.exporter import LatestExporter
from tensorflow_estimator.python.estimator.extenders import add_metrics
from tensorflow_estimator.python.estimator.hooks import basic_session_run_hooks
from tensorflow_estimator.python.estimator.hooks import hooks
from tensorflow_estimator.python.estimator.hooks import session_run_hook
from tensorflow_estimator.python.estimator.inputs import inputs
from tensorflow_estimator.python.estimator.keras import model_to_estimator
from tensorflow_estimator.python.estimator.model_fn import call_logit_fn
from tensorflow_estimator.python.estimator.model_fn import EstimatorSpec
from tensorflow_estimator.python.estimator.model_fn import ModeKeys
from tensorflow_estimator.python.estimator.run_config import RunConfig
from tensorflow_estimator.python.estimator.training import EvalSpec
from tensorflow_estimator.python.estimator.training import train_and_evaluate
from tensorflow_estimator.python.estimator.training import TrainSpec

# pylint: enable=unused-import,line-too-long,wildcard-import
