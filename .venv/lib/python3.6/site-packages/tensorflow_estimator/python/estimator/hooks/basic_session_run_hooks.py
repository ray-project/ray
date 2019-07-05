# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
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
"""Some common SessionRunHook classes."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from tensorflow.python.training.basic_session_run_hooks import CheckpointSaverHook
from tensorflow.python.training.basic_session_run_hooks import CheckpointSaverListener
from tensorflow.python.training.basic_session_run_hooks import FeedFnHook
from tensorflow.python.training.basic_session_run_hooks import FinalOpsHook
from tensorflow.python.training.basic_session_run_hooks import GlobalStepWaiterHook
from tensorflow.python.training.basic_session_run_hooks import LoggingTensorHook
from tensorflow.python.training.basic_session_run_hooks import NanLossDuringTrainingError
from tensorflow.python.training.basic_session_run_hooks import NanTensorHook
from tensorflow.python.training.basic_session_run_hooks import ProfilerHook
from tensorflow.python.training.basic_session_run_hooks import SecondOrStepTimer
from tensorflow.python.training.basic_session_run_hooks import StepCounterHook
from tensorflow.python.training.basic_session_run_hooks import StopAtStepHook
from tensorflow.python.training.basic_session_run_hooks import SummarySaverHook
from tensorflow.python.util.tf_export import estimator_export

estimator_export("estimator.SecondOrStepTimer")(SecondOrStepTimer)
estimator_export("estimator.LoggingTensorHook")(LoggingTensorHook)
estimator_export("estimator.StopAtStepHook")(StopAtStepHook)
estimator_export("estimator.CheckpointSaverListener")(CheckpointSaverListener)
estimator_export("estimator.CheckpointSaverHook")(CheckpointSaverHook)
estimator_export("estimator.StepCounterHook")(StepCounterHook)
estimator_export("estimator.NanLossDuringTrainingError")(
    NanLossDuringTrainingError)
estimator_export("estimator.NanTensorHook")(NanTensorHook)
estimator_export("estimator.SummarySaverHook")(SummarySaverHook)
estimator_export("estimator.GlobalStepWaiterHook")(GlobalStepWaiterHook)
estimator_export("estimator.FinalOpsHook")(FinalOpsHook)
estimator_export("estimator.FeedFnHook")(FeedFnHook)
estimator_export("estimator.ProfilerHook")(ProfilerHook)
