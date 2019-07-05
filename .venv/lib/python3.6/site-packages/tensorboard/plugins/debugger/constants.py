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
"""Contains important constants used throughout the debugger plugin code."""

DEBUGGER_PLUGIN_NAME = "debugger"

# The directory within logdir we write debugger-related events to.
DEBUGGER_DATA_DIRECTORY_NAME = "__debugger_data__"

# The suffix of watch keys of debug numeric summary nodes, which contain health
# pill data.
DEBUG_NUMERIC_SUMMARY_SUFFIX = ":DebugNumericSummary"

# The sentinel step value to use if the (session_run_index) value cannot be
# determined from the event with core metadata.
SENTINEL_FOR_UNDETERMINED_STEP = -1

# The version of events to use. TensorBoard uses this to determine whether to
# use step values to purge events. Health pill events should not be purged based
# on step value, so this version must be set to >= 2.
EVENTS_VERSION = "brain.Event:2"

# The minimum number of elements within the tensor provided by a
# DebugNumericSummaryOp.
MIN_DEBUG_NUMERIC_SUMMARY_TENSOR_LENGTH = 14

# Keys used to store categories of numeric alerts.
NAN_KEY = "nan"
NEG_INF_KEY = "neg_inf"
POS_INF_KEY = "pos_inf"

# The indices within NumericSummary debug ops of bad values.
NAN_NUMERIC_SUMMARY_OP_INDEX = 2
NEG_INF_NUMERIC_SUMMARY_OP_INDEX = 3
POS_INF_NUMERIC_SUMMARY_OP_INDEX = 7

# The name of the file used to back up the registry.
ALERT_REGISTRY_BACKUP_FILE_NAME = "debugger_alert_registry_backup.json"
