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
"""Data structures and algorithms for numerics alert.

The alerts are generated when a Tensor's elements contain bad values,
including nan, -inf and +inf.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import json
import re

import tensorflow as tf

from tensorflow.python import debug as tf_debug
from tensorboard.plugins.debugger import constants

# The following two namedtuples are the same except that
# 1) `timestamp` in `NumericsAlert` is the timestamp of a single alerting event,
#    while `first_timestamp` in `NumericsAlertReportRow` is the first (earliest)
#    timestamp of a se tof aggregated `NumericsAlert`s.
# 2) The counts in `NumericsAlert` are the counts of elements in the tensor,
#    while the event counts in `NumericsAlertReportRow` are counts of previous
#    `NumericsAlert` events of the corresponding categories.
NumericsAlert = collections.namedtuple(
    "NumericsAlert",
    ["device_name", "tensor_name", "timestamp", "nan_count", "neg_inf_count",
     "pos_inf_count"])

NumericsAlertReportRow = collections.namedtuple(
    "NumericsAlertReportRow",
    ["device_name", "tensor_name", "first_timestamp", "nan_event_count",
     "neg_inf_event_count", "pos_inf_event_count"])

# Used to reconstruct an _EventTracker from data read from disk. When updating
# this named tuple, make sure to keep the properties of _EventTracker in sync.
EventTrackerDescription = collections.namedtuple(
    "EventTrackerDescription",
    ["event_count", "first_timestamp", "last_timestamp"])

# Used to reconstruct NumericsAlertHistory.
HistoryTriplet = collections.namedtuple(
    "HistoryTriplet",
    ["device", "tensor", "jsonable_history"])


class _EventTracker(object):
  """Track events for a single category of values (NaN, -Inf, or +Inf)."""

  def __init__(self, event_count=0, first_timestamp=-1, last_timestamp=-1):
    """Tracks events for a single category of values.

    Args:
      event_count: The initial event count to use.
      first_timestamp: The timestamp of the first event with this value.
      last_timestamp: The timestamp of the last event with this category of
          values.
    """

    # When updating the properties of this class, make sure to keep
    # EventTrackerDescription in sync so that data can be written to and from
    # disk correctly.
    self.event_count = event_count
    self.first_timestamp = first_timestamp
    self.last_timestamp = last_timestamp

  def add(self, timestamp):
    if self.event_count == 0:
      self.first_timestamp = timestamp
      self.last_timestamp = timestamp
    else:
      if timestamp < self.first_timestamp:
        self.first_timestamp = timestamp
      if timestamp > self.last_timestamp:
        self.last_timestamp = timestamp
    self.event_count += 1

  def get_description(self):
    return EventTrackerDescription(
        self.event_count, self.first_timestamp, self.last_timestamp)


class NumericsAlertHistory(object):
  """History of numerics alerts."""

  def __init__(self, initialization_list=None):
    """Stores alert history for a single device, tensor pair.

    Args:
      initialization_list: (`list`) An optional list parsed from JSON read
        from disk. That entity is used to initialize this NumericsAlertHistory.
        Use the create_jsonable_object method of this class to create such an
        object.
    """
    if initialization_list:
      # Use data to initialize this NumericsAlertHistory.
      self._trackers = {}
      for value_category_key, description_list in initialization_list.items():
        description = EventTrackerDescription._make(description_list)
        self._trackers[value_category_key] = _EventTracker(
            event_count=description.event_count,
            first_timestamp=description.first_timestamp,
            last_timestamp=description.last_timestamp)
    else:
      # Start cleanly. With no prior data.
      self._trackers = {
          constants.NAN_KEY: _EventTracker(),
          constants.NEG_INF_KEY: _EventTracker(),
          constants.POS_INF_KEY: _EventTracker(),
      }

  def add(self, numerics_alert):
    if numerics_alert.nan_count:
      self._trackers[constants.NAN_KEY].add(numerics_alert.timestamp)
    if numerics_alert.neg_inf_count:
      self._trackers[constants.NEG_INF_KEY].add(numerics_alert.timestamp)
    if numerics_alert.pos_inf_count:
      self._trackers[constants.POS_INF_KEY].add(numerics_alert.timestamp)

  def first_timestamp(self, event_key=None):
    """Obtain the first timestamp.

    Args:
      event_key: the type key of the sought events (e.g., constants.NAN_KEY).
      If None, includes all event type keys.

    Returns:
      First (earliest) timestamp of all the events of the given type (or all
        event types if event_key is None).
    """
    if event_key is None:
      timestamps = [self._trackers[key].first_timestamp
                    for key in self._trackers]
      return min(timestamp for timestamp in timestamps if timestamp >= 0)
    else:
      return self._trackers[event_key].first_timestamp

  def last_timestamp(self, event_key=None):
    """Obtain the last timestamp.

    Args:
      event_key: the type key of the sought events (e.g., constants.NAN_KEY). If
      None, includes all event type keys.

    Returns:
      Last (latest) timestamp of all the events of the given type (or all
        event types if event_key is None).
    """
    if event_key is None:
      timestamps = [self._trackers[key].first_timestamp
                    for key in self._trackers]
      return max(timestamp for timestamp in timestamps if timestamp >= 0)
    else:
      return self._trackers[event_key].last_timestamp

  def event_count(self, event_key):
    """Obtain event count.

    Args:
      event_key: the type key of the sought events (e.g., constants.NAN_KEY). If
      None, includes all event type keys.

    Returns:
      If event_key is None, return the sum of the event_count of all event
      types. Otherwise, return the event_count of the specified event type.
    """
    return self._trackers[event_key].event_count

  def create_jsonable_history(self):
    """Creates a JSON-able representation of this object.

    Returns:
      A dictionary mapping key to EventTrackerDescription (which can be used to
      create event trackers).
    """
    return {value_category_key: tracker.get_description()
            for (value_category_key, tracker) in self._trackers.items()}


class NumericsAlertRegistry(object):
  """A registry for alerts on numerics (e.g., due to NaNs and infinities)."""

  def __init__(self, capacity=100, initialization_list=None):
    """Constructor.

    Args:
      capacity: (`int`) maximum number of device-tensor keys to store.
      initialization_list: (`list`) An optional list (parsed from JSON) that
        is used to initialize the data within this registry. Use the
        create_jsonable_registry method of NumericsAlertRegistry to create such
        a list.
    """
    self._capacity = capacity

    # A map from device-tensor key to a the TensorAlertRecord namedtuple.
    # The device-tensor key is a 2-tuple of the format (device_name, node_name).
    # E.g., ("/job:worker/replica:0/task:1/gpu:0", "cross_entropy/Log:0").
    self._data = dict()

    if initialization_list:
      # Initialize the alert registry using the data passed in. This might be
      # backup data used to restore the registry after say a borg pre-emption.
      for entry in initialization_list:
        triplet = HistoryTriplet._make(entry)
        self._data[(triplet.device, triplet.tensor)] = NumericsAlertHistory(
            initialization_list=triplet.jsonable_history)

  def register(self, numerics_alert):
    """Register an alerting numeric event.

    Args:
      numerics_alert: An instance of `NumericsAlert`.
    """
    key = (numerics_alert.device_name, numerics_alert.tensor_name)
    if key in self._data:
      self._data[key].add(numerics_alert)
    else:
      if len(self._data) < self._capacity:
        history = NumericsAlertHistory()
        history.add(numerics_alert)
        self._data[key] = history

  def report(self, device_name_filter=None, tensor_name_filter=None):
    """Get a report of offending device/tensor names.

    The report includes information about the device name, tensor name, first
    (earliest) timestamp of the alerting events from the tensor, in addition to
    counts of nan, positive inf and negative inf events.

    Args:
      device_name_filter: regex filter for device name, or None (not filtered).
      tensor_name_filter: regex filter for tensor name, or None (not filtered).

    Returns:
      A list of NumericsAlertReportRow, sorted by the first_timestamp in
      asecnding order.
    """
    report = []
    for key in self._data:
      device_name, tensor_name = key
      history = self._data[key]
      report.append(
          NumericsAlertReportRow(
              device_name=device_name,
              tensor_name=tensor_name,
              first_timestamp=history.first_timestamp(),
              nan_event_count=history.event_count(constants.NAN_KEY),
              neg_inf_event_count=history.event_count(constants.NEG_INF_KEY),
              pos_inf_event_count=history.event_count(constants.POS_INF_KEY)))

    if device_name_filter:
      device_name_pattern = re.compile(device_name_filter)
      report = [item for item in report
                if device_name_pattern.match(item.device_name)]
    if tensor_name_filter:
      tensor_name_pattern = re.compile(tensor_name_filter)
      report = [item for item in report
                if tensor_name_pattern.match(item.tensor_name)]
    # Sort results chronologically.
    return sorted(report, key=lambda x: x.first_timestamp)

  def create_jsonable_registry(self):
    """Creates a JSON-able representation of this object.

    Returns:
      A dictionary mapping (device, tensor name) to JSON-able object
      representations of NumericsAlertHistory.
    """
    # JSON does not support tuples as keys. Only strings. Therefore, we store
    # the device name, tensor name, and dictionary data within a 3-item list.
    return [HistoryTriplet(pair[0], pair[1], history.create_jsonable_history())
            for (pair, history) in self._data.items()]


def extract_numerics_alert(event):
  """Determines whether a health pill event contains bad values.

  A bad value is one of NaN, -Inf, or +Inf.

  Args:
    event: (`Event`) A `tensorflow.Event` proto from `DebugNumericSummary`
      ops.

  Returns:
    An instance of `NumericsAlert`, if bad values are found.
    `None`, if no bad values are found.

  Raises:
    ValueError: if the event does not have the expected tag prefix or the
      debug op name is not the expected debug op name suffix.
  """
  value = event.summary.value[0]
  debugger_plugin_metadata_content = None
  if value.HasField("metadata"):
    plugin_data = value.metadata.plugin_data
    if plugin_data.plugin_name == constants.DEBUGGER_PLUGIN_NAME:
      debugger_plugin_metadata_content = plugin_data.content

  if not debugger_plugin_metadata_content:
    raise ValueError("Event proto input lacks debugger plugin SummaryMetadata.")

  debugger_plugin_metadata_content = tf.compat.as_text(
      debugger_plugin_metadata_content)
  try:
    content_object = json.loads(debugger_plugin_metadata_content)
    device_name = content_object["device"]
  except (KeyError, ValueError) as e:
    raise ValueError("Could not determine device from JSON string %r, %r" %
                     (debugger_plugin_metadata_content, e))

  debug_op_suffix = ":DebugNumericSummary"
  if not value.node_name.endswith(debug_op_suffix):
    raise ValueError(
        "Event proto input does not have the expected debug op suffix %s" %
        debug_op_suffix)
  tensor_name = value.node_name[:-len(debug_op_suffix)]

  elements = tf_debug.load_tensor_from_event(event)
  nan_count = elements[constants.NAN_NUMERIC_SUMMARY_OP_INDEX]
  neg_inf_count = elements[constants.NEG_INF_NUMERIC_SUMMARY_OP_INDEX]
  pos_inf_count = elements[constants.POS_INF_NUMERIC_SUMMARY_OP_INDEX]
  if nan_count > 0 or neg_inf_count > 0 or pos_inf_count > 0:
    return NumericsAlert(
        device_name, tensor_name, event.wall_time, nan_count, neg_inf_count,
        pos_inf_count)
  return None
