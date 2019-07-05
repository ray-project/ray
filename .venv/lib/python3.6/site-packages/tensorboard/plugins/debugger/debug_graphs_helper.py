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
"""Helper methods and classes for tfdbg-decorated graphs."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import threading

from tensorflow.python.debug.lib import debug_graphs


class DebugGraphWrapper(object):
  """A wrapper for potentially debugger-decorated GraphDef."""

  def __init__(self, graph_def):
    self._graph_def = graph_def
    # A map from debug op to list of debug-op-attached tensors.
    self._grpc_gated_tensors = dict()
    self._grpc_gated_lock = threading.Lock()
    self._maybe_base_expanded_node_names = None
    self._node_name_lock = threading.Lock()

  def get_gated_grpc_tensors(self, matching_debug_op=None):
    """Extract all nodes with gated-gRPC debug ops attached.

    Uses cached values if available.
    This method is thread-safe.

    Args:
      graph_def: A tf.GraphDef proto.
      matching_debug_op: Return tensors and nodes with only matching the
        specified debug op name (optional). If `None`, will extract only
        `DebugIdentity` debug ops.

    Returns:
      A list of (node_name, op_type, output_slot, debug_op) tuples.
    """
    with self._grpc_gated_lock:
      matching_debug_op = matching_debug_op or 'DebugIdentity'
      if matching_debug_op not in self._grpc_gated_tensors:
        # First, construct a map from node name to op type.
        node_name_to_op_type = dict(
            (node.name, node.op) for node in self._graph_def.node)

        # Second, populate the output list.
        gated = []
        for node in self._graph_def.node:
          if node.op == matching_debug_op:
            for attr_key in node.attr:
              if attr_key == 'gated_grpc' and node.attr[attr_key].b:
                node_name, output_slot, _, debug_op = (
                    debug_graphs.parse_debug_node_name(node.name))
                gated.append(
                    (node_name, node_name_to_op_type[node_name], output_slot,
                     debug_op))
                break
        self._grpc_gated_tensors[matching_debug_op] = gated

      return self._grpc_gated_tensors[matching_debug_op]

  def maybe_base_expanded_node_name(self, node_name):
    """Expand the base name if there are node names nested under the node.

    For example, if there are two nodes in the graph, "a" and "a/read", then
    calling this function on "a" will give "a/(a)", a form that points at
    a leaf node in the nested TensorBoard graph. Calling this function on
    "a/read" will just return "a/read", because there is no node nested under
    it.

    This method is thread-safe.

    Args:
      node_name: Name of the node.
      graph_def: The `GraphDef` that the node is a part of.

    Returns:
      Possibly base-expanded node name.
    """
    with self._node_name_lock:
      # Lazily populate the map from original node name to base-expanded ones.
      if self._maybe_base_expanded_node_names is None:
        self._maybe_base_expanded_node_names = dict()
        # Sort all the node names.
        sorted_names = sorted(node.name for node in self._graph_def.node)
        for i, name in enumerate(sorted_names):
          j = i + 1
          while j < len(sorted_names) and sorted_names[j].startswith(name):
            if sorted_names[j].startswith(name + '/'):
              self._maybe_base_expanded_node_names[name] = (
                  name + '/(' + name.split('/')[-1] + ')')
              break
            j += 1
      return self._maybe_base_expanded_node_names.get(node_name, node_name)

  @property
  def graph_def(self):
    return self._graph_def
