# Copyright 2018 The TensorFlow Authors. All Rights Reserved.
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
"""Python module for calculating health pills.

"Health pills" are succinct summaries of tensor values, including data such as
total number of elements, counts of NaN and infinity elements, counts of
positive, negative and zero elements, min, max, mean and variances, etc.

N.B.: tfdbg's DebugNumericSummary op calculates health pills as well. This
module computes the same health pill values, but in Python. See the following
code for a detailed description of elements of a health pill:
https://github.com/tensorflow/tensorflow/blob/master/tensorflow/core/ops/debug_ops.cc#L157
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


def calc_health_pill(tensor):
  """Calculate health pill of a tensor.

  Args:
    tensor: An instance of `np.array` (for initialized tensors) or
      `tensorflow.python.debug.lib.debug_data.InconvertibleTensorProto`
      (for unininitialized tensors).

  Returns:
    If `tensor` is an initialized tensor of numeric or boolean types:
      the calculated health pill, as a `list` of `float`s.
    Else if `tensor` is an initialized tensor with `string`, `resource` or any
      other non-numeric types:
      `None`.
    Else (i.e., if `tensor` is uninitialized): An all-zero `list`, with the
      first element signifying that the tensor is uninitialized.
  """
  health_pill = [0.0] * 14

  # TODO(cais): Add unit test for this method that compares results with
  #   DebugNumericSummary output.

  # Is tensor initialized.
  if not isinstance(tensor, np.ndarray):
    return health_pill
  health_pill[0] = 1.0

  if not (np.issubdtype(tensor.dtype, np.float) or
          np.issubdtype(tensor.dtype, np.complex) or
          np.issubdtype(tensor.dtype, np.integer) or
          tensor.dtype == np.bool):
    return None

  # Total number of elements.
  health_pill[1] = float(np.size(tensor))

  # TODO(cais): Further performance optimization?
  nan_mask = np.isnan(tensor)
  inf_mask = np.isinf(tensor)
  # Number of NaN elements.
  health_pill[2] = float(np.sum(nan_mask))
  # Number of -Inf elements.
  health_pill[3] = float(np.sum(tensor == -np.inf))
  # Number of finite negative elements.
  health_pill[4] = float(np.sum(
      np.logical_and(np.logical_not(inf_mask), tensor < 0.0)))
  # Number of zero elements.
  health_pill[5] = float(np.sum(tensor == 0.0))
  # Number finite positive elements.
  health_pill[6] = float(np.sum(
      np.logical_and(np.logical_not(inf_mask), tensor > 0.0)))
  # Number of +Inf elements.
  health_pill[7] = float(np.sum(tensor == np.inf))

  finite_subset = tensor[
      np.logical_and(np.logical_not(nan_mask), np.logical_not(inf_mask))]
  if np.size(finite_subset):
    # Finite subset is not empty.
    # Minimum of the non-NaN non-Inf elements.
    health_pill[8] = float(np.min(finite_subset))
    # Maximum of the non-NaN non-Inf elements.
    health_pill[9] = float(np.max(finite_subset))
    # Mean of the non-NaN non-Inf elements.
    health_pill[10] = float(np.mean(finite_subset))
    # Variance of the non-NaN non-Inf elements.
    health_pill[11] = float(np.var(finite_subset))
  else:
    # If no finite element exists:
    # Set minimum to +inf.
    health_pill[8] = np.inf
    # Set maximum to -inf.
    health_pill[9] = -np.inf
    # Set mean to NaN.
    health_pill[10] = np.nan
    # Set variance to NaN.
    health_pill[11] = np.nan

  # DType encoded as a number.
  # TODO(cais): Convert numpy dtype to corresponding tensorflow dtype enum.
  health_pill[12] = -1.0
  # ndims.
  health_pill[13] = float(len(tensor.shape))
  # Size of the dimensions.
  health_pill.extend([float(x) for x in tensor.shape])
  return health_pill
