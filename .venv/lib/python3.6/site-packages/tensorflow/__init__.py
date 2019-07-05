# Copyright 2015 The TensorFlow Authors. All Rights Reserved.
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
"""Bring in all of the public TensorFlow interface into this module."""

from __future__ import absolute_import as _absolute_import
from __future__ import division as _division
from __future__ import print_function as _print_function

import os as _os

# pylint: disable=g-bad-import-order
from tensorflow.python import pywrap_tensorflow  # pylint: disable=unused-import

from tensorflow._api.v1 import app
from tensorflow._api.v1 import autograph
from tensorflow._api.v1 import bitwise
from tensorflow._api.v1 import compat
from tensorflow._api.v1 import data
from tensorflow._api.v1 import debugging
from tensorflow._api.v1 import distribute
from tensorflow._api.v1 import distributions
from tensorflow._api.v1 import dtypes
from tensorflow._api.v1 import errors
from tensorflow._api.v1 import experimental
from tensorflow._api.v1 import feature_column
from tensorflow._api.v1 import gfile
from tensorflow._api.v1 import graph_util
from tensorflow._api.v1 import image
from tensorflow._api.v1 import initializers
from tensorflow._api.v1 import io
from tensorflow._api.v1 import keras
from tensorflow._api.v1 import layers
from tensorflow._api.v1 import linalg
from tensorflow._api.v1 import lite
from tensorflow._api.v1 import logging
from tensorflow._api.v1 import losses
from tensorflow._api.v1 import manip
from tensorflow._api.v1 import math
from tensorflow._api.v1 import metrics
from tensorflow._api.v1 import nn
from tensorflow._api.v1 import profiler
from tensorflow._api.v1 import python_io
from tensorflow._api.v1 import quantization
from tensorflow._api.v1 import queue
from tensorflow._api.v1 import ragged
from tensorflow._api.v1 import random
from tensorflow._api.v1 import resource_loader
from tensorflow._api.v1 import saved_model
from tensorflow._api.v1 import sets
from tensorflow._api.v1 import signal
from tensorflow._api.v1 import sparse
from tensorflow._api.v1 import spectral
from tensorflow._api.v1 import strings
from tensorflow._api.v1 import summary
from tensorflow._api.v1 import sysconfig
from tensorflow._api.v1 import test
from tensorflow._api.v1 import train
from tensorflow._api.v1 import user_ops
from tensorflow._api.v1 import version
from tensorflow.lite.python.lite import _import_graph_def as import_graph_def
from tensorflow.python import AggregationMethod
from tensorflow.python import Assert
from tensorflow.python import AttrValue
from tensorflow.python import ConditionalAccumulator
from tensorflow.python import ConditionalAccumulatorBase
from tensorflow.python import ConfigProto
from tensorflow.python import Constant as constant_initializer
from tensorflow.python import DType
from tensorflow.python import DeviceSpec
from tensorflow.python import Dimension
from tensorflow.python import Event
from tensorflow.python import FIFOQueue
from tensorflow.python import FixedLenFeature
from tensorflow.python import FixedLenSequenceFeature
from tensorflow.python import FixedLengthRecordReader
from tensorflow.python import GPUOptions
from tensorflow.python import GlorotNormal as glorot_normal_initializer
from tensorflow.python import GlorotUniform as glorot_uniform_initializer
from tensorflow.python import GradientTape
from tensorflow.python import Graph
from tensorflow.python import GraphDef
from tensorflow.python import GraphKeys
from tensorflow.python import GraphOptions
from tensorflow.python import HistogramProto
from tensorflow.python import IdentityReader
from tensorflow.python import IndexedSlices
from tensorflow.python import InteractiveSession
from tensorflow.python import LMDBReader
from tensorflow.python import LogMessage
from tensorflow.python import MetaGraphDef
from tensorflow.python import NameAttrList
from tensorflow.python import NoGradient
from tensorflow.python import NoGradient as NotDifferentiable
from tensorflow.python import NoGradient as no_gradient
from tensorflow.python import NodeDef
from tensorflow.python import Ones as ones_initializer
from tensorflow.python import OpError
from tensorflow.python import Operation
from tensorflow.python import OptimizerOptions
from tensorflow.python import Orthogonal as orthogonal_initializer
from tensorflow.python import PaddingFIFOQueue
from tensorflow.python import Print
from tensorflow.python import PriorityQueue
from tensorflow.python import QueueBase
from tensorflow.python import RandomNormal as random_normal_initializer
from tensorflow.python import RandomShuffleQueue
from tensorflow.python import RandomUniform as random_uniform_initializer
from tensorflow.python import ReaderBase
from tensorflow.python import RegisterGradient
from tensorflow.python import RunMetadata
from tensorflow.python import RunOptions
from tensorflow.python import Session
from tensorflow.python import SessionLog
from tensorflow.python import SparseConditionalAccumulator
from tensorflow.python import SparseFeature
from tensorflow.python import SparseTensor
from tensorflow.python import SparseTensorValue
from tensorflow.python import Summary
from tensorflow.python import SummaryMetadata
from tensorflow.python import TFRecordReader
from tensorflow.python import Tensor
from tensorflow.python import TensorArray
from tensorflow.python import TensorInfo
from tensorflow.python import TensorShape
from tensorflow.python import TextLineReader
from tensorflow.python import TruncatedNormal as truncated_normal_initializer
from tensorflow.python import UnconnectedGradients
from tensorflow.python import UniformUnitScaling as uniform_unit_scaling_initializer
from tensorflow.python import VarLenFeature
from tensorflow.python import VariableAggregation
from tensorflow.python import VariableScope
from tensorflow.python import VariableSynchronization
from tensorflow.python import VariableV1 as Variable
from tensorflow.python import VarianceScaling as variance_scaling_initializer
from tensorflow.python import WholeFileReader
from tensorflow.python import Zeros as zeros_initializer
from tensorflow.python import abs
from tensorflow.python import accumulate_n
from tensorflow.python import acos
from tensorflow.python import acosh
from tensorflow.python import add
from tensorflow.python import add_check_numerics_ops
from tensorflow.python import add_n
from tensorflow.python import add_to_collection
from tensorflow.python import add_to_collections
from tensorflow.python import all_variables
from tensorflow.python import angle
from tensorflow.python import arg_max
from tensorflow.python import arg_min
from tensorflow.python import argmax
from tensorflow.python import argmin
from tensorflow.python import argsort
from tensorflow.python import as_dtype
from tensorflow.python import as_string
from tensorflow.python import asin
from tensorflow.python import asinh
from tensorflow.python import assert_equal
from tensorflow.python import assert_greater
from tensorflow.python import assert_greater_equal
from tensorflow.python import assert_integer
from tensorflow.python import assert_less
from tensorflow.python import assert_less_equal
from tensorflow.python import assert_near
from tensorflow.python import assert_negative
from tensorflow.python import assert_non_negative
from tensorflow.python import assert_non_positive
from tensorflow.python import assert_none_equal
from tensorflow.python import assert_positive
from tensorflow.python import assert_proper_iterable
from tensorflow.python import assert_rank
from tensorflow.python import assert_rank_at_least
from tensorflow.python import assert_rank_in
from tensorflow.python import assert_same_float_dtype
from tensorflow.python import assert_scalar
from tensorflow.python import assert_type
from tensorflow.python import assert_variables_initialized
from tensorflow.python import assign
from tensorflow.python import assign_add
from tensorflow.python import assign_sub
from tensorflow.python import atan
from tensorflow.python import atan2
from tensorflow.python import atanh
from tensorflow.python import batch_gather
from tensorflow.python import batch_to_space
from tensorflow.python import batch_to_space_nd
from tensorflow.python import betainc
from tensorflow.python import bincount_v1 as bincount
from tensorflow.python import bitcast
from tensorflow.python import boolean_mask
from tensorflow.python import broadcast_dynamic_shape
from tensorflow.python import broadcast_static_shape
from tensorflow.python import broadcast_to
from tensorflow.python import case
from tensorflow.python import cast
from tensorflow.python import ceil
from tensorflow.python import check_numerics
from tensorflow.python import cholesky
from tensorflow.python import cholesky_solve
from tensorflow.python import clip_by_average_norm
from tensorflow.python import clip_by_global_norm
from tensorflow.python import clip_by_norm
from tensorflow.python import clip_by_value
from tensorflow.python import colocate_with
from tensorflow.python import complex
from tensorflow.python import concat
from tensorflow.python import cond
from tensorflow.python import conj
from tensorflow.python import container
from tensorflow.python import control_dependencies
from tensorflow.python import convert_to_tensor
from tensorflow.python import convert_to_tensor_or_indexed_slices
from tensorflow.python import convert_to_tensor_or_sparse_tensor
from tensorflow.python import cos
from tensorflow.python import cosh
from tensorflow.python import count_nonzero
from tensorflow.python import count_up_to
from tensorflow.python import create_partitioned_variables
from tensorflow.python import cross
from tensorflow.python import cumprod
from tensorflow.python import cumsum
from tensorflow.python import custom_gradient
from tensorflow.python import decode_base64
from tensorflow.python import decode_compressed
from tensorflow.python import decode_csv
from tensorflow.python import decode_json_example
from tensorflow.python import decode_raw
from tensorflow.python import delete_session_tensor
from tensorflow.python import depth_to_space
from tensorflow.python import dequantize
from tensorflow.python import deserialize_many_sparse
from tensorflow.python import device
from tensorflow.python import diag
from tensorflow.python import diag_part
from tensorflow.python import digamma
from tensorflow.python import div
from tensorflow.python import div_no_nan
from tensorflow.python import divide
from tensorflow.python import dynamic_partition
from tensorflow.python import dynamic_stitch
from tensorflow.python import edit_distance
from tensorflow.python import einsum
from tensorflow.python import enable_eager_execution
from tensorflow.python import encode_base64
from tensorflow.python import equal
from tensorflow.python import erf
from tensorflow.python import erfc
from tensorflow.python import executing_eagerly
from tensorflow.python import exp
from tensorflow.python import expand_dims
from tensorflow.python import expm1
from tensorflow.python import extract_image_patches
from tensorflow.python import extract_volume_patches
from tensorflow.python import eye
from tensorflow.python import fake_quant_with_min_max_args
from tensorflow.python import fake_quant_with_min_max_args_gradient
from tensorflow.python import fake_quant_with_min_max_vars
from tensorflow.python import fake_quant_with_min_max_vars_gradient
from tensorflow.python import fake_quant_with_min_max_vars_per_channel
from tensorflow.python import fake_quant_with_min_max_vars_per_channel_gradient
from tensorflow.python import fill
from tensorflow.python import fixed_size_partitioner
from tensorflow.python import floor
from tensorflow.python import floor_div
from tensorflow.python import floor_mod as floormod
from tensorflow.python import floor_mod as mod
from tensorflow.python import floordiv
from tensorflow.python import foldl
from tensorflow.python import foldr
from tensorflow.python import gather
from tensorflow.python import gather_nd
from tensorflow.python import get_collection
from tensorflow.python import get_collection_ref
from tensorflow.python import get_default_graph
from tensorflow.python import get_default_session
from tensorflow.python import get_local_variable
from tensorflow.python import get_seed
from tensorflow.python import get_session_handle
from tensorflow.python import get_session_tensor
from tensorflow.python import get_variable
from tensorflow.python import get_variable_scope
from tensorflow.python import global_norm
from tensorflow.python import global_variables
from tensorflow.python import global_variables_initializer
from tensorflow.python import gradients
from tensorflow.python import greater
from tensorflow.python import greater_equal
from tensorflow.python import group
from tensorflow.python import guarantee_const
from tensorflow.python import hessians
from tensorflow.python import histogram_fixed_width
from tensorflow.python import histogram_fixed_width_bins
from tensorflow.python import identity
from tensorflow.python import identity_n
from tensorflow.python import igamma
from tensorflow.python import igammac
from tensorflow.python import imag
from tensorflow.python import initialize_all_tables
from tensorflow.python import initialize_all_variables
from tensorflow.python import initialize_local_variables
from tensorflow.python import initialize_variables
from tensorflow.python import invert_permutation
from tensorflow.python import is_finite
from tensorflow.python import is_inf
from tensorflow.python import is_nan
from tensorflow.python import is_non_decreasing
from tensorflow.python import is_numeric_tensor
from tensorflow.python import is_strictly_increasing
from tensorflow.python import is_variable_initialized
from tensorflow.python import lbeta
from tensorflow.python import less
from tensorflow.python import less_equal
from tensorflow.python import lgamma
from tensorflow.python import lin_space
from tensorflow.python import lin_space as linspace
from tensorflow.python import load_file_system_library
from tensorflow.python import load_library
from tensorflow.python import load_op_library
from tensorflow.python import local_variables
from tensorflow.python import local_variables_initializer
from tensorflow.python import log
from tensorflow.python import log1p
from tensorflow.python import log_sigmoid
from tensorflow.python import logical_and
from tensorflow.python import logical_not
from tensorflow.python import logical_or
from tensorflow.python import logical_xor
from tensorflow.python import make_ndarray
from tensorflow.python import make_template
from tensorflow.python import make_tensor_proto
from tensorflow.python import map_fn
from tensorflow.python import matching_files
from tensorflow.python import matmul
from tensorflow.python import matrix_band_part
from tensorflow.python import matrix_determinant
from tensorflow.python import matrix_diag
from tensorflow.python import matrix_diag_part
from tensorflow.python import matrix_inverse
from tensorflow.python import matrix_set_diag
from tensorflow.python import matrix_solve
from tensorflow.python import matrix_solve_ls
from tensorflow.python import matrix_square_root
from tensorflow.python import matrix_transpose
from tensorflow.python import matrix_triangular_solve
from tensorflow.python import maximum
from tensorflow.python import meshgrid
from tensorflow.python import min_max_variable_partitioner
from tensorflow.python import minimum
from tensorflow.python import model_variables
from tensorflow.python import moving_average_variables
from tensorflow.python import multinomial
from tensorflow.python import multiply
from tensorflow.python import name_scope
from tensorflow.python import neg as negative
from tensorflow.python import no_op
from tensorflow.python import no_regularizer
from tensorflow.python import norm
from tensorflow.python import not_equal
from tensorflow.python import one_hot
from tensorflow.python import ones
from tensorflow.python import ones_like
from tensorflow.python import op_scope
from tensorflow.python import pad
from tensorflow.python import parallel_stack
from tensorflow.python import parse_example
from tensorflow.python import parse_single_example
from tensorflow.python import parse_single_sequence_example
from tensorflow.python import parse_tensor
from tensorflow.python import placeholder
from tensorflow.python import placeholder_with_default
from tensorflow.python import polygamma
from tensorflow.python import pow
from tensorflow.python import py_func
from tensorflow.python import qr
from tensorflow.python import quantize
from tensorflow.python import quantize_v2
from tensorflow.python import quantized_concat
from tensorflow.python import random_crop
from tensorflow.python import random_gamma
from tensorflow.python import random_normal
from tensorflow.python import random_poisson
from tensorflow.python import random_shuffle
from tensorflow.python import random_uniform
from tensorflow.python import range
from tensorflow.python import rank
from tensorflow.python import read_file
from tensorflow.python import real
from tensorflow.python import real_div as realdiv
from tensorflow.python import reciprocal
from tensorflow.python import reduce_all_v1 as reduce_all
from tensorflow.python import reduce_any_v1 as reduce_any
from tensorflow.python import reduce_join
from tensorflow.python import reduce_logsumexp_v1 as reduce_logsumexp
from tensorflow.python import reduce_max_v1 as reduce_max
from tensorflow.python import reduce_mean_v1 as reduce_mean
from tensorflow.python import reduce_min_v1 as reduce_min
from tensorflow.python import reduce_prod_v1 as reduce_prod
from tensorflow.python import reduce_sum_v1 as reduce_sum
from tensorflow.python import regex_replace
from tensorflow.python import register_tensor_conversion_function
from tensorflow.python import report_uninitialized_variables
from tensorflow.python import required_space_to_batch_paddings
from tensorflow.python import reset_default_graph
from tensorflow.python import reshape
from tensorflow.python import reverse
from tensorflow.python import reverse as reverse_v2
from tensorflow.python import reverse_sequence
from tensorflow.python import rint
from tensorflow.python import roll
from tensorflow.python import round
from tensorflow.python import rsqrt
from tensorflow.python import saturate_cast
from tensorflow.python import scalar_mul
from tensorflow.python import scan
from tensorflow.python import scatter_add
from tensorflow.python import scatter_div
from tensorflow.python import scatter_max
from tensorflow.python import scatter_min
from tensorflow.python import scatter_mul
from tensorflow.python import scatter_nd
from tensorflow.python import scatter_nd_add
from tensorflow.python import scatter_nd_sub
from tensorflow.python import scatter_nd_update
from tensorflow.python import scatter_sub
from tensorflow.python import scatter_update
from tensorflow.python import searchsorted
from tensorflow.python import segment_max
from tensorflow.python import segment_mean
from tensorflow.python import segment_min
from tensorflow.python import segment_prod
from tensorflow.python import segment_sum
from tensorflow.python import self_adjoint_eig
from tensorflow.python import self_adjoint_eigvals
from tensorflow.python import sequence_mask
from tensorflow.python import serialize_many_sparse
from tensorflow.python import serialize_sparse
from tensorflow.python import serialize_tensor
from tensorflow.python import set_random_seed
from tensorflow.python import setdiff1d
from tensorflow.python import shape
from tensorflow.python import shape_n
from tensorflow.python import sigmoid
from tensorflow.python import sign
from tensorflow.python import sin
from tensorflow.python import sinh
from tensorflow.python import size
from tensorflow.python import slice
from tensorflow.python import sort
from tensorflow.python import space_to_batch
from tensorflow.python import space_to_batch_nd
from tensorflow.python import space_to_depth
from tensorflow.python import sparse_add
from tensorflow.python import sparse_concat
from tensorflow.python import sparse_fill_empty_rows
from tensorflow.python import sparse_mask
from tensorflow.python import sparse_mat_mul as sparse_matmul
from tensorflow.python import sparse_maximum
from tensorflow.python import sparse_merge
from tensorflow.python import sparse_minimum
from tensorflow.python import sparse_placeholder
from tensorflow.python import sparse_reduce_max
from tensorflow.python import sparse_reduce_max_sparse
from tensorflow.python import sparse_reduce_sum
from tensorflow.python import sparse_reduce_sum_sparse
from tensorflow.python import sparse_reorder
from tensorflow.python import sparse_reset_shape
from tensorflow.python import sparse_reshape
from tensorflow.python import sparse_retain
from tensorflow.python import sparse_segment_mean
from tensorflow.python import sparse_segment_sqrt_n
from tensorflow.python import sparse_segment_sum
from tensorflow.python import sparse_slice
from tensorflow.python import sparse_softmax
from tensorflow.python import sparse_split
from tensorflow.python import sparse_tensor_dense_matmul
from tensorflow.python import sparse_tensor_to_dense
from tensorflow.python import sparse_to_dense
from tensorflow.python import sparse_to_indicator
from tensorflow.python import sparse_transpose
from tensorflow.python import split
from tensorflow.python import sqrt
from tensorflow.python import square
from tensorflow.python import squared_difference
from tensorflow.python import squeeze
from tensorflow.python import stack
from tensorflow.python import stop_gradient
from tensorflow.python import strided_slice
from tensorflow.python import string_join
from tensorflow.python import string_split
from tensorflow.python import string_strip
from tensorflow.python import string_to_hash_bucket_fast
from tensorflow.python import string_to_hash_bucket_strong
from tensorflow.python import substr_deprecated as substr
from tensorflow.python import subtract
from tensorflow.python import svd
from tensorflow.python import tables_initializer
from tensorflow.python import tan
from tensorflow.python import tanh
from tensorflow.python import tensor_scatter_add
from tensorflow.python import tensor_scatter_sub
from tensorflow.python import tensor_scatter_update
from tensorflow.python import tensordot
from tensorflow.python import tile
from tensorflow.python import timestamp
from tensorflow.python import to_bfloat16
from tensorflow.python import to_complex128
from tensorflow.python import to_complex64
from tensorflow.python import to_double
from tensorflow.python import to_float
from tensorflow.python import to_int32
from tensorflow.python import to_int64
from tensorflow.python import trace
from tensorflow.python import trainable_variables
from tensorflow.python import transpose
from tensorflow.python import truediv
from tensorflow.python import truncate_div as truncatediv
from tensorflow.python import truncate_mod as truncatemod
from tensorflow.python import truncated_normal
from tensorflow.python import tuple
from tensorflow.python import unique
from tensorflow.python import unique_with_counts
from tensorflow.python import unravel_index
from tensorflow.python import unsorted_segment_max
from tensorflow.python import unsorted_segment_mean
from tensorflow.python import unsorted_segment_min
from tensorflow.python import unsorted_segment_prod
from tensorflow.python import unsorted_segment_sqrt_n
from tensorflow.python import unsorted_segment_sum
from tensorflow.python import unstack
from tensorflow.python import variable_axis_size_partitioner
from tensorflow.python import variable_op_scope
from tensorflow.python import variable_scope
from tensorflow.python import variables_initializer
from tensorflow.python import verify_tensor_all_finite
from tensorflow.python import where
from tensorflow.python import while_loop
from tensorflow.python import write_file
from tensorflow.python import zeros
from tensorflow.python import zeros_like
from tensorflow.python import zeta
from tensorflow.python.compat.compat import disable_v2_behavior
from tensorflow.python.compat.compat import enable_v2_behavior
from tensorflow.python.eager.wrap_function import wrap_function
from tensorflow.python.framework.constant_op import constant_v1 as constant
from tensorflow.python.framework.dtypes import QUANTIZED_DTYPES
from tensorflow.python.framework.dtypes import bfloat16
from tensorflow.python.framework.dtypes import bool
from tensorflow.python.framework.dtypes import complex128
from tensorflow.python.framework.dtypes import complex64
from tensorflow.python.framework.dtypes import double
from tensorflow.python.framework.dtypes import float16
from tensorflow.python.framework.dtypes import float32
from tensorflow.python.framework.dtypes import float64
from tensorflow.python.framework.dtypes import half
from tensorflow.python.framework.dtypes import int16
from tensorflow.python.framework.dtypes import int32
from tensorflow.python.framework.dtypes import int64
from tensorflow.python.framework.dtypes import int8
from tensorflow.python.framework.dtypes import qint16
from tensorflow.python.framework.dtypes import qint32
from tensorflow.python.framework.dtypes import qint8
from tensorflow.python.framework.dtypes import quint16
from tensorflow.python.framework.dtypes import quint8
from tensorflow.python.framework.dtypes import resource
from tensorflow.python.framework.dtypes import string
from tensorflow.python.framework.dtypes import uint16
from tensorflow.python.framework.dtypes import uint32
from tensorflow.python.framework.dtypes import uint64
from tensorflow.python.framework.dtypes import uint8
from tensorflow.python.framework.dtypes import variant
from tensorflow.python.framework.ops import disable_eager_execution
from tensorflow.python.framework.ops import init_scope
from tensorflow.python.framework.tensor_shape import dimension_at_index
from tensorflow.python.framework.tensor_shape import dimension_value
from tensorflow.python.framework.tensor_shape import disable_v2_tensorshape
from tensorflow.python.framework.tensor_shape import enable_v2_tensorshape
from tensorflow.python.framework.tensor_spec import TensorSpec
from tensorflow.python.framework.versions import COMPILER_VERSION
from tensorflow.python.framework.versions import COMPILER_VERSION as __compiler_version__
from tensorflow.python.framework.versions import CXX11_ABI_FLAG
from tensorflow.python.framework.versions import CXX11_ABI_FLAG as __cxx11_abi_flag__
from tensorflow.python.framework.versions import GIT_VERSION
from tensorflow.python.framework.versions import GIT_VERSION as __git_version__
from tensorflow.python.framework.versions import GRAPH_DEF_VERSION
from tensorflow.python.framework.versions import GRAPH_DEF_VERSION_MIN_CONSUMER
from tensorflow.python.framework.versions import GRAPH_DEF_VERSION_MIN_PRODUCER
from tensorflow.python.framework.versions import MONOLITHIC_BUILD
from tensorflow.python.framework.versions import MONOLITHIC_BUILD as __monolithic_build__
from tensorflow.python.framework.versions import VERSION
from tensorflow.python.framework.versions import VERSION as __version__
from tensorflow.python.ops.array_ops import newaxis
from tensorflow.python.ops.check_ops import ensure_shape
from tensorflow.python.ops.confusion_matrix import confusion_matrix_v1 as confusion_matrix
from tensorflow.python.ops.gen_parsing_ops import string_to_number
from tensorflow.python.ops.gen_spectral_ops import fft
from tensorflow.python.ops.gen_spectral_ops import fft2d
from tensorflow.python.ops.gen_spectral_ops import fft3d
from tensorflow.python.ops.gen_spectral_ops import ifft
from tensorflow.python.ops.gen_spectral_ops import ifft2d
from tensorflow.python.ops.gen_spectral_ops import ifft3d
from tensorflow.python.ops.gen_string_ops import string_to_hash_bucket
from tensorflow.python.ops.logging_ops import print_v2 as print
from tensorflow.python.ops.ragged.ragged_tensor import RaggedTensor
from tensorflow.python.ops.script_ops import eager_py_func as py_function
from tensorflow.python.ops.state_ops import batch_scatter_update
from tensorflow.python.ops.variable_scope import AUTO_REUSE
from tensorflow.python.ops.variable_scope import disable_resource_variables
from tensorflow.python.ops.variable_scope import enable_resource_variables
from tensorflow.python.ops.variable_scope import variable_creator_scope_v1 as variable_creator_scope
from tensorflow.python.platform.tf_logging import get_logger
_names_with_underscore = ['__version__', '__git_version__', '__compiler_version__', '__cxx11_abi_flag__', '__monolithic_build__']
__all__ = [_s for _s in dir() if not _s.startswith('_')]
__all__.extend([_s for _s in _names_with_underscore])


from tensorflow.python.tools import component_api_helper as _component_api_helper
_component_api_helper.package_hook(
    parent_package_str=__name__,
    child_package_str=('tensorflow_estimator.python.estimator.api.estimator'))

from tensorflow.python.util.lazy_loader import LazyLoader  # pylint: disable=g-import-not-at-top
_CONTRIB_WARNING = """
WARNING: The TensorFlow contrib module will not be included in TensorFlow 2.0.
For more information, please see:
  * https://github.com/tensorflow/community/blob/master/rfcs/20180907-contrib-sunset.md
  * https://github.com/tensorflow/addons
If you depend on functionality not listed there, please file an issue.
"""
contrib = LazyLoader('contrib', globals(), 'tensorflow.contrib',
                     _CONTRIB_WARNING)
del LazyLoader
# The templated code that replaces the placeholder above sometimes
# sets the __all__ variable. If it does, we have to be sure to add
# "contrib".
if '__all__' in vars():
  vars()['__all__'].append('contrib')

from tensorflow.python.platform import flags  # pylint: disable=g-import-not-at-top
app.flags = flags  # pylint: disable=undefined-variable

# Make sure directory containing top level submodules is in
# the __path__ so that "from tensorflow.foo import bar" works.
_tf_api_dir = _os.path.dirname(_os.path.dirname(app.__file__))  # pylint: disable=undefined-variable
if _tf_api_dir not in __path__:
  __path__.append(_tf_api_dir)


# These symbols appear because we import the python package which
# in turn imports from tensorflow.core and tensorflow.python. They
# must come from this module. So python adds these symbols for the
# resolution to succeed.
# pylint: disable=undefined-variable
try:
  del python
  del core
except NameError:
  # Don't fail if these modules are not available.
  # For e.g. this file will be originally placed under tensorflow/_api/v1 which
  # does not have 'python', 'core' directories. Then, it will be copied
  # to tensorflow/ which does have these two directories.
  pass
# Similarly for compiler. Do it separately to make sure we do this even if the
# others don't exist.
try:
  del compiler
except NameError:
  pass
# pylint: enable=undefined-variable
