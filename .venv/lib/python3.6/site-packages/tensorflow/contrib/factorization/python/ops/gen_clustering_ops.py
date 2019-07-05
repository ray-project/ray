"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_clustering_ops.cc
"""

import collections as _collections
import six as _six

from tensorflow.python import pywrap_tensorflow as _pywrap_tensorflow
from tensorflow.python.eager import context as _context
from tensorflow.python.eager import core as _core
from tensorflow.python.eager import execute as _execute
from tensorflow.python.framework import dtypes as _dtypes
from tensorflow.python.framework import errors as _errors
from tensorflow.python.framework import tensor_shape as _tensor_shape

from tensorflow.core.framework import op_def_pb2 as _op_def_pb2
# Needed to trigger the call to _set_call_cpp_shape_fn.
from tensorflow.python.framework import common_shapes as _common_shapes
from tensorflow.python.framework import op_def_registry as _op_def_registry
from tensorflow.python.framework import ops as _ops
from tensorflow.python.framework import op_def_library as _op_def_library
from tensorflow.python.util.deprecation import deprecated_endpoints
from tensorflow.python.util import dispatch as _dispatch
from tensorflow.python.util.tf_export import tf_export


@_dispatch.add_dispatch_list
@tf_export('kmc2_chain_initialization')
def kmc2_chain_initialization(distances, seed, name=None):
  r"""Returns the index of a data point that should be added to the seed set.

  Entries in distances are assumed to be squared distances of candidate points to
  the already sampled centers in the seed set. The op constructs one Markov chain
  of the k-MC^2 algorithm and returns the index of one candidate point to be added
  as an additional cluster center.

  Args:
    distances: A `Tensor` of type `float32`.
      Vector with squared distances to the closest previously sampled
      cluster center for each candidate point.
    seed: A `Tensor` of type `int64`.
      Scalar. Seed for initializing the random number generator.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`. Scalar with the index of the sampled point.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "KMC2ChainInitialization", name, _ctx._post_execution_callbacks,
        distances, seed)
      return _result
    except _core._FallbackException:
      try:
        return kmc2_chain_initialization_eager_fallback(
            distances, seed, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              kmc2_chain_initialization, distances=distances, seed=seed,
                                         name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "KMC2ChainInitialization", distances=distances, seed=seed, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          kmc2_chain_initialization, distances=distances, seed=seed,
                                     name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "KMC2ChainInitialization", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def kmc2_chain_initialization_eager_fallback(distances, seed, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function kmc2_chain_initialization
  """
  _ctx = ctx if ctx else _context.context()
  distances = _ops.convert_to_tensor(distances, _dtypes.float32)
  seed = _ops.convert_to_tensor(seed, _dtypes.int64)
  _inputs_flat = [distances, seed]
  _attrs = None
  _result = _execute.execute(b"KMC2ChainInitialization", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "KMC2ChainInitialization", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("KMC2ChainInitialization")(None)


@_dispatch.add_dispatch_list
@tf_export('kmeans_plus_plus_initialization')
def kmeans_plus_plus_initialization(points, num_to_sample, seed, num_retries_per_sample, name=None):
  r"""Selects num_to_sample rows of input using the KMeans++ criterion.

  Rows of points are assumed to be input points. One row is selected at random.
  Subsequent rows are sampled with probability proportional to the squared L2
  distance from the nearest row selected thus far till num_to_sample rows have
  been sampled.

  Args:
    points: A `Tensor` of type `float32`.
      Matrix of shape (n, d). Rows are assumed to be input points.
    num_to_sample: A `Tensor` of type `int64`.
      Scalar. The number of rows to sample. This value must not be
      larger than n.
    seed: A `Tensor` of type `int64`.
      Scalar. Seed for initializing the random number generator.
    num_retries_per_sample: A `Tensor` of type `int64`.
      Scalar. For each row that is sampled, this parameter
      specifies the number of additional points to draw from the current
      distribution before selecting the best. If a negative value is specified, a
      heuristic is used to sample O(log(num_to_sample)) additional points.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `float32`.
    Matrix of shape (num_to_sample, d). The sampled rows.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "KmeansPlusPlusInitialization", name, _ctx._post_execution_callbacks,
        points, num_to_sample, seed, num_retries_per_sample)
      return _result
    except _core._FallbackException:
      try:
        return kmeans_plus_plus_initialization_eager_fallback(
            points, num_to_sample, seed, num_retries_per_sample, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              kmeans_plus_plus_initialization, points=points,
                                               num_to_sample=num_to_sample,
                                               seed=seed,
                                               num_retries_per_sample=num_retries_per_sample,
                                               name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "KmeansPlusPlusInitialization", points=points,
                                        num_to_sample=num_to_sample,
                                        seed=seed,
                                        num_retries_per_sample=num_retries_per_sample,
                                        name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          kmeans_plus_plus_initialization, points=points,
                                           num_to_sample=num_to_sample,
                                           seed=seed,
                                           num_retries_per_sample=num_retries_per_sample,
                                           name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "KmeansPlusPlusInitialization", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def kmeans_plus_plus_initialization_eager_fallback(points, num_to_sample, seed, num_retries_per_sample, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function kmeans_plus_plus_initialization
  """
  _ctx = ctx if ctx else _context.context()
  points = _ops.convert_to_tensor(points, _dtypes.float32)
  num_to_sample = _ops.convert_to_tensor(num_to_sample, _dtypes.int64)
  seed = _ops.convert_to_tensor(seed, _dtypes.int64)
  num_retries_per_sample = _ops.convert_to_tensor(num_retries_per_sample, _dtypes.int64)
  _inputs_flat = [points, num_to_sample, seed, num_retries_per_sample]
  _attrs = None
  _result = _execute.execute(b"KmeansPlusPlusInitialization", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "KmeansPlusPlusInitialization", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

_ops.RegisterShape("KmeansPlusPlusInitialization")(None)


_nearest_neighbors_outputs = ["nearest_center_indices",
                             "nearest_center_distances"]
_NearestNeighborsOutput = _collections.namedtuple(
    "NearestNeighbors", _nearest_neighbors_outputs)


@_dispatch.add_dispatch_list
@tf_export('nearest_neighbors')
def nearest_neighbors(points, centers, k, name=None):
  r"""Selects the k nearest centers for each point.

  Rows of points are assumed to be input points. Rows of centers are assumed to be
  the list of candidate centers. For each point, the k centers that have least L2
  distance to it are computed.

  Args:
    points: A `Tensor` of type `float32`.
      Matrix of shape (n, d). Rows are assumed to be input points.
    centers: A `Tensor` of type `float32`.
      Matrix of shape (m, d). Rows are assumed to be centers.
    k: A `Tensor` of type `int64`.
      Scalar. Number of nearest centers to return for each point. If k is larger
      than m, then only m centers are returned.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (nearest_center_indices, nearest_center_distances).

    nearest_center_indices: A `Tensor` of type `int64`. Matrix of shape (n, min(m, k)). Each row contains the
      indices of the centers closest to the corresponding point, ordered by
      increasing distance.
    nearest_center_distances: A `Tensor` of type `float32`. Matrix of shape (n, min(m, k)). Each row contains the
      squared L2 distance to the corresponding center in nearest_center_indices.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "NearestNeighbors", name, _ctx._post_execution_callbacks, points,
        centers, k)
      _result = _NearestNeighborsOutput._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return nearest_neighbors_eager_fallback(
            points, centers, k, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              nearest_neighbors, points=points, centers=centers, k=k,
                                 name=name)
        if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
          return result
        raise
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "NearestNeighbors", points=points, centers=centers, k=k, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          nearest_neighbors, points=points, centers=centers, k=k, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "NearestNeighbors", _inputs_flat, _attrs, _result, name)
  _result = _NearestNeighborsOutput._make(_result)
  return _result



def nearest_neighbors_eager_fallback(points, centers, k, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function nearest_neighbors
  """
  _ctx = ctx if ctx else _context.context()
  points = _ops.convert_to_tensor(points, _dtypes.float32)
  centers = _ops.convert_to_tensor(centers, _dtypes.float32)
  k = _ops.convert_to_tensor(k, _dtypes.int64)
  _inputs_flat = [points, centers, k]
  _attrs = None
  _result = _execute.execute(b"NearestNeighbors", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "NearestNeighbors", _inputs_flat, _attrs, _result, name)
  _result = _NearestNeighborsOutput._make(_result)
  return _result

_ops.RegisterShape("NearestNeighbors")(None)

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "KMC2ChainInitialization"
#   input_arg {
#     name: "distances"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "seed"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "index"
#     type: DT_INT64
#   }
# }
# op {
#   name: "KmeansPlusPlusInitialization"
#   input_arg {
#     name: "points"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "num_to_sample"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "seed"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "num_retries_per_sample"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "samples"
#     type: DT_FLOAT
#   }
# }
# op {
#   name: "NearestNeighbors"
#   input_arg {
#     name: "points"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "centers"
#     type: DT_FLOAT
#   }
#   input_arg {
#     name: "k"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "nearest_center_indices"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "nearest_center_distances"
#     type: DT_FLOAT
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n=\n\027KMC2ChainInitialization\022\r\n\tdistances\030\001\022\010\n\004seed\030\t\032\t\n\005index\030\t\np\n\034KmeansPlusPlusInitialization\022\n\n\006points\030\001\022\021\n\rnum_to_sample\030\t\022\010\n\004seed\030\t\022\032\n\026num_retries_per_sample\030\t\032\013\n\007samples\030\001\nl\n\020NearestNeighbors\022\n\n\006points\030\001\022\013\n\007centers\030\001\022\005\n\001k\030\t\032\032\n\026nearest_center_indices\030\t\032\034\n\030nearest_center_distances\030\001")
