"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: gen_bigquery_reader_ops.cc
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
@tf_export('big_query_reader')
def big_query_reader(project_id, dataset_id, table_id, columns, timestamp_millis, container="", shared_name="", test_end_point="", name=None):
  r"""A Reader that outputs rows from a BigQuery table as tensorflow Examples.

  Args:
    project_id: A `string`. GCP project ID.
    dataset_id: A `string`. BigQuery Dataset ID.
    table_id: A `string`. Table to read.
    columns: A list of `strings`.
      List of columns to read. Leave empty to read all columns.
    timestamp_millis: An `int`.
      Table snapshot timestamp in millis since epoch. Relative
      (negative or zero) snapshot times are not allowed. For more details, see
      'Table Decorators' in BigQuery docs.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    test_end_point: An optional `string`. Defaults to `""`.
      Do not use. For testing purposes only.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`. The handle to reference the Reader.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("big_query_reader op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  project_id = _execute.make_str(project_id, "project_id")
  dataset_id = _execute.make_str(dataset_id, "dataset_id")
  table_id = _execute.make_str(table_id, "table_id")
  if not isinstance(columns, (list, tuple)):
    raise TypeError(
        "Expected list for 'columns' argument to "
        "'big_query_reader' Op, not %r." % columns)
  columns = [_execute.make_str(_s, "columns") for _s in columns]
  timestamp_millis = _execute.make_int(timestamp_millis, "timestamp_millis")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if test_end_point is None:
    test_end_point = ""
  test_end_point = _execute.make_str(test_end_point, "test_end_point")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "BigQueryReader", project_id=project_id, dataset_id=dataset_id,
                          table_id=table_id, columns=columns,
                          timestamp_millis=timestamp_millis,
                          container=container, shared_name=shared_name,
                          test_end_point=test_end_point, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          big_query_reader, project_id=project_id, dataset_id=dataset_id,
                            table_id=table_id, columns=columns,
                            timestamp_millis=timestamp_millis,
                            container=container, shared_name=shared_name,
                            test_end_point=test_end_point, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "project_id",
            _op.get_attr("project_id"), "dataset_id",
            _op.get_attr("dataset_id"), "table_id", _op.get_attr("table_id"),
            "columns", _op.get_attr("columns"), "timestamp_millis",
            _op.get_attr("timestamp_millis"), "test_end_point",
            _op.get_attr("test_end_point"))
  _execute.record_gradient(
      "BigQueryReader", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def big_query_reader_eager_fallback(project_id, dataset_id, table_id, columns, timestamp_millis, container="", shared_name="", test_end_point="", name=None, ctx=None):
  raise RuntimeError("big_query_reader op does not support eager execution. Arg 'reader_handle' is a ref.")

@_dispatch.add_dispatch_list
@tf_export('generate_big_query_reader_partitions')
def generate_big_query_reader_partitions(project_id, dataset_id, table_id, columns, timestamp_millis, num_partitions, test_end_point="", name=None):
  r"""Generates serialized partition messages suitable for batch reads.

  This op should not be used directly by clients. Instead, the
  bigquery_reader_ops.py file defines a clean interface to the reader.

  Args:
    project_id: A `string`. GCP project ID.
    dataset_id: A `string`. BigQuery Dataset ID.
    table_id: A `string`. Table to read.
    columns: A list of `strings`.
      List of columns to read. Leave empty to read all columns.
    timestamp_millis: An `int`.
      Table snapshot timestamp in millis since epoch. Relative
      (negative or zero) snapshot times are not allowed. For more details, see
      'Table Decorators' in BigQuery docs.
    num_partitions: An `int`. Number of partitions to split the table into.
    test_end_point: An optional `string`. Defaults to `""`.
      Do not use. For testing purposes only.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`. Serialized table partitions.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "GenerateBigQueryReaderPartitions", name,
        _ctx._post_execution_callbacks, "project_id", project_id,
        "dataset_id", dataset_id, "table_id", table_id, "columns", columns,
        "timestamp_millis", timestamp_millis, "num_partitions",
        num_partitions, "test_end_point", test_end_point)
      return _result
    except _core._FallbackException:
      try:
        return generate_big_query_reader_partitions_eager_fallback(
            project_id=project_id, dataset_id=dataset_id, table_id=table_id,
            columns=columns, timestamp_millis=timestamp_millis,
            num_partitions=num_partitions, test_end_point=test_end_point,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              generate_big_query_reader_partitions, project_id=project_id,
                                                    dataset_id=dataset_id,
                                                    table_id=table_id,
                                                    columns=columns,
                                                    timestamp_millis=timestamp_millis,
                                                    num_partitions=num_partitions,
                                                    test_end_point=test_end_point,
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
  project_id = _execute.make_str(project_id, "project_id")
  dataset_id = _execute.make_str(dataset_id, "dataset_id")
  table_id = _execute.make_str(table_id, "table_id")
  if not isinstance(columns, (list, tuple)):
    raise TypeError(
        "Expected list for 'columns' argument to "
        "'generate_big_query_reader_partitions' Op, not %r." % columns)
  columns = [_execute.make_str(_s, "columns") for _s in columns]
  timestamp_millis = _execute.make_int(timestamp_millis, "timestamp_millis")
  num_partitions = _execute.make_int(num_partitions, "num_partitions")
  if test_end_point is None:
    test_end_point = ""
  test_end_point = _execute.make_str(test_end_point, "test_end_point")
  try:
    _, _, _op = _op_def_lib._apply_op_helper(
        "GenerateBigQueryReaderPartitions", project_id=project_id,
                                            dataset_id=dataset_id,
                                            table_id=table_id,
                                            columns=columns,
                                            timestamp_millis=timestamp_millis,
                                            num_partitions=num_partitions,
                                            test_end_point=test_end_point,
                                            name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          generate_big_query_reader_partitions, project_id=project_id,
                                                dataset_id=dataset_id,
                                                table_id=table_id,
                                                columns=columns,
                                                timestamp_millis=timestamp_millis,
                                                num_partitions=num_partitions,
                                                test_end_point=test_end_point,
                                                name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("project_id", _op.get_attr("project_id"), "dataset_id",
            _op.get_attr("dataset_id"), "table_id", _op.get_attr("table_id"),
            "columns", _op.get_attr("columns"), "timestamp_millis",
            _op.get_attr("timestamp_millis"), "num_partitions",
            _op.get_attr("num_partitions"), "test_end_point",
            _op.get_attr("test_end_point"))
  _execute.record_gradient(
      "GenerateBigQueryReaderPartitions", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def generate_big_query_reader_partitions_eager_fallback(project_id, dataset_id, table_id, columns, timestamp_millis, num_partitions, test_end_point="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function generate_big_query_reader_partitions
  """
  _ctx = ctx if ctx else _context.context()
  project_id = _execute.make_str(project_id, "project_id")
  dataset_id = _execute.make_str(dataset_id, "dataset_id")
  table_id = _execute.make_str(table_id, "table_id")
  if not isinstance(columns, (list, tuple)):
    raise TypeError(
        "Expected list for 'columns' argument to "
        "'generate_big_query_reader_partitions' Op, not %r." % columns)
  columns = [_execute.make_str(_s, "columns") for _s in columns]
  timestamp_millis = _execute.make_int(timestamp_millis, "timestamp_millis")
  num_partitions = _execute.make_int(num_partitions, "num_partitions")
  if test_end_point is None:
    test_end_point = ""
  test_end_point = _execute.make_str(test_end_point, "test_end_point")
  _inputs_flat = []
  _attrs = ("project_id", project_id, "dataset_id", dataset_id, "table_id",
  table_id, "columns", columns, "timestamp_millis", timestamp_millis,
  "num_partitions", num_partitions, "test_end_point", test_end_point)
  _result = _execute.execute(b"GenerateBigQueryReaderPartitions", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "GenerateBigQueryReaderPartitions", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "BigQueryReader"
#   output_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "container"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   attr {
#     name: "project_id"
#     type: "string"
#   }
#   attr {
#     name: "dataset_id"
#     type: "string"
#   }
#   attr {
#     name: "table_id"
#     type: "string"
#   }
#   attr {
#     name: "columns"
#     type: "list(string)"
#   }
#   attr {
#     name: "timestamp_millis"
#     type: "int"
#   }
#   attr {
#     name: "test_end_point"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "GenerateBigQueryReaderPartitions"
#   output_arg {
#     name: "partitions"
#     type: DT_STRING
#   }
#   attr {
#     name: "project_id"
#     type: "string"
#   }
#   attr {
#     name: "dataset_id"
#     type: "string"
#   }
#   attr {
#     name: "table_id"
#     type: "string"
#   }
#   attr {
#     name: "columns"
#     type: "list(string)"
#   }
#   attr {
#     name: "timestamp_millis"
#     type: "int"
#   }
#   attr {
#     name: "num_partitions"
#     type: "int"
#   }
#   attr {
#     name: "test_end_point"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\355\001\n\016BigQueryReader\032\024\n\rreader_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"\024\n\nproject_id\022\006string\"\024\n\ndataset_id\022\006string\"\022\n\010table_id\022\006string\"\027\n\007columns\022\014list(string)\"\027\n\020timestamp_millis\022\003int\"\034\n\016test_end_point\022\006string\032\002\022\000\210\001\001\n\331\001\n GenerateBigQueryReaderPartitions\032\016\n\npartitions\030\007\"\024\n\nproject_id\022\006string\"\024\n\ndataset_id\022\006string\"\022\n\010table_id\022\006string\"\027\n\007columns\022\014list(string)\"\027\n\020timestamp_millis\022\003int\"\025\n\016num_partitions\022\003int\"\034\n\016test_end_point\022\006string\032\002\022\000")
