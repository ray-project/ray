"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: experimental_dataset_ops.cc
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


def experimental_assert_next_dataset(input_dataset, transformations, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    transformations: A `Tensor` of type `string`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalAssertNextDataset", name, _ctx._post_execution_callbacks,
        input_dataset, transformations, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_assert_next_dataset_eager_fallback(
            input_dataset, transformations, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_assert_next_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_assert_next_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalAssertNextDataset", input_dataset=input_dataset,
                                         transformations=transformations,
                                         output_types=output_types,
                                         output_shapes=output_shapes,
                                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalAssertNextDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_assert_next_dataset_eager_fallback(input_dataset, transformations, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_assert_next_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_assert_next_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_assert_next_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  transformations = _ops.convert_to_tensor(transformations, _dtypes.string)
  _inputs_flat = [input_dataset, transformations]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalAssertNextDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalAssertNextDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_bytes_produced_stats_dataset(input_dataset, tag, output_types, output_shapes, name=None):
  r"""Records the bytes size of each element of `input_dataset` in a StatsAggregator.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    tag: A `Tensor` of type `string`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalBytesProducedStatsDataset", name,
        _ctx._post_execution_callbacks, input_dataset, tag, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_bytes_produced_stats_dataset_eager_fallback(
            input_dataset, tag, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_bytes_produced_stats_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_bytes_produced_stats_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalBytesProducedStatsDataset", input_dataset=input_dataset,
                                                 tag=tag,
                                                 output_types=output_types,
                                                 output_shapes=output_shapes,
                                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalBytesProducedStatsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_bytes_produced_stats_dataset_eager_fallback(input_dataset, tag, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_bytes_produced_stats_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_bytes_produced_stats_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_bytes_produced_stats_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  _inputs_flat = [input_dataset, tag]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalBytesProducedStatsDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalBytesProducedStatsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_csv_dataset(filenames, compression_type, buffer_size, header, field_delim, use_quote_delim, na_value, select_cols, record_defaults, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    filenames: A `Tensor` of type `string`.
    compression_type: A `Tensor` of type `string`.
    buffer_size: A `Tensor` of type `int64`.
    header: A `Tensor` of type `bool`.
    field_delim: A `Tensor` of type `string`.
    use_quote_delim: A `Tensor` of type `bool`.
    na_value: A `Tensor` of type `string`.
    select_cols: A `Tensor` of type `int64`.
    record_defaults: A list of `Tensor` objects with types from: `float32`, `float64`, `int32`, `int64`, `string`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalCSVDataset", name, _ctx._post_execution_callbacks,
        filenames, compression_type, buffer_size, header, field_delim,
        use_quote_delim, na_value, select_cols, record_defaults,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_csv_dataset_eager_fallback(
            filenames, compression_type, buffer_size, header, field_delim,
            use_quote_delim, na_value, select_cols, record_defaults,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_csv_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalCSVDataset", filenames=filenames,
                                  compression_type=compression_type,
                                  buffer_size=buffer_size, header=header,
                                  field_delim=field_delim,
                                  use_quote_delim=use_quote_delim,
                                  na_value=na_value, select_cols=select_cols,
                                  record_defaults=record_defaults,
                                  output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalCSVDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_csv_dataset_eager_fallback(filenames, compression_type, buffer_size, header, field_delim, use_quote_delim, na_value, select_cols, record_defaults, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_csv_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_csv_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_output_types, record_defaults = _execute.convert_to_mixed_eager_tensors(record_defaults, _ctx)
  filenames = _ops.convert_to_tensor(filenames, _dtypes.string)
  compression_type = _ops.convert_to_tensor(compression_type, _dtypes.string)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  header = _ops.convert_to_tensor(header, _dtypes.bool)
  field_delim = _ops.convert_to_tensor(field_delim, _dtypes.string)
  use_quote_delim = _ops.convert_to_tensor(use_quote_delim, _dtypes.bool)
  na_value = _ops.convert_to_tensor(na_value, _dtypes.string)
  select_cols = _ops.convert_to_tensor(select_cols, _dtypes.int64)
  _inputs_flat = [filenames, compression_type, buffer_size, header, field_delim, use_quote_delim, na_value, select_cols] + list(record_defaults)
  _attrs = ("output_types", _attr_output_types, "output_shapes",
  output_shapes)
  _result = _execute.execute(b"ExperimentalCSVDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalCSVDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_dataset_cardinality(input_dataset, name=None):
  r"""Returns the cardinality of `input_dataset`.

  Returns the cardinality of `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the dataset to return cardinality for.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalDatasetCardinality", name,
        _ctx._post_execution_callbacks, input_dataset)
      return _result
    except _core._FallbackException:
      try:
        return experimental_dataset_cardinality_eager_fallback(
            input_dataset, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalDatasetCardinality", input_dataset=input_dataset,
                                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ExperimentalDatasetCardinality", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_dataset_cardinality_eager_fallback(input_dataset, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_dataset_cardinality
  """
  _ctx = ctx if ctx else _context.context()
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = None
  _result = _execute.execute(b"ExperimentalDatasetCardinality", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalDatasetCardinality", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_dataset_to_tf_record(input_dataset, filename, compression_type, name=None):
  r"""Writes the given dataset to the given file using the TFRecord format.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the dataset to write.
    filename: A `Tensor` of type `string`.
      A scalar string tensor representing the filename to use.
    compression_type: A `Tensor` of type `string`.
      A scalar string tensor containing either (i) the empty string (no
      compression), (ii) "ZLIB", or (iii) "GZIP".
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalDatasetToTFRecord", name, _ctx._post_execution_callbacks,
        input_dataset, filename, compression_type)
      return _result
    except _core._FallbackException:
      try:
        return experimental_dataset_to_tf_record_eager_fallback(
            input_dataset, filename, compression_type, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalDatasetToTFRecord", input_dataset=input_dataset,
                                         filename=filename,
                                         compression_type=compression_type,
                                         name=name)
  return _op
  _result = None
  return _result



def experimental_dataset_to_tf_record_eager_fallback(input_dataset, filename, compression_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_dataset_to_tf_record
  """
  _ctx = ctx if ctx else _context.context()
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  filename = _ops.convert_to_tensor(filename, _dtypes.string)
  compression_type = _ops.convert_to_tensor(compression_type, _dtypes.string)
  _inputs_flat = [input_dataset, filename, compression_type]
  _attrs = None
  _result = _execute.execute(b"ExperimentalDatasetToTFRecord", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def experimental_dense_to_sparse_batch_dataset(input_dataset, batch_size, row_shape, output_types, output_shapes, name=None):
  r"""Creates a dataset that batches input elements into a SparseTensor.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A handle to an input dataset. Must have a single component.
    batch_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a
      batch.
    row_shape: A `Tensor` of type `int64`.
      A vector representing the dense shape of each row in the produced
      SparseTensor. The shape may be partially specified, using `-1` to indicate
      that a particular dimension should use the maximum size of all batch elements.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalDenseToSparseBatchDataset", name,
        _ctx._post_execution_callbacks, input_dataset, batch_size, row_shape,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_dense_to_sparse_batch_dataset_eager_fallback(
            input_dataset, batch_size, row_shape, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_dense_to_sparse_batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_dense_to_sparse_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalDenseToSparseBatchDataset", input_dataset=input_dataset,
                                                 batch_size=batch_size,
                                                 row_shape=row_shape,
                                                 output_types=output_types,
                                                 output_shapes=output_shapes,
                                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalDenseToSparseBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_dense_to_sparse_batch_dataset_eager_fallback(input_dataset, batch_size, row_shape, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_dense_to_sparse_batch_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_dense_to_sparse_batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_dense_to_sparse_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  batch_size = _ops.convert_to_tensor(batch_size, _dtypes.int64)
  row_shape = _ops.convert_to_tensor(row_shape, _dtypes.int64)
  _inputs_flat = [input_dataset, batch_size, row_shape]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalDenseToSparseBatchDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalDenseToSparseBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_directed_interleave_dataset(selector_input_dataset, data_input_datasets, output_types, output_shapes, name=None):
  r"""A substitute for `InterleaveDataset` on a fixed list of `N` datasets.

  Args:
    selector_input_dataset: A `Tensor` of type `variant`.
      A dataset of scalar `DT_INT64` elements that determines which of the
      `N` data inputs should produce the next output element.
    data_input_datasets: A list of at least 1 `Tensor` objects with type `variant`.
      `N` datasets with the same type that will be interleaved according to
      the values of `selector_input_dataset`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalDirectedInterleaveDataset", name,
        _ctx._post_execution_callbacks, selector_input_dataset,
        data_input_datasets, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_directed_interleave_dataset_eager_fallback(
            selector_input_dataset, data_input_datasets,
            output_types=output_types, output_shapes=output_shapes, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(data_input_datasets, (list, tuple)):
    raise TypeError(
        "Expected list for 'data_input_datasets' argument to "
        "'experimental_directed_interleave_dataset' Op, not %r." % data_input_datasets)
  _attr_N = len(data_input_datasets)
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_directed_interleave_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_directed_interleave_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalDirectedInterleaveDataset", selector_input_dataset=selector_input_dataset,
                                                 data_input_datasets=data_input_datasets,
                                                 output_types=output_types,
                                                 output_shapes=output_shapes,
                                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "N", _op.get_attr("N"))
  _execute.record_gradient(
      "ExperimentalDirectedInterleaveDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_directed_interleave_dataset_eager_fallback(selector_input_dataset, data_input_datasets, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_directed_interleave_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(data_input_datasets, (list, tuple)):
    raise TypeError(
        "Expected list for 'data_input_datasets' argument to "
        "'experimental_directed_interleave_dataset' Op, not %r." % data_input_datasets)
  _attr_N = len(data_input_datasets)
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_directed_interleave_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_directed_interleave_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  selector_input_dataset = _ops.convert_to_tensor(selector_input_dataset, _dtypes.variant)
  data_input_datasets = _ops.convert_n_to_tensor(data_input_datasets, _dtypes.variant)
  _inputs_flat = [selector_input_dataset] + list(data_input_datasets)
  _attrs = ("output_types", output_types, "output_shapes", output_shapes, "N",
  _attr_N)
  _result = _execute.execute(b"ExperimentalDirectedInterleaveDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalDirectedInterleaveDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_group_by_reducer_dataset(input_dataset, key_func_other_arguments, init_func_other_arguments, reduce_func_other_arguments, finalize_func_other_arguments, key_func, init_func, reduce_func, finalize_func, output_types, output_shapes, name=None):
  r"""Creates a dataset that computes a group-by on `input_dataset`.

  Creates a dataset that computes a group-by on `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the input dataset.
    key_func_other_arguments: A list of `Tensor` objects.
      A list of tensors, typically values that were captured when
      building a closure for `key_func`.
    init_func_other_arguments: A list of `Tensor` objects.
      A list of tensors, typically values that were captured when
      building a closure for `init_func`.
    reduce_func_other_arguments: A list of `Tensor` objects.
      A list of tensors, typically values that were captured when
      building a closure for `reduce_func`.
    finalize_func_other_arguments: A list of `Tensor` objects.
      A list of tensors, typically values that were captured when
      building a closure for `finalize_func`.
    key_func: A function decorated with @Defun.
      A function mapping an element of `input_dataset`, concatenated
      with `key_func_other_arguments` to a scalar value of type DT_INT64.
    init_func: A function decorated with @Defun.
      A function mapping a key of type DT_INT64, concatenated with
      `init_func_other_arguments` to the initial reducer state.
    reduce_func: A function decorated with @Defun.
      A function mapping the current reducer state and an element of `input_dataset`,
      concatenated with `reduce_func_other_arguments` to a new reducer state.
    finalize_func: A function decorated with @Defun.
      A function mapping the final reducer state to an output element.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalGroupByReducerDataset", name,
        _ctx._post_execution_callbacks, input_dataset,
        key_func_other_arguments, init_func_other_arguments,
        reduce_func_other_arguments, finalize_func_other_arguments,
        "key_func", key_func, "init_func", init_func, "reduce_func",
        reduce_func, "finalize_func", finalize_func, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_group_by_reducer_dataset_eager_fallback(
            input_dataset, key_func_other_arguments,
            init_func_other_arguments, reduce_func_other_arguments,
            finalize_func_other_arguments, key_func=key_func,
            init_func=init_func, reduce_func=reduce_func,
            finalize_func=finalize_func, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_group_by_reducer_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_group_by_reducer_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalGroupByReducerDataset", input_dataset=input_dataset,
                                             key_func_other_arguments=key_func_other_arguments,
                                             init_func_other_arguments=init_func_other_arguments,
                                             reduce_func_other_arguments=reduce_func_other_arguments,
                                             finalize_func_other_arguments=finalize_func_other_arguments,
                                             key_func=key_func,
                                             init_func=init_func,
                                             reduce_func=reduce_func,
                                             finalize_func=finalize_func,
                                             output_types=output_types,
                                             output_shapes=output_shapes,
                                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("key_func", _op.get_attr("key_func"), "init_func",
            _op.get_attr("init_func"), "reduce_func",
            _op.get_attr("reduce_func"), "finalize_func",
            _op.get_attr("finalize_func"), "Tkey_func_other_arguments",
            _op.get_attr("Tkey_func_other_arguments"),
            "Tinit_func_other_arguments",
            _op.get_attr("Tinit_func_other_arguments"),
            "Treduce_func_other_arguments",
            _op.get_attr("Treduce_func_other_arguments"),
            "Tfinalize_func_other_arguments",
            _op.get_attr("Tfinalize_func_other_arguments"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalGroupByReducerDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_group_by_reducer_dataset_eager_fallback(input_dataset, key_func_other_arguments, init_func_other_arguments, reduce_func_other_arguments, finalize_func_other_arguments, key_func, init_func, reduce_func, finalize_func, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_group_by_reducer_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_group_by_reducer_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_group_by_reducer_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Tkey_func_other_arguments, key_func_other_arguments = _execute.convert_to_mixed_eager_tensors(key_func_other_arguments, _ctx)
  _attr_Tinit_func_other_arguments, init_func_other_arguments = _execute.convert_to_mixed_eager_tensors(init_func_other_arguments, _ctx)
  _attr_Treduce_func_other_arguments, reduce_func_other_arguments = _execute.convert_to_mixed_eager_tensors(reduce_func_other_arguments, _ctx)
  _attr_Tfinalize_func_other_arguments, finalize_func_other_arguments = _execute.convert_to_mixed_eager_tensors(finalize_func_other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset] + list(key_func_other_arguments) + list(init_func_other_arguments) + list(reduce_func_other_arguments) + list(finalize_func_other_arguments)
  _attrs = ("key_func", key_func, "init_func", init_func, "reduce_func",
  reduce_func, "finalize_func", finalize_func, "Tkey_func_other_arguments",
  _attr_Tkey_func_other_arguments, "Tinit_func_other_arguments",
  _attr_Tinit_func_other_arguments, "Treduce_func_other_arguments",
  _attr_Treduce_func_other_arguments, "Tfinalize_func_other_arguments",
  _attr_Tfinalize_func_other_arguments, "output_types", output_types,
  "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalGroupByReducerDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalGroupByReducerDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_group_by_window_dataset(input_dataset, key_func_other_arguments, reduce_func_other_arguments, window_size_func_other_arguments, key_func, reduce_func, window_size_func, output_types, output_shapes, name=None):
  r"""Creates a dataset that computes a windowed group-by on `input_dataset`.

  // TODO(mrry): Support non-int64 keys.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    key_func_other_arguments: A list of `Tensor` objects.
    reduce_func_other_arguments: A list of `Tensor` objects.
    window_size_func_other_arguments: A list of `Tensor` objects.
    key_func: A function decorated with @Defun.
      A function mapping an element of `input_dataset`, concatenated
      with `key_func_other_arguments` to a scalar value of type DT_INT64.
    reduce_func: A function decorated with @Defun.
    window_size_func: A function decorated with @Defun.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalGroupByWindowDataset", name,
        _ctx._post_execution_callbacks, input_dataset,
        key_func_other_arguments, reduce_func_other_arguments,
        window_size_func_other_arguments, "key_func", key_func, "reduce_func",
        reduce_func, "window_size_func", window_size_func, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_group_by_window_dataset_eager_fallback(
            input_dataset, key_func_other_arguments,
            reduce_func_other_arguments, window_size_func_other_arguments,
            key_func=key_func, reduce_func=reduce_func,
            window_size_func=window_size_func, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_group_by_window_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_group_by_window_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalGroupByWindowDataset", input_dataset=input_dataset,
                                            key_func_other_arguments=key_func_other_arguments,
                                            reduce_func_other_arguments=reduce_func_other_arguments,
                                            window_size_func_other_arguments=window_size_func_other_arguments,
                                            key_func=key_func,
                                            reduce_func=reduce_func,
                                            window_size_func=window_size_func,
                                            output_types=output_types,
                                            output_shapes=output_shapes,
                                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("key_func", _op.get_attr("key_func"), "reduce_func",
            _op.get_attr("reduce_func"), "window_size_func",
            _op.get_attr("window_size_func"), "Tkey_func_other_arguments",
            _op.get_attr("Tkey_func_other_arguments"),
            "Treduce_func_other_arguments",
            _op.get_attr("Treduce_func_other_arguments"),
            "Twindow_size_func_other_arguments",
            _op.get_attr("Twindow_size_func_other_arguments"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalGroupByWindowDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_group_by_window_dataset_eager_fallback(input_dataset, key_func_other_arguments, reduce_func_other_arguments, window_size_func_other_arguments, key_func, reduce_func, window_size_func, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_group_by_window_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_group_by_window_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_group_by_window_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Tkey_func_other_arguments, key_func_other_arguments = _execute.convert_to_mixed_eager_tensors(key_func_other_arguments, _ctx)
  _attr_Treduce_func_other_arguments, reduce_func_other_arguments = _execute.convert_to_mixed_eager_tensors(reduce_func_other_arguments, _ctx)
  _attr_Twindow_size_func_other_arguments, window_size_func_other_arguments = _execute.convert_to_mixed_eager_tensors(window_size_func_other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset] + list(key_func_other_arguments) + list(reduce_func_other_arguments) + list(window_size_func_other_arguments)
  _attrs = ("key_func", key_func, "reduce_func", reduce_func,
  "window_size_func", window_size_func, "Tkey_func_other_arguments",
  _attr_Tkey_func_other_arguments, "Treduce_func_other_arguments",
  _attr_Treduce_func_other_arguments, "Twindow_size_func_other_arguments",
  _attr_Twindow_size_func_other_arguments, "output_types", output_types,
  "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalGroupByWindowDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalGroupByWindowDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_identity_indexed_dataset(size, name=None):
  r"""TODO: add doc.

  Args:
    size: A `Tensor` of type `uint64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalIdentityIndexedDataset", name,
        _ctx._post_execution_callbacks, size)
      return _result
    except _core._FallbackException:
      try:
        return experimental_identity_indexed_dataset_eager_fallback(
            size, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalIdentityIndexedDataset", size=size, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ExperimentalIdentityIndexedDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_identity_indexed_dataset_eager_fallback(size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_identity_indexed_dataset
  """
  _ctx = ctx if ctx else _context.context()
  size = _ops.convert_to_tensor(size, _dtypes.uint64)
  _inputs_flat = [size]
  _attrs = None
  _result = _execute.execute(b"ExperimentalIdentityIndexedDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalIdentityIndexedDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_ignore_errors_dataset(input_dataset, output_types, output_shapes, name=None):
  r"""Creates a dataset that contains the elements of `input_dataset` ignoring errors.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalIgnoreErrorsDataset", name,
        _ctx._post_execution_callbacks, input_dataset, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_ignore_errors_dataset_eager_fallback(
            input_dataset, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_ignore_errors_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_ignore_errors_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalIgnoreErrorsDataset", input_dataset=input_dataset,
                                           output_types=output_types,
                                           output_shapes=output_shapes,
                                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalIgnoreErrorsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_ignore_errors_dataset_eager_fallback(input_dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_ignore_errors_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_ignore_errors_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_ignore_errors_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalIgnoreErrorsDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalIgnoreErrorsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_indexed_dataset_get(materialized, index, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    materialized: A `Tensor` of type `resource`.
    index: A `Tensor` of type `uint64`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `output_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalIndexedDatasetGet", name, _ctx._post_execution_callbacks,
        materialized, index, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_indexed_dataset_get_eager_fallback(
            materialized, index, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_indexed_dataset_get' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_indexed_dataset_get' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalIndexedDatasetGet", materialized=materialized,
                                         index=index,
                                         output_types=output_types,
                                         output_shapes=output_shapes,
                                         name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalIndexedDatasetGet", _inputs_flat, _attrs, _result, name)
  return _result



def experimental_indexed_dataset_get_eager_fallback(materialized, index, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_indexed_dataset_get
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_indexed_dataset_get' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_indexed_dataset_get' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  materialized = _ops.convert_to_tensor(materialized, _dtypes.resource)
  index = _ops.convert_to_tensor(index, _dtypes.uint64)
  _inputs_flat = [materialized, index]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalIndexedDatasetGet",
                             len(output_types), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ExperimentalIndexedDatasetGet", _inputs_flat, _attrs, _result, name)
  return _result


def experimental_indexed_dataset_materialize(dataset, materialized, name=None):
  r"""TODO: add doc.

  Args:
    dataset: A `Tensor` of type `variant`.
    materialized: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalIndexedDatasetMaterialize", name,
        _ctx._post_execution_callbacks, dataset, materialized)
      return _result
    except _core._FallbackException:
      try:
        return experimental_indexed_dataset_materialize_eager_fallback(
            dataset, materialized, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalIndexedDatasetMaterialize", dataset=dataset,
                                                 materialized=materialized,
                                                 name=name)
  return _op
  _result = None
  return _result



def experimental_indexed_dataset_materialize_eager_fallback(dataset, materialized, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_indexed_dataset_materialize
  """
  _ctx = ctx if ctx else _context.context()
  dataset = _ops.convert_to_tensor(dataset, _dtypes.variant)
  materialized = _ops.convert_to_tensor(materialized, _dtypes.resource)
  _inputs_flat = [dataset, materialized]
  _attrs = None
  _result = _execute.execute(b"ExperimentalIndexedDatasetMaterialize", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def experimental_iterator_get_device(resource, name=None):
  r"""Returns the name of the device on which `resource` has been placed.

  Args:
    resource: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalIteratorGetDevice", name, _ctx._post_execution_callbacks,
        resource)
      return _result
    except _core._FallbackException:
      try:
        return experimental_iterator_get_device_eager_fallback(
            resource, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalIteratorGetDevice", resource=resource, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ExperimentalIteratorGetDevice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_iterator_get_device_eager_fallback(resource, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_iterator_get_device
  """
  _ctx = ctx if ctx else _context.context()
  resource = _ops.convert_to_tensor(resource, _dtypes.resource)
  _inputs_flat = [resource]
  _attrs = None
  _result = _execute.execute(b"ExperimentalIteratorGetDevice", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalIteratorGetDevice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_lmdb_dataset(filenames, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    filenames: A `Tensor` of type `string`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalLMDBDataset", name, _ctx._post_execution_callbacks,
        filenames, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_lmdb_dataset_eager_fallback(
            filenames, output_types=output_types, output_shapes=output_shapes,
            name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_lmdb_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_lmdb_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalLMDBDataset", filenames=filenames,
                                   output_types=output_types,
                                   output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalLMDBDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_lmdb_dataset_eager_fallback(filenames, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_lmdb_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_lmdb_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_lmdb_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  filenames = _ops.convert_to_tensor(filenames, _dtypes.string)
  _inputs_flat = [filenames]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalLMDBDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalLMDBDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_latency_stats_dataset(input_dataset, tag, output_types, output_shapes, name=None):
  r"""Records the latency of producing `input_dataset` elements in a StatsAggregator.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    tag: A `Tensor` of type `string`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalLatencyStatsDataset", name,
        _ctx._post_execution_callbacks, input_dataset, tag, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_latency_stats_dataset_eager_fallback(
            input_dataset, tag, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_latency_stats_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_latency_stats_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalLatencyStatsDataset", input_dataset=input_dataset,
                                           tag=tag, output_types=output_types,
                                           output_shapes=output_shapes,
                                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalLatencyStatsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_latency_stats_dataset_eager_fallback(input_dataset, tag, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_latency_stats_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_latency_stats_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_latency_stats_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  _inputs_flat = [input_dataset, tag]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalLatencyStatsDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalLatencyStatsDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_map_and_batch_dataset(input_dataset, other_arguments, batch_size, num_parallel_calls, drop_remainder, f, output_types, output_shapes, preserve_cardinality=False, name=None):
  r"""Creates a dataset that fuses mapping with batching.

  Creates a dataset that applies `f` to the outputs of `input_dataset` and then
  batches `batch_size` of them.

  Unlike a "MapDataset", which applies `f` sequentially, this dataset invokes up
  to `batch_size * num_parallel_batches` copies of `f` in parallel.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the input dataset.
    other_arguments: A list of `Tensor` objects.
      A list of tensors, typically values that were captured when building a closure
      for `f`.
    batch_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a
      batch. It determines the number of concurrent invocations of `f` that process
      elements from `input_dataset` in parallel.
    num_parallel_calls: A `Tensor` of type `int64`.
      A scalar representing the maximum number of parallel invocations of the `map_fn`
      function. Applying the `map_fn` on consecutive input elements in parallel has
      the potential to improve input pipeline throughput.
    drop_remainder: A `Tensor` of type `bool`.
      A scalar representing whether the last batch should be dropped in case its size
      is smaller than desired.
    f: A function decorated with @Defun.
      A function to apply to the outputs of `input_dataset`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    preserve_cardinality: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalMapAndBatchDataset", name,
        _ctx._post_execution_callbacks, input_dataset, other_arguments,
        batch_size, num_parallel_calls, drop_remainder, "f", f,
        "output_types", output_types, "output_shapes", output_shapes,
        "preserve_cardinality", preserve_cardinality)
      return _result
    except _core._FallbackException:
      try:
        return experimental_map_and_batch_dataset_eager_fallback(
            input_dataset, other_arguments, batch_size, num_parallel_calls,
            drop_remainder, f=f, output_types=output_types,
            output_shapes=output_shapes,
            preserve_cardinality=preserve_cardinality, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_map_and_batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_map_and_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalMapAndBatchDataset", input_dataset=input_dataset,
                                          other_arguments=other_arguments,
                                          batch_size=batch_size,
                                          num_parallel_calls=num_parallel_calls,
                                          drop_remainder=drop_remainder, f=f,
                                          output_types=output_types,
                                          output_shapes=output_shapes,
                                          preserve_cardinality=preserve_cardinality,
                                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "preserve_cardinality",
            _op.get_attr("preserve_cardinality"))
  _execute.record_gradient(
      "ExperimentalMapAndBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_map_and_batch_dataset_eager_fallback(input_dataset, other_arguments, batch_size, num_parallel_calls, drop_remainder, f, output_types, output_shapes, preserve_cardinality=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_map_and_batch_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_map_and_batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_map_and_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  batch_size = _ops.convert_to_tensor(batch_size, _dtypes.int64)
  num_parallel_calls = _ops.convert_to_tensor(num_parallel_calls, _dtypes.int64)
  drop_remainder = _ops.convert_to_tensor(drop_remainder, _dtypes.bool)
  _inputs_flat = [input_dataset] + list(other_arguments) + [batch_size, num_parallel_calls, drop_remainder]
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes, "preserve_cardinality",
  preserve_cardinality)
  _result = _execute.execute(b"ExperimentalMapAndBatchDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalMapAndBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_map_dataset(input_dataset, other_arguments, f, output_types, output_shapes, use_inter_op_parallelism=True, preserve_cardinality=False, name=None):
  r"""Creates a dataset that applies `f` to the outputs of `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    other_arguments: A list of `Tensor` objects.
    f: A function decorated with @Defun.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    use_inter_op_parallelism: An optional `bool`. Defaults to `True`.
    preserve_cardinality: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalMapDataset", name, _ctx._post_execution_callbacks,
        input_dataset, other_arguments, "f", f, "output_types", output_types,
        "output_shapes", output_shapes, "use_inter_op_parallelism",
        use_inter_op_parallelism, "preserve_cardinality",
        preserve_cardinality)
      return _result
    except _core._FallbackException:
      try:
        return experimental_map_dataset_eager_fallback(
            input_dataset, other_arguments, f=f, output_types=output_types,
            output_shapes=output_shapes,
            use_inter_op_parallelism=use_inter_op_parallelism,
            preserve_cardinality=preserve_cardinality, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_map_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if use_inter_op_parallelism is None:
    use_inter_op_parallelism = True
  use_inter_op_parallelism = _execute.make_bool(use_inter_op_parallelism, "use_inter_op_parallelism")
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalMapDataset", input_dataset=input_dataset,
                                  other_arguments=other_arguments, f=f,
                                  output_types=output_types,
                                  output_shapes=output_shapes,
                                  use_inter_op_parallelism=use_inter_op_parallelism,
                                  preserve_cardinality=preserve_cardinality,
                                  name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "use_inter_op_parallelism",
            _op.get_attr("use_inter_op_parallelism"), "preserve_cardinality",
            _op.get_attr("preserve_cardinality"))
  _execute.record_gradient(
      "ExperimentalMapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_map_dataset_eager_fallback(input_dataset, other_arguments, f, output_types, output_shapes, use_inter_op_parallelism=True, preserve_cardinality=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_map_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_map_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if use_inter_op_parallelism is None:
    use_inter_op_parallelism = True
  use_inter_op_parallelism = _execute.make_bool(use_inter_op_parallelism, "use_inter_op_parallelism")
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset] + list(other_arguments)
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes, "use_inter_op_parallelism",
  use_inter_op_parallelism, "preserve_cardinality", preserve_cardinality)
  _result = _execute.execute(b"ExperimentalMapDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalMapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_matching_files_dataset(patterns, name=None):
  r"""TODO: add doc.

  Args:
    patterns: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalMatchingFilesDataset", name,
        _ctx._post_execution_callbacks, patterns)
      return _result
    except _core._FallbackException:
      try:
        return experimental_matching_files_dataset_eager_fallback(
            patterns, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalMatchingFilesDataset", patterns=patterns, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ExperimentalMatchingFilesDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_matching_files_dataset_eager_fallback(patterns, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_matching_files_dataset
  """
  _ctx = ctx if ctx else _context.context()
  patterns = _ops.convert_to_tensor(patterns, _dtypes.string)
  _inputs_flat = [patterns]
  _attrs = None
  _result = _execute.execute(b"ExperimentalMatchingFilesDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalMatchingFilesDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_materialized_index_dataset_handle(container, shared_name, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    container: A `string`.
    shared_name: A `string`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalMaterializedIndexDatasetHandle", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_materialized_index_dataset_handle_eager_fallback(
            container=container, shared_name=shared_name,
            output_types=output_types, output_shapes=output_shapes, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  container = _execute.make_str(container, "container")
  shared_name = _execute.make_str(shared_name, "shared_name")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_materialized_index_dataset_handle' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_materialized_index_dataset_handle' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalMaterializedIndexDatasetHandle", container=container,
                                                      shared_name=shared_name,
                                                      output_types=output_types,
                                                      output_shapes=output_shapes,
                                                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalMaterializedIndexDatasetHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_materialized_index_dataset_handle_eager_fallback(container, shared_name, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_materialized_index_dataset_handle
  """
  _ctx = ctx if ctx else _context.context()
  container = _execute.make_str(container, "container")
  shared_name = _execute.make_str(shared_name, "shared_name")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_materialized_index_dataset_handle' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_materialized_index_dataset_handle' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name,
  "output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalMaterializedIndexDatasetHandle", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalMaterializedIndexDatasetHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_max_intra_op_parallelism_dataset(input_dataset, max_intra_op_parallelism, output_types, output_shapes, name=None):
  r"""Creates a dataset that overrides the maximum intra-op parallelism.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    max_intra_op_parallelism: A `Tensor` of type `int64`.
      Identifies the maximum intra-op parallelism to use.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalMaxIntraOpParallelismDataset", name,
        _ctx._post_execution_callbacks, input_dataset,
        max_intra_op_parallelism, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_max_intra_op_parallelism_dataset_eager_fallback(
            input_dataset, max_intra_op_parallelism,
            output_types=output_types, output_shapes=output_shapes, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_max_intra_op_parallelism_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_max_intra_op_parallelism_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalMaxIntraOpParallelismDataset", input_dataset=input_dataset,
                                                    max_intra_op_parallelism=max_intra_op_parallelism,
                                                    output_types=output_types,
                                                    output_shapes=output_shapes,
                                                    name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalMaxIntraOpParallelismDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_max_intra_op_parallelism_dataset_eager_fallback(input_dataset, max_intra_op_parallelism, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_max_intra_op_parallelism_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_max_intra_op_parallelism_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_max_intra_op_parallelism_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  max_intra_op_parallelism = _ops.convert_to_tensor(max_intra_op_parallelism, _dtypes.int64)
  _inputs_flat = [input_dataset, max_intra_op_parallelism]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalMaxIntraOpParallelismDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalMaxIntraOpParallelismDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_non_serializable_dataset(input_dataset, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalNonSerializableDataset", name,
        _ctx._post_execution_callbacks, input_dataset, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_non_serializable_dataset_eager_fallback(
            input_dataset, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_non_serializable_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_non_serializable_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalNonSerializableDataset", input_dataset=input_dataset,
                                              output_types=output_types,
                                              output_shapes=output_shapes,
                                              name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalNonSerializableDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_non_serializable_dataset_eager_fallback(input_dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_non_serializable_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_non_serializable_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_non_serializable_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalNonSerializableDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalNonSerializableDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_numa_map_and_batch_dataset(input_dataset, other_arguments, batch_size, num_parallel_calls, drop_remainder, f, output_types, output_shapes, preserve_cardinality=False, name=None):
  r"""Creates a dataset that fuses mapping with batching.

  Creates a dataset that applies `f` to the outputs of `input_dataset` and then
  batches `batch_size` of them.

  Unlike a "MapDataset", which applies `f` sequentially, this dataset invokes up
  to `batch_size * num_parallel_batches` copies of `f` in parallel.

  Unlike "MapAndBatchDatasetV2", this dataset uses a NUMA-aware thread scheduling
  policy. Because it uses the single-threaded executor, it only supports the
  function-based control flow ops.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the input dataset.
    other_arguments: A list of `Tensor` objects.
      A list of tensors, typically values that were captured when building a closure
      for `f`.
    batch_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a
      batch. It determines the number of concurrent invocations of `f` that process
      elements from `input_dataset` in parallel.
    num_parallel_calls: A `Tensor` of type `int64`.
      A scalar representing the maximum number of parallel invocations of the `map_fn`
      function. Applying the `map_fn` on consecutive input elements in parallel has
      the potential to improve input pipeline throughput.
    drop_remainder: A `Tensor` of type `bool`.
      A scalar representing whether the last batch should be dropped in case its size
      is smaller than desired.
    f: A function decorated with @Defun.
      A function to apply to the outputs of `input_dataset`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    preserve_cardinality: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalNumaMapAndBatchDataset", name,
        _ctx._post_execution_callbacks, input_dataset, other_arguments,
        batch_size, num_parallel_calls, drop_remainder, "f", f,
        "output_types", output_types, "output_shapes", output_shapes,
        "preserve_cardinality", preserve_cardinality)
      return _result
    except _core._FallbackException:
      try:
        return experimental_numa_map_and_batch_dataset_eager_fallback(
            input_dataset, other_arguments, batch_size, num_parallel_calls,
            drop_remainder, f=f, output_types=output_types,
            output_shapes=output_shapes,
            preserve_cardinality=preserve_cardinality, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_numa_map_and_batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_numa_map_and_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalNumaMapAndBatchDataset", input_dataset=input_dataset,
                                              other_arguments=other_arguments,
                                              batch_size=batch_size,
                                              num_parallel_calls=num_parallel_calls,
                                              drop_remainder=drop_remainder,
                                              f=f, output_types=output_types,
                                              output_shapes=output_shapes,
                                              preserve_cardinality=preserve_cardinality,
                                              name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "preserve_cardinality",
            _op.get_attr("preserve_cardinality"))
  _execute.record_gradient(
      "ExperimentalNumaMapAndBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_numa_map_and_batch_dataset_eager_fallback(input_dataset, other_arguments, batch_size, num_parallel_calls, drop_remainder, f, output_types, output_shapes, preserve_cardinality=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_numa_map_and_batch_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_numa_map_and_batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_numa_map_and_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  batch_size = _ops.convert_to_tensor(batch_size, _dtypes.int64)
  num_parallel_calls = _ops.convert_to_tensor(num_parallel_calls, _dtypes.int64)
  drop_remainder = _ops.convert_to_tensor(drop_remainder, _dtypes.bool)
  _inputs_flat = [input_dataset] + list(other_arguments) + [batch_size, num_parallel_calls, drop_remainder]
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes, "preserve_cardinality",
  preserve_cardinality)
  _result = _execute.execute(b"ExperimentalNumaMapAndBatchDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalNumaMapAndBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_parallel_interleave_dataset(input_dataset, other_arguments, cycle_length, block_length, sloppy, buffer_output_elements, prefetch_input_elements, f, output_types, output_shapes, name=None):
  r"""Creates a dataset that applies `f` to the outputs of `input_dataset`.

  The resulting dataset is similar to the `InterleaveDataset`, with the exception
  that if retrieving the next value from a dataset would cause the requester to
  block, it will skip that input dataset. This dataset is especially useful
  when loading data from a variable-latency datastores (e.g. HDFS, GCS), as it
  allows the training step to proceed so long as some data is available.

  !! WARNING !! This dataset is not deterministic!

  Args:
    input_dataset: A `Tensor` of type `variant`.
    other_arguments: A list of `Tensor` objects.
    cycle_length: A `Tensor` of type `int64`.
    block_length: A `Tensor` of type `int64`.
    sloppy: A `Tensor` of type `bool`.
    buffer_output_elements: A `Tensor` of type `int64`.
    prefetch_input_elements: A `Tensor` of type `int64`.
    f: A function decorated with @Defun.
      A function mapping elements of `input_dataset`, concatenated with
      `other_arguments`, to a Dataset variant that contains elements matching
      `output_types` and `output_shapes`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalParallelInterleaveDataset", name,
        _ctx._post_execution_callbacks, input_dataset, other_arguments,
        cycle_length, block_length, sloppy, buffer_output_elements,
        prefetch_input_elements, "f", f, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_parallel_interleave_dataset_eager_fallback(
            input_dataset, other_arguments, cycle_length, block_length,
            sloppy, buffer_output_elements, prefetch_input_elements, f=f,
            output_types=output_types, output_shapes=output_shapes, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_parallel_interleave_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_parallel_interleave_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalParallelInterleaveDataset", input_dataset=input_dataset,
                                                 other_arguments=other_arguments,
                                                 cycle_length=cycle_length,
                                                 block_length=block_length,
                                                 sloppy=sloppy,
                                                 buffer_output_elements=buffer_output_elements,
                                                 prefetch_input_elements=prefetch_input_elements,
                                                 f=f,
                                                 output_types=output_types,
                                                 output_shapes=output_shapes,
                                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalParallelInterleaveDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_parallel_interleave_dataset_eager_fallback(input_dataset, other_arguments, cycle_length, block_length, sloppy, buffer_output_elements, prefetch_input_elements, f, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_parallel_interleave_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_parallel_interleave_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_parallel_interleave_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  cycle_length = _ops.convert_to_tensor(cycle_length, _dtypes.int64)
  block_length = _ops.convert_to_tensor(block_length, _dtypes.int64)
  sloppy = _ops.convert_to_tensor(sloppy, _dtypes.bool)
  buffer_output_elements = _ops.convert_to_tensor(buffer_output_elements, _dtypes.int64)
  prefetch_input_elements = _ops.convert_to_tensor(prefetch_input_elements, _dtypes.int64)
  _inputs_flat = [input_dataset] + list(other_arguments) + [cycle_length, block_length, sloppy, buffer_output_elements, prefetch_input_elements]
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalParallelInterleaveDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalParallelInterleaveDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_parse_example_dataset(input_dataset, num_parallel_calls, dense_defaults, sparse_keys, dense_keys, sparse_types, dense_shapes, output_types, output_shapes, sloppy=False, name=None):
  r"""Transforms `input_dataset` containing `Example` protos as vectors of DT_STRING into a dataset of `Tensor` or `SparseTensor` objects representing the parsed features.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    num_parallel_calls: A `Tensor` of type `int64`.
    dense_defaults: A list of `Tensor` objects with types from: `float32`, `int64`, `string`.
      A dict mapping string keys to `Tensor`s.
      The keys of the dict must match the dense_keys of the feature.
    sparse_keys: A list of `strings`.
      A list of string keys in the examples features.
      The results for these keys will be returned as `SparseTensor` objects.
    dense_keys: A list of `strings`.
      A list of Ndense string Tensors (scalars).
      The keys expected in the Examples features associated with dense values.
    sparse_types: A list of `tf.DTypes` from: `tf.float32, tf.int64, tf.string`.
      A list of `DTypes` of the same length as `sparse_keys`.
      Only `tf.float32` (`FloatList`), `tf.int64` (`Int64List`),
      and `tf.string` (`BytesList`) are supported.
    dense_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`).
      List of tuples with the same length as `dense_keys`.
      The shape of the data for each dense feature referenced by `dense_keys`.
      Required for any input tensors identified by `dense_keys`.  Must be
      either fully defined, or may contain an unknown first dimension.
      An unknown first dimension means the feature is treated as having
      a variable number of blocks, and the output shape along this dimension
      is considered unknown at graph build time.  Padding is applied for
      minibatch elements smaller than the maximum number of blocks for the
      given feature along this dimension.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
      The type list for the return values.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
      The list of shapes being produced.
    sloppy: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalParseExampleDataset", name,
        _ctx._post_execution_callbacks, input_dataset, num_parallel_calls,
        dense_defaults, "sparse_keys", sparse_keys, "dense_keys", dense_keys,
        "sparse_types", sparse_types, "dense_shapes", dense_shapes,
        "output_types", output_types, "output_shapes", output_shapes,
        "sloppy", sloppy)
      return _result
    except _core._FallbackException:
      try:
        return experimental_parse_example_dataset_eager_fallback(
            input_dataset, num_parallel_calls, dense_defaults,
            sparse_keys=sparse_keys, dense_keys=dense_keys,
            sparse_types=sparse_types, dense_shapes=dense_shapes,
            output_types=output_types, output_shapes=output_shapes,
            sloppy=sloppy, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(sparse_keys, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_keys' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % sparse_keys)
  sparse_keys = [_execute.make_str(_s, "sparse_keys") for _s in sparse_keys]
  if not isinstance(dense_keys, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_keys' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % dense_keys)
  dense_keys = [_execute.make_str(_s, "dense_keys") for _s in dense_keys]
  if not isinstance(sparse_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_types' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % sparse_types)
  sparse_types = [_execute.make_type(_t, "sparse_types") for _t in sparse_types]
  if not isinstance(dense_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_shapes' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % dense_shapes)
  dense_shapes = [_execute.make_shape(_s, "dense_shapes") for _s in dense_shapes]
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if sloppy is None:
    sloppy = False
  sloppy = _execute.make_bool(sloppy, "sloppy")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalParseExampleDataset", input_dataset=input_dataset,
                                           num_parallel_calls=num_parallel_calls,
                                           dense_defaults=dense_defaults,
                                           sparse_keys=sparse_keys,
                                           dense_keys=dense_keys,
                                           sparse_types=sparse_types,
                                           dense_shapes=dense_shapes,
                                           output_types=output_types,
                                           output_shapes=output_shapes,
                                           sloppy=sloppy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("sparse_keys", _op.get_attr("sparse_keys"), "dense_keys",
            _op.get_attr("dense_keys"), "sparse_types",
            _op.get_attr("sparse_types"), "Tdense", _op.get_attr("Tdense"),
            "dense_shapes", _op.get_attr("dense_shapes"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "sloppy", _op.get_attr("sloppy"))
  _execute.record_gradient(
      "ExperimentalParseExampleDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_parse_example_dataset_eager_fallback(input_dataset, num_parallel_calls, dense_defaults, sparse_keys, dense_keys, sparse_types, dense_shapes, output_types, output_shapes, sloppy=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_parse_example_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(sparse_keys, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_keys' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % sparse_keys)
  sparse_keys = [_execute.make_str(_s, "sparse_keys") for _s in sparse_keys]
  if not isinstance(dense_keys, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_keys' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % dense_keys)
  dense_keys = [_execute.make_str(_s, "dense_keys") for _s in dense_keys]
  if not isinstance(sparse_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'sparse_types' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % sparse_types)
  sparse_types = [_execute.make_type(_t, "sparse_types") for _t in sparse_types]
  if not isinstance(dense_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dense_shapes' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % dense_shapes)
  dense_shapes = [_execute.make_shape(_s, "dense_shapes") for _s in dense_shapes]
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_parse_example_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if sloppy is None:
    sloppy = False
  sloppy = _execute.make_bool(sloppy, "sloppy")
  _attr_Tdense, dense_defaults = _execute.convert_to_mixed_eager_tensors(dense_defaults, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  num_parallel_calls = _ops.convert_to_tensor(num_parallel_calls, _dtypes.int64)
  _inputs_flat = [input_dataset, num_parallel_calls] + list(dense_defaults)
  _attrs = ("sparse_keys", sparse_keys, "dense_keys", dense_keys,
  "sparse_types", sparse_types, "Tdense", _attr_Tdense, "dense_shapes",
  dense_shapes, "output_types", output_types, "output_shapes", output_shapes,
  "sloppy", sloppy)
  _result = _execute.execute(b"ExperimentalParseExampleDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalParseExampleDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_private_thread_pool_dataset(input_dataset, num_threads, output_types, output_shapes, name=None):
  r"""Creates a dataset that uses a custom thread pool to compute `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    num_threads: A `Tensor` of type `int64`.
      Identifies the number of threads to use for the private threadpool.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalPrivateThreadPoolDataset", name,
        _ctx._post_execution_callbacks, input_dataset, num_threads,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_private_thread_pool_dataset_eager_fallback(
            input_dataset, num_threads, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_private_thread_pool_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_private_thread_pool_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalPrivateThreadPoolDataset", input_dataset=input_dataset,
                                                num_threads=num_threads,
                                                output_types=output_types,
                                                output_shapes=output_shapes,
                                                name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalPrivateThreadPoolDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_private_thread_pool_dataset_eager_fallback(input_dataset, num_threads, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_private_thread_pool_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_private_thread_pool_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_private_thread_pool_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  num_threads = _ops.convert_to_tensor(num_threads, _dtypes.int64)
  _inputs_flat = [input_dataset, num_threads]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalPrivateThreadPoolDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalPrivateThreadPoolDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_random_dataset(seed, seed2, output_types, output_shapes, name=None):
  r"""Creates a Dataset that returns pseudorandom numbers.

  Args:
    seed: A `Tensor` of type `int64`.
      A scalar seed for the random number generator. If either seed or
      seed2 is set to be non-zero, the random number generator is seeded
      by the given seed.  Otherwise, a random seed is used.
    seed2: A `Tensor` of type `int64`.
      A second scalar seed to avoid seed collision.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalRandomDataset", name, _ctx._post_execution_callbacks,
        seed, seed2, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_random_dataset_eager_fallback(
            seed, seed2, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_random_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_random_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalRandomDataset", seed=seed, seed2=seed2,
                                     output_types=output_types,
                                     output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalRandomDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_random_dataset_eager_fallback(seed, seed2, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_random_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_random_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_random_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  seed = _ops.convert_to_tensor(seed, _dtypes.int64)
  seed2 = _ops.convert_to_tensor(seed2, _dtypes.int64)
  _inputs_flat = [seed, seed2]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalRandomDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalRandomDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_scan_dataset(input_dataset, initial_state, other_arguments, f, output_types, output_shapes, preserve_cardinality=False, name=None):
  r"""Creates a dataset successively reduces `f` over the elements of `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    initial_state: A list of `Tensor` objects.
    other_arguments: A list of `Tensor` objects.
    f: A function decorated with @Defun.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    preserve_cardinality: An optional `bool`. Defaults to `False`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalScanDataset", name, _ctx._post_execution_callbacks,
        input_dataset, initial_state, other_arguments, "f", f, "output_types",
        output_types, "output_shapes", output_shapes, "preserve_cardinality",
        preserve_cardinality)
      return _result
    except _core._FallbackException:
      try:
        return experimental_scan_dataset_eager_fallback(
            input_dataset, initial_state, other_arguments, f=f,
            output_types=output_types, output_shapes=output_shapes,
            preserve_cardinality=preserve_cardinality, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_scan_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_scan_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalScanDataset", input_dataset=input_dataset,
                                   initial_state=initial_state,
                                   other_arguments=other_arguments, f=f,
                                   output_types=output_types,
                                   output_shapes=output_shapes,
                                   preserve_cardinality=preserve_cardinality,
                                   name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Tstate", _op.get_attr("Tstate"),
            "Targuments", _op.get_attr("Targuments"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "preserve_cardinality",
            _op.get_attr("preserve_cardinality"))
  _execute.record_gradient(
      "ExperimentalScanDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_scan_dataset_eager_fallback(input_dataset, initial_state, other_arguments, f, output_types, output_shapes, preserve_cardinality=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_scan_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_scan_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_scan_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _attr_Tstate, initial_state = _execute.convert_to_mixed_eager_tensors(initial_state, _ctx)
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset] + list(initial_state) + list(other_arguments)
  _attrs = ("f", f, "Tstate", _attr_Tstate, "Targuments", _attr_Targuments,
  "output_types", output_types, "output_shapes", output_shapes,
  "preserve_cardinality", preserve_cardinality)
  _result = _execute.execute(b"ExperimentalScanDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalScanDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_set_stats_aggregator_dataset(input_dataset, stats_aggregator, tag, counter_prefix, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    stats_aggregator: A `Tensor` of type `resource`.
    tag: A `Tensor` of type `string`.
    counter_prefix: A `Tensor` of type `string`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalSetStatsAggregatorDataset", name,
        _ctx._post_execution_callbacks, input_dataset, stats_aggregator, tag,
        counter_prefix, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_set_stats_aggregator_dataset_eager_fallback(
            input_dataset, stats_aggregator, tag, counter_prefix,
            output_types=output_types, output_shapes=output_shapes, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_set_stats_aggregator_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_set_stats_aggregator_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalSetStatsAggregatorDataset", input_dataset=input_dataset,
                                                 stats_aggregator=stats_aggregator,
                                                 tag=tag,
                                                 counter_prefix=counter_prefix,
                                                 output_types=output_types,
                                                 output_shapes=output_shapes,
                                                 name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalSetStatsAggregatorDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_set_stats_aggregator_dataset_eager_fallback(input_dataset, stats_aggregator, tag, counter_prefix, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_set_stats_aggregator_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_set_stats_aggregator_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_set_stats_aggregator_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  stats_aggregator = _ops.convert_to_tensor(stats_aggregator, _dtypes.resource)
  tag = _ops.convert_to_tensor(tag, _dtypes.string)
  counter_prefix = _ops.convert_to_tensor(counter_prefix, _dtypes.string)
  _inputs_flat = [input_dataset, stats_aggregator, tag, counter_prefix]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalSetStatsAggregatorDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalSetStatsAggregatorDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_sleep_dataset(input_dataset, sleep_microseconds, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    sleep_microseconds: A `Tensor` of type `int64`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalSleepDataset", name, _ctx._post_execution_callbacks,
        input_dataset, sleep_microseconds, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_sleep_dataset_eager_fallback(
            input_dataset, sleep_microseconds, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_sleep_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_sleep_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalSleepDataset", input_dataset=input_dataset,
                                    sleep_microseconds=sleep_microseconds,
                                    output_types=output_types,
                                    output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalSleepDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_sleep_dataset_eager_fallback(input_dataset, sleep_microseconds, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_sleep_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_sleep_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_sleep_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  sleep_microseconds = _ops.convert_to_tensor(sleep_microseconds, _dtypes.int64)
  _inputs_flat = [input_dataset, sleep_microseconds]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalSleepDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalSleepDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_sliding_window_dataset(input_dataset, window_size, window_shift, window_stride, output_types, output_shapes, name=None):
  r"""Creates a dataset that passes a sliding window over `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    window_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements in the
      sliding window.
    window_shift: A `Tensor` of type `int64`.
      A scalar representing the steps moving the sliding window
      forward in one iteration. It must be positive.
    window_stride: A `Tensor` of type `int64`.
      A scalar representing the stride of the input elements of the sliding window.
      It must be positive.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalSlidingWindowDataset", name,
        _ctx._post_execution_callbacks, input_dataset, window_size,
        window_shift, window_stride, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_sliding_window_dataset_eager_fallback(
            input_dataset, window_size, window_shift, window_stride,
            output_types=output_types, output_shapes=output_shapes, name=name,
            ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_sliding_window_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_sliding_window_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalSlidingWindowDataset", input_dataset=input_dataset,
                                            window_size=window_size,
                                            window_shift=window_shift,
                                            window_stride=window_stride,
                                            output_types=output_types,
                                            output_shapes=output_shapes,
                                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalSlidingWindowDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_sliding_window_dataset_eager_fallback(input_dataset, window_size, window_shift, window_stride, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_sliding_window_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_sliding_window_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_sliding_window_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  window_size = _ops.convert_to_tensor(window_size, _dtypes.int64)
  window_shift = _ops.convert_to_tensor(window_shift, _dtypes.int64)
  window_stride = _ops.convert_to_tensor(window_stride, _dtypes.int64)
  _inputs_flat = [input_dataset, window_size, window_shift, window_stride]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalSlidingWindowDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalSlidingWindowDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_sql_dataset(driver_name, data_source_name, query, output_types, output_shapes, name=None):
  r"""Creates a dataset that executes a SQL query and emits rows of the result set.

  Args:
    driver_name: A `Tensor` of type `string`.
      The database type. Currently, the only supported type is 'sqlite'.
    data_source_name: A `Tensor` of type `string`.
      A connection string to connect to the database.
    query: A `Tensor` of type `string`. A SQL query to execute.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalSqlDataset", name, _ctx._post_execution_callbacks,
        driver_name, data_source_name, query, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_sql_dataset_eager_fallback(
            driver_name, data_source_name, query, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_sql_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_sql_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalSqlDataset", driver_name=driver_name,
                                  data_source_name=data_source_name,
                                  query=query, output_types=output_types,
                                  output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalSqlDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_sql_dataset_eager_fallback(driver_name, data_source_name, query, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_sql_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_sql_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_sql_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  driver_name = _ops.convert_to_tensor(driver_name, _dtypes.string)
  data_source_name = _ops.convert_to_tensor(data_source_name, _dtypes.string)
  query = _ops.convert_to_tensor(query, _dtypes.string)
  _inputs_flat = [driver_name, data_source_name, query]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalSqlDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalSqlDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_stats_aggregator_handle(container="", shared_name="", name=None):
  r"""Creates a statistics manager resource.

  Args:
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalStatsAggregatorHandle", name,
        _ctx._post_execution_callbacks, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return experimental_stats_aggregator_handle_eager_fallback(
            container=container, shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalStatsAggregatorHandle", container=container,
                                             shared_name=shared_name,
                                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "ExperimentalStatsAggregatorHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_stats_aggregator_handle_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_stats_aggregator_handle
  """
  _ctx = ctx if ctx else _context.context()
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name)
  _result = _execute.execute(b"ExperimentalStatsAggregatorHandle", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalStatsAggregatorHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_stats_aggregator_summary(iterator, name=None):
  r"""Produces a summary of any statistics recorded by the given statistics manager.

  Args:
    iterator: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalStatsAggregatorSummary", name,
        _ctx._post_execution_callbacks, iterator)
      return _result
    except _core._FallbackException:
      try:
        return experimental_stats_aggregator_summary_eager_fallback(
            iterator, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalStatsAggregatorSummary", iterator=iterator, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ExperimentalStatsAggregatorSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_stats_aggregator_summary_eager_fallback(iterator, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_stats_aggregator_summary
  """
  _ctx = ctx if ctx else _context.context()
  iterator = _ops.convert_to_tensor(iterator, _dtypes.resource)
  _inputs_flat = [iterator]
  _attrs = None
  _result = _execute.execute(b"ExperimentalStatsAggregatorSummary", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalStatsAggregatorSummary", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_thread_pool_dataset(input_dataset, thread_pool, output_types, output_shapes, name=None):
  r"""Creates a dataset that uses a custom thread pool to compute `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    thread_pool: A `Tensor` of type `resource`.
      A resource produced by the ThreadPoolHandle op.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalThreadPoolDataset", name, _ctx._post_execution_callbacks,
        input_dataset, thread_pool, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_thread_pool_dataset_eager_fallback(
            input_dataset, thread_pool, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_thread_pool_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_thread_pool_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalThreadPoolDataset", input_dataset=input_dataset,
                                         thread_pool=thread_pool,
                                         output_types=output_types,
                                         output_shapes=output_shapes,
                                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalThreadPoolDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_thread_pool_dataset_eager_fallback(input_dataset, thread_pool, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_thread_pool_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_thread_pool_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_thread_pool_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  thread_pool = _ops.convert_to_tensor(thread_pool, _dtypes.resource)
  _inputs_flat = [input_dataset, thread_pool]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalThreadPoolDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalThreadPoolDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_thread_pool_handle(num_threads, display_name, max_intra_op_parallelism=1, container="", shared_name="", name=None):
  r"""Creates a dataset that uses a custom thread pool to compute `input_dataset`.

  Args:
    num_threads: An `int`. The number of threads in the thread pool.
    display_name: A `string`.
      A human-readable name for the threads that may be visible in some
      visualizations.
      threadpool.
    max_intra_op_parallelism: An optional `int`. Defaults to `1`.
      The maximum degree of parallelism to use within operations that execute on this
      threadpool.
    container: An optional `string`. Defaults to `""`.
    shared_name: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalThreadPoolHandle", name, _ctx._post_execution_callbacks,
        "num_threads", num_threads, "max_intra_op_parallelism",
        max_intra_op_parallelism, "display_name", display_name, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return experimental_thread_pool_handle_eager_fallback(
            num_threads=num_threads,
            max_intra_op_parallelism=max_intra_op_parallelism,
            display_name=display_name, container=container,
            shared_name=shared_name, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  num_threads = _execute.make_int(num_threads, "num_threads")
  display_name = _execute.make_str(display_name, "display_name")
  if max_intra_op_parallelism is None:
    max_intra_op_parallelism = 1
  max_intra_op_parallelism = _execute.make_int(max_intra_op_parallelism, "max_intra_op_parallelism")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalThreadPoolHandle", num_threads=num_threads,
                                        display_name=display_name,
                                        max_intra_op_parallelism=max_intra_op_parallelism,
                                        container=container,
                                        shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("num_threads", _op.get_attr("num_threads"),
            "max_intra_op_parallelism",
            _op.get_attr("max_intra_op_parallelism"), "display_name",
            _op.get_attr("display_name"), "container",
            _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "ExperimentalThreadPoolHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_thread_pool_handle_eager_fallback(num_threads, display_name, max_intra_op_parallelism=1, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_thread_pool_handle
  """
  _ctx = ctx if ctx else _context.context()
  num_threads = _execute.make_int(num_threads, "num_threads")
  display_name = _execute.make_str(display_name, "display_name")
  if max_intra_op_parallelism is None:
    max_intra_op_parallelism = 1
  max_intra_op_parallelism = _execute.make_int(max_intra_op_parallelism, "max_intra_op_parallelism")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("num_threads", num_threads, "max_intra_op_parallelism",
  max_intra_op_parallelism, "display_name", display_name, "container",
  container, "shared_name", shared_name)
  _result = _execute.execute(b"ExperimentalThreadPoolHandle", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalThreadPoolHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_unbatch_dataset(input_dataset, output_types, output_shapes, name=None):
  r"""A dataset that splits the elements of its input into multiple elements.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalUnbatchDataset", name, _ctx._post_execution_callbacks,
        input_dataset, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_unbatch_dataset_eager_fallback(
            input_dataset, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_unbatch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_unbatch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalUnbatchDataset", input_dataset=input_dataset,
                                      output_types=output_types,
                                      output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalUnbatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_unbatch_dataset_eager_fallback(input_dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_unbatch_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_unbatch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_unbatch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalUnbatchDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalUnbatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def experimental_unique_dataset(input_dataset, output_types, output_shapes, name=None):
  r"""Creates a dataset that contains the unique elements of `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ExperimentalUniqueDataset", name, _ctx._post_execution_callbacks,
        input_dataset, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return experimental_unique_dataset_eager_fallback(
            input_dataset, output_types=output_types,
            output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_unique_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_unique_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ExperimentalUniqueDataset", input_dataset=input_dataset,
                                     output_types=output_types,
                                     output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ExperimentalUniqueDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def experimental_unique_dataset_eager_fallback(input_dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function experimental_unique_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'experimental_unique_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'experimental_unique_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ExperimentalUniqueDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ExperimentalUniqueDataset", _inputs_flat, _attrs, _result, name)
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
#   name: "ExperimentalAssertNextDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "transformations"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalBytesProducedStatsDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalCSVDataset"
#   input_arg {
#     name: "filenames"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "compression_type"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "buffer_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "header"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "field_delim"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "use_quote_delim"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "na_value"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "select_cols"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "record_defaults"
#     type_list_attr: "output_types"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_DOUBLE
#         type: DT_INT32
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalDatasetCardinality"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "cardinality"
#     type: DT_INT64
#   }
# }
# op {
#   name: "ExperimentalDatasetToTFRecord"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "filename"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "compression_type"
#     type: DT_STRING
#   }
# }
# op {
#   name: "ExperimentalDenseToSparseBatchDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "batch_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "row_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalDirectedInterleaveDataset"
#   input_arg {
#     name: "selector_input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "data_input_datasets"
#     type: DT_VARIANT
#     number_attr: "N"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "N"
#     type: "int"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalGroupByReducerDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "key_func_other_arguments"
#     type_list_attr: "Tkey_func_other_arguments"
#   }
#   input_arg {
#     name: "init_func_other_arguments"
#     type_list_attr: "Tinit_func_other_arguments"
#   }
#   input_arg {
#     name: "reduce_func_other_arguments"
#     type_list_attr: "Treduce_func_other_arguments"
#   }
#   input_arg {
#     name: "finalize_func_other_arguments"
#     type_list_attr: "Tfinalize_func_other_arguments"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "key_func"
#     type: "func"
#   }
#   attr {
#     name: "init_func"
#     type: "func"
#   }
#   attr {
#     name: "reduce_func"
#     type: "func"
#   }
#   attr {
#     name: "finalize_func"
#     type: "func"
#   }
#   attr {
#     name: "Tkey_func_other_arguments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tinit_func_other_arguments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Treduce_func_other_arguments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tfinalize_func_other_arguments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalGroupByWindowDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "key_func_other_arguments"
#     type_list_attr: "Tkey_func_other_arguments"
#   }
#   input_arg {
#     name: "reduce_func_other_arguments"
#     type_list_attr: "Treduce_func_other_arguments"
#   }
#   input_arg {
#     name: "window_size_func_other_arguments"
#     type_list_attr: "Twindow_size_func_other_arguments"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "key_func"
#     type: "func"
#   }
#   attr {
#     name: "reduce_func"
#     type: "func"
#   }
#   attr {
#     name: "window_size_func"
#     type: "func"
#   }
#   attr {
#     name: "Tkey_func_other_arguments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Treduce_func_other_arguments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Twindow_size_func_other_arguments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalIdentityIndexedDataset"
#   input_arg {
#     name: "size"
#     type: DT_UINT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalIgnoreErrorsDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalIndexedDatasetGet"
#   input_arg {
#     name: "materialized"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "index"
#     type: DT_UINT64
#   }
#   output_arg {
#     name: "components"
#     type_list_attr: "output_types"
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalIndexedDatasetMaterialize"
#   input_arg {
#     name: "dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "materialized"
#     type: DT_RESOURCE
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalIteratorGetDevice"
#   input_arg {
#     name: "resource"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "device"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalLMDBDataset"
#   input_arg {
#     name: "filenames"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalLatencyStatsDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalMapAndBatchDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "other_arguments"
#     type_list_attr: "Targuments"
#   }
#   input_arg {
#     name: "batch_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "num_parallel_calls"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "drop_remainder"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   attr {
#     name: "Targuments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "preserve_cardinality"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ExperimentalMapDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "other_arguments"
#     type_list_attr: "Targuments"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   attr {
#     name: "Targuments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "use_inter_op_parallelism"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   attr {
#     name: "preserve_cardinality"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ExperimentalMatchingFilesDataset"
#   input_arg {
#     name: "patterns"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalMaterializedIndexDatasetHandle"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "container"
#     type: "string"
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalMaxIntraOpParallelismDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "max_intra_op_parallelism"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalNonSerializableDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalNumaMapAndBatchDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "other_arguments"
#     type_list_attr: "Targuments"
#   }
#   input_arg {
#     name: "batch_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "num_parallel_calls"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "drop_remainder"
#     type: DT_BOOL
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   attr {
#     name: "Targuments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "preserve_cardinality"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ExperimentalParallelInterleaveDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "other_arguments"
#     type_list_attr: "Targuments"
#   }
#   input_arg {
#     name: "cycle_length"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "block_length"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "sloppy"
#     type: DT_BOOL
#   }
#   input_arg {
#     name: "buffer_output_elements"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "prefetch_input_elements"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   attr {
#     name: "Targuments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalParseExampleDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "num_parallel_calls"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "dense_defaults"
#     type_list_attr: "Tdense"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "sparse_keys"
#     type: "list(string)"
#     has_minimum: true
#   }
#   attr {
#     name: "dense_keys"
#     type: "list(string)"
#     has_minimum: true
#   }
#   attr {
#     name: "sparse_types"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "Tdense"
#     type: "list(type)"
#     has_minimum: true
#     allowed_values {
#       list {
#         type: DT_FLOAT
#         type: DT_INT64
#         type: DT_STRING
#       }
#     }
#   }
#   attr {
#     name: "dense_shapes"
#     type: "list(shape)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "sloppy"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ExperimentalPrivateThreadPoolDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "num_threads"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalRandomDataset"
#   input_arg {
#     name: "seed"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "seed2"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalScanDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "initial_state"
#     type_list_attr: "Tstate"
#   }
#   input_arg {
#     name: "other_arguments"
#     type_list_attr: "Targuments"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "f"
#     type: "func"
#   }
#   attr {
#     name: "Tstate"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "Targuments"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "preserve_cardinality"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ExperimentalSetStatsAggregatorDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "stats_aggregator"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "tag"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "counter_prefix"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalSleepDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "sleep_microseconds"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalSlidingWindowDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "window_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "window_shift"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "window_stride"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalSqlDataset"
#   input_arg {
#     name: "driver_name"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "data_source_name"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "query"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalStatsAggregatorHandle"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
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
#   is_stateful: true
# }
# op {
#   name: "ExperimentalStatsAggregatorSummary"
#   input_arg {
#     name: "iterator"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "summary"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalThreadPoolDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "thread_pool"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ExperimentalThreadPoolHandle"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "num_threads"
#     type: "int"
#   }
#   attr {
#     name: "max_intra_op_parallelism"
#     type: "int"
#     default_value {
#       i: 1
#     }
#   }
#   attr {
#     name: "display_name"
#     type: "string"
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
#   is_stateful: true
# }
# op {
#   name: "ExperimentalUnbatchDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "ExperimentalUniqueDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     has_minimum: true
#     minimum: 1
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\225\001\n\035ExperimentalAssertNextDataset\022\021\n\rinput_dataset\030\025\022\023\n\017transformations\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\221\001\n%ExperimentalBytesProducedStatsDataset\022\021\n\rinput_dataset\030\025\022\007\n\003tag\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\234\002\n\026ExperimentalCSVDataset\022\r\n\tfilenames\030\007\022\024\n\020compression_type\030\007\022\017\n\013buffer_size\030\t\022\n\n\006header\030\n\022\017\n\013field_delim\030\007\022\023\n\017use_quote_delim\030\n\022\014\n\010na_value\030\007\022\017\n\013select_cols\030\t\022\037\n\017record_defaults2\014output_types\032\n\n\006handle\030\025\")\n\014output_types\022\nlist(type)(\0010\001:\t\n\0072\005\001\002\003\t\007\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\nD\n\036ExperimentalDatasetCardinality\022\021\n\rinput_dataset\030\025\032\017\n\013cardinality\030\t\nV\n\035ExperimentalDatasetToTFRecord\022\021\n\rinput_dataset\030\025\022\014\n\010filename\030\007\022\024\n\020compression_type\030\007\n\247\001\n%ExperimentalDenseToSparseBatchDataset\022\021\n\rinput_dataset\030\025\022\016\n\nbatch_size\030\t\022\r\n\trow_shape\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\273\001\n%ExperimentalDirectedInterleaveDataset\022\032\n\026selector_input_dataset\030\025\022\032\n\023data_input_datasets\030\025*\001N\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\014\n\001N\022\003int(\0010\001\n\373\004\n!ExperimentalGroupByReducerDataset\022\021\n\rinput_dataset\030\025\0225\n\030key_func_other_arguments2\031Tkey_func_other_arguments\0227\n\031init_func_other_arguments2\032Tinit_func_other_arguments\022;\n\033reduce_func_other_arguments2\034Treduce_func_other_arguments\022?\n\035finalize_func_other_arguments2\036Tfinalize_func_other_arguments\032\n\n\006handle\030\025\"\020\n\010key_func\022\004func\"\021\n\tinit_func\022\004func\"\023\n\013reduce_func\022\004func\"\025\n\rfinalize_func\022\004func\")\n\031Tkey_func_other_arguments\022\nlist(type)(\001\"*\n\032Tinit_func_other_arguments\022\nlist(type)(\001\",\n\034Treduce_func_other_arguments\022\nlist(type)(\001\".\n\036Tfinalize_func_other_arguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\213\004\n ExperimentalGroupByWindowDataset\022\021\n\rinput_dataset\030\025\0225\n\030key_func_other_arguments2\031Tkey_func_other_arguments\022;\n\033reduce_func_other_arguments2\034Treduce_func_other_arguments\022E\n window_size_func_other_arguments2!Twindow_size_func_other_arguments\032\n\n\006handle\030\025\"\020\n\010key_func\022\004func\"\023\n\013reduce_func\022\004func\"\030\n\020window_size_func\022\004func\")\n\031Tkey_func_other_arguments\022\nlist(type)(\001\",\n\034Treduce_func_other_arguments\022\nlist(type)(\001\"1\n!Twindow_size_func_other_arguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n=\n\"ExperimentalIdentityIndexedDataset\022\010\n\004size\030\027\032\n\n\006handle\030\025\210\001\001\n\202\001\n\037ExperimentalIgnoreErrorsDataset\022\021\n\rinput_dataset\030\025\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\235\001\n\035ExperimentalIndexedDatasetGet\022\020\n\014materialized\030\024\022\t\n\005index\030\027\032\032\n\ncomponents2\014output_types\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\nI\n%ExperimentalIndexedDatasetMaterialize\022\013\n\007dataset\030\025\022\020\n\014materialized\030\024\210\001\001\n<\n\035ExperimentalIteratorGetDevice\022\014\n\010resource\030\024\032\n\n\006device\030\007\210\001\001\ny\n\027ExperimentalLMDBDataset\022\r\n\tfilenames\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\213\001\n\037ExperimentalLatencyStatsDataset\022\021\n\rinput_dataset\030\025\022\007\n\003tag\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\245\002\n\036ExperimentalMapAndBatchDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\022\016\n\nbatch_size\030\t\022\026\n\022num_parallel_calls\030\t\022\022\n\016drop_remainder\030\n\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\" \n\024preserve_cardinality\022\004bool\032\002(\000\n\207\002\n\026ExperimentalMapDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"$\n\030use_inter_op_parallelism\022\004bool\032\002(\001\" \n\024preserve_cardinality\022\004bool\032\002(\000\n?\n ExperimentalMatchingFilesDataset\022\014\n\010patterns\030\007\032\n\n\006handle\030\025\210\001\001\n\251\001\n*ExperimentalMaterializedIndexDatasetHandle\032\n\n\006handle\030\024\"\023\n\tcontainer\022\006string\"\025\n\013shared_name\022\006string\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\251\001\n(ExperimentalMaxIntraOpParallelismDataset\022\021\n\rinput_dataset\030\025\022\034\n\030max_intra_op_parallelism\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\205\001\n\"ExperimentalNonSerializableDataset\022\021\n\rinput_dataset\030\025\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\251\002\n\"ExperimentalNumaMapAndBatchDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\022\016\n\nbatch_size\030\t\022\026\n\022num_parallel_calls\030\t\022\022\n\016drop_remainder\030\n\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\" \n\024preserve_cardinality\022\004bool\032\002(\000\n\267\002\n%ExperimentalParallelInterleaveDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\022\020\n\014cycle_length\030\t\022\020\n\014block_length\030\t\022\n\n\006sloppy\030\n\022\032\n\026buffer_output_elements\030\t\022\033\n\027prefetch_input_elements\030\t\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\354\002\n\037ExperimentalParseExampleDataset\022\021\n\rinput_dataset\030\025\022\026\n\022num_parallel_calls\030\t\022\030\n\016dense_defaults2\006Tdense\032\n\n\006handle\030\025\"\035\n\013sparse_keys\022\014list(string)(\001\"\034\n\ndense_keys\022\014list(string)(\001\"%\n\014sparse_types\022\nlist(type)(\001:\007\n\0052\003\001\t\007\"\037\n\006Tdense\022\nlist(type)(\001:\007\n\0052\003\001\t\007\"\035\n\014dense_shapes\022\013list(shape)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\022\n\006sloppy\022\004bool\032\002(\000\n\230\001\n$ExperimentalPrivateThreadPoolDataset\022\021\n\rinput_dataset\030\025\022\017\n\013num_threads\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\201\001\n\031ExperimentalRandomDataset\022\010\n\004seed\030\t\022\t\n\005seed2\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\225\002\n\027ExperimentalScanDataset\022\021\n\rinput_dataset\030\025\022\027\n\rinitial_state2\006Tstate\022\035\n\017other_arguments2\nTarguments\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\030\n\006Tstate\022\nlist(type)(\0010\001\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\" \n\024preserve_cardinality\022\004bool\032\002(\000\n\276\001\n%ExperimentalSetStatsAggregatorDataset\022\021\n\rinput_dataset\030\025\022\024\n\020stats_aggregator\030\024\022\007\n\003tag\030\007\022\022\n\016counter_prefix\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\223\001\n\030ExperimentalSleepDataset\022\021\n\rinput_dataset\030\025\022\026\n\022sleep_microseconds\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\271\001\n ExperimentalSlidingWindowDataset\022\021\n\rinput_dataset\030\025\022\017\n\013window_size\030\t\022\020\n\014window_shift\030\t\022\021\n\rwindow_stride\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\233\001\n\026ExperimentalSqlDataset\022\017\n\013driver_name\030\007\022\024\n\020data_source_name\030\007\022\t\n\005query\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\nf\n!ExperimentalStatsAggregatorHandle\032\n\n\006handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\nB\n\"ExperimentalStatsAggregatorSummary\022\014\n\010iterator\030\024\032\013\n\007summary\030\007\210\001\001\n\224\001\n\035ExperimentalThreadPoolDataset\022\021\n\rinput_dataset\030\025\022\017\n\013thread_pool\030\024\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\262\001\n\034ExperimentalThreadPoolHandle\032\n\n\006handle\030\024\"\022\n\013num_threads\022\003int\"#\n\030max_intra_op_parallelism\022\003int\032\002\030\001\"\026\n\014display_name\022\006string\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n}\n\032ExperimentalUnbatchDataset\022\021\n\rinput_dataset\030\025\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n|\n\031ExperimentalUniqueDataset\022\021\n\rinput_dataset\030\025\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001")
