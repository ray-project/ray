"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: dataset_ops.cc
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


def anonymous_iterator(output_types, output_shapes, name=None):
  r"""A container for an iterator resource.

  Args:
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
        "AnonymousIterator", name, _ctx._post_execution_callbacks,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return anonymous_iterator_eager_fallback(
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
        "'anonymous_iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'anonymous_iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "AnonymousIterator", output_types=output_types,
                             output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "AnonymousIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def anonymous_iterator_eager_fallback(output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function anonymous_iterator
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'anonymous_iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'anonymous_iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _inputs_flat = []
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"AnonymousIterator", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "AnonymousIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_dataset(input_dataset, batch_size, output_types, output_shapes, name=None):
  r"""Creates a dataset that batches `batch_size` elements from `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    batch_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a
      batch.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "BatchDataset",
        name, _ctx._post_execution_callbacks, input_dataset, batch_size,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return batch_dataset_eager_fallback(
            input_dataset, batch_size, output_types=output_types,
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
        "'batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchDataset", input_dataset=input_dataset, batch_size=batch_size,
                        output_types=output_types,
                        output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "BatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_dataset_eager_fallback(input_dataset, batch_size, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'batch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  batch_size = _ops.convert_to_tensor(batch_size, _dtypes.int64)
  _inputs_flat = [input_dataset, batch_size]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"BatchDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def batch_dataset_v2(input_dataset, batch_size, drop_remainder, output_types, output_shapes, name=None):
  r"""Creates a dataset that batches `batch_size` elements from `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    batch_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a batch.
    drop_remainder: A `Tensor` of type `bool`.
      A scalar representing whether the last batch should be dropped in case its size
      is smaller than desired.
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
        "BatchDatasetV2", name, _ctx._post_execution_callbacks, input_dataset,
        batch_size, drop_remainder, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return batch_dataset_v2_eager_fallback(
            input_dataset, batch_size, drop_remainder,
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
        "'batch_dataset_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'batch_dataset_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "BatchDatasetV2", input_dataset=input_dataset, batch_size=batch_size,
                          drop_remainder=drop_remainder,
                          output_types=output_types,
                          output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "BatchDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def batch_dataset_v2_eager_fallback(input_dataset, batch_size, drop_remainder, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function batch_dataset_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'batch_dataset_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'batch_dataset_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  batch_size = _ops.convert_to_tensor(batch_size, _dtypes.int64)
  drop_remainder = _ops.convert_to_tensor(drop_remainder, _dtypes.bool)
  _inputs_flat = [input_dataset, batch_size, drop_remainder]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"BatchDatasetV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "BatchDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def cache_dataset(input_dataset, filename, output_types, output_shapes, name=None):
  r"""Creates a dataset that caches elements from `input_dataset`.

  A CacheDataset will iterate over the input_dataset, and store tensors. If the
  cache already exists, the cache will be used. If the cache is inappropriate
  (e.g. cannot be opened, contains tensors of the wrong shape / size), an error
  will the returned when used.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    filename: A `Tensor` of type `string`.
      A path on the filesystem where we should cache the dataset. Note: this
      will be a directory.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "CacheDataset",
        name, _ctx._post_execution_callbacks, input_dataset, filename,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return cache_dataset_eager_fallback(
            input_dataset, filename, output_types=output_types,
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
        "'cache_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'cache_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "CacheDataset", input_dataset=input_dataset, filename=filename,
                        output_types=output_types,
                        output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "CacheDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def cache_dataset_eager_fallback(input_dataset, filename, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function cache_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'cache_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'cache_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  filename = _ops.convert_to_tensor(filename, _dtypes.string)
  _inputs_flat = [input_dataset, filename]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"CacheDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "CacheDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def concatenate_dataset(input_dataset, another_dataset, output_types, output_shapes, name=None):
  r"""Creates a dataset that concatenates `input_dataset` with `another_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    another_dataset: A `Tensor` of type `variant`.
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
        "ConcatenateDataset", name, _ctx._post_execution_callbacks,
        input_dataset, another_dataset, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return concatenate_dataset_eager_fallback(
            input_dataset, another_dataset, output_types=output_types,
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
        "'concatenate_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'concatenate_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ConcatenateDataset", input_dataset=input_dataset,
                              another_dataset=another_dataset,
                              output_types=output_types,
                              output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ConcatenateDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def concatenate_dataset_eager_fallback(input_dataset, another_dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function concatenate_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'concatenate_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'concatenate_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  another_dataset = _ops.convert_to_tensor(another_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset, another_dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ConcatenateDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ConcatenateDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def dataset_to_graph(input_dataset, name=None):
  r"""Returns a serialized GraphDef representing `input_dataset`.

  Returns a graph representation for `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the dataset to return the graph representation for.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DatasetToGraph", name, _ctx._post_execution_callbacks, input_dataset)
      return _result
    except _core._FallbackException:
      try:
        return dataset_to_graph_eager_fallback(
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
        "DatasetToGraph", input_dataset=input_dataset, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "DatasetToGraph", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def dataset_to_graph_eager_fallback(input_dataset, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dataset_to_graph
  """
  _ctx = ctx if ctx else _context.context()
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = None
  _result = _execute.execute(b"DatasetToGraph", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "DatasetToGraph", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def dataset_to_single_element(dataset, output_types, output_shapes, name=None):
  r"""Outputs the single element from the given dataset.

  Args:
    dataset: A `Tensor` of type `variant`.
      A handle to a dataset that contains a single element.
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
        "DatasetToSingleElement", name, _ctx._post_execution_callbacks,
        dataset, "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return dataset_to_single_element_eager_fallback(
            dataset, output_types=output_types, output_shapes=output_shapes,
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
        "'dataset_to_single_element' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'dataset_to_single_element' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "DatasetToSingleElement", dataset=dataset, output_types=output_types,
                                  output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "DatasetToSingleElement", _inputs_flat, _attrs, _result, name)
  return _result



def dataset_to_single_element_eager_fallback(dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function dataset_to_single_element
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'dataset_to_single_element' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'dataset_to_single_element' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  dataset = _ops.convert_to_tensor(dataset, _dtypes.variant)
  _inputs_flat = [dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"DatasetToSingleElement", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "DatasetToSingleElement", _inputs_flat, _attrs, _result, name)
  return _result


def deserialize_iterator(resource_handle, serialized, name=None):
  r"""Converts the given variant tensor to an iterator and stores it in the given resource.

  Args:
    resource_handle: A `Tensor` of type `resource`.
      A handle to an iterator resource.
    serialized: A `Tensor` of type `variant`.
      A variant tensor storing the state of the iterator contained in the
      resource.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "DeserializeIterator", name, _ctx._post_execution_callbacks,
        resource_handle, serialized)
      return _result
    except _core._FallbackException:
      try:
        return deserialize_iterator_eager_fallback(
            resource_handle, serialized, name=name, ctx=_ctx)
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
        "DeserializeIterator", resource_handle=resource_handle,
                               serialized=serialized, name=name)
  return _op
  _result = None
  return _result



def deserialize_iterator_eager_fallback(resource_handle, serialized, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function deserialize_iterator
  """
  _ctx = ctx if ctx else _context.context()
  resource_handle = _ops.convert_to_tensor(resource_handle, _dtypes.resource)
  serialized = _ops.convert_to_tensor(serialized, _dtypes.variant)
  _inputs_flat = [resource_handle, serialized]
  _attrs = None
  _result = _execute.execute(b"DeserializeIterator", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def filter_by_last_component_dataset(input_dataset, output_types, output_shapes, name=None):
  r"""Creates a dataset containing elements of first component of `input_dataset` having true in the last component.

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
        "FilterByLastComponentDataset", name, _ctx._post_execution_callbacks,
        input_dataset, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return filter_by_last_component_dataset_eager_fallback(
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
        "'filter_by_last_component_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'filter_by_last_component_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "FilterByLastComponentDataset", input_dataset=input_dataset,
                                        output_types=output_types,
                                        output_shapes=output_shapes,
                                        name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "FilterByLastComponentDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def filter_by_last_component_dataset_eager_fallback(input_dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function filter_by_last_component_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'filter_by_last_component_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'filter_by_last_component_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"FilterByLastComponentDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FilterByLastComponentDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def filter_dataset(input_dataset, other_arguments, predicate, output_types, output_shapes, name=None):
  r"""Creates a dataset containing elements of `input_dataset` matching `predicate`.

  The `predicate` function must return a scalar boolean and accept the
  following arguments:

  * One tensor for each component of an element of `input_dataset`.
  * One tensor for each value in `other_arguments`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    other_arguments: A list of `Tensor` objects.
      A list of tensors, typically values that were captured when
      building a closure for `predicate`.
    predicate: A function decorated with @Defun.
      A function returning a scalar boolean.
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
        "FilterDataset", name, _ctx._post_execution_callbacks, input_dataset,
        other_arguments, "predicate", predicate, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return filter_dataset_eager_fallback(
            input_dataset, other_arguments, predicate=predicate,
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
        "'filter_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'filter_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "FilterDataset", input_dataset=input_dataset,
                         other_arguments=other_arguments, predicate=predicate,
                         output_types=output_types,
                         output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("predicate", _op.get_attr("predicate"), "Targuments",
            _op.get_attr("Targuments"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "FilterDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def filter_dataset_eager_fallback(input_dataset, other_arguments, predicate, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function filter_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'filter_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'filter_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset] + list(other_arguments)
  _attrs = ("predicate", predicate, "Targuments", _attr_Targuments,
  "output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"FilterDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FilterDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def fixed_length_record_dataset(filenames, header_bytes, record_bytes, footer_bytes, buffer_size, name=None):
  r"""Creates a dataset that emits the records from one or more binary files.

  Args:
    filenames: A `Tensor` of type `string`.
      A scalar or a vector containing the name(s) of the file(s) to be
      read.
    header_bytes: A `Tensor` of type `int64`.
      A scalar representing the number of bytes to skip at the
      beginning of a file.
    record_bytes: A `Tensor` of type `int64`.
      A scalar representing the number of bytes in each record.
    footer_bytes: A `Tensor` of type `int64`.
      A scalar representing the number of bytes to skip at the end
      of a file.
    buffer_size: A `Tensor` of type `int64`.
      A scalar representing the number of bytes to buffer. Must be > 0.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FixedLengthRecordDataset", name, _ctx._post_execution_callbacks,
        filenames, header_bytes, record_bytes, footer_bytes, buffer_size)
      return _result
    except _core._FallbackException:
      try:
        return fixed_length_record_dataset_eager_fallback(
            filenames, header_bytes, record_bytes, footer_bytes, buffer_size,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "FixedLengthRecordDataset", filenames=filenames,
                                    header_bytes=header_bytes,
                                    record_bytes=record_bytes,
                                    footer_bytes=footer_bytes,
                                    buffer_size=buffer_size, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "FixedLengthRecordDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fixed_length_record_dataset_eager_fallback(filenames, header_bytes, record_bytes, footer_bytes, buffer_size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fixed_length_record_dataset
  """
  _ctx = ctx if ctx else _context.context()
  filenames = _ops.convert_to_tensor(filenames, _dtypes.string)
  header_bytes = _ops.convert_to_tensor(header_bytes, _dtypes.int64)
  record_bytes = _ops.convert_to_tensor(record_bytes, _dtypes.int64)
  footer_bytes = _ops.convert_to_tensor(footer_bytes, _dtypes.int64)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  _inputs_flat = [filenames, header_bytes, record_bytes, footer_bytes, buffer_size]
  _attrs = None
  _result = _execute.execute(b"FixedLengthRecordDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FixedLengthRecordDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def fixed_length_record_dataset_v2(filenames, header_bytes, record_bytes, footer_bytes, buffer_size, compression_type, name=None):
  r"""TODO: add doc.

  Args:
    filenames: A `Tensor` of type `string`.
    header_bytes: A `Tensor` of type `int64`.
    record_bytes: A `Tensor` of type `int64`.
    footer_bytes: A `Tensor` of type `int64`.
    buffer_size: A `Tensor` of type `int64`.
    compression_type: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FixedLengthRecordDatasetV2", name, _ctx._post_execution_callbacks,
        filenames, header_bytes, record_bytes, footer_bytes, buffer_size,
        compression_type)
      return _result
    except _core._FallbackException:
      try:
        return fixed_length_record_dataset_v2_eager_fallback(
            filenames, header_bytes, record_bytes, footer_bytes, buffer_size,
            compression_type, name=name, ctx=_ctx)
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
        "FixedLengthRecordDatasetV2", filenames=filenames,
                                      header_bytes=header_bytes,
                                      record_bytes=record_bytes,
                                      footer_bytes=footer_bytes,
                                      buffer_size=buffer_size,
                                      compression_type=compression_type,
                                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "FixedLengthRecordDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fixed_length_record_dataset_v2_eager_fallback(filenames, header_bytes, record_bytes, footer_bytes, buffer_size, compression_type, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fixed_length_record_dataset_v2
  """
  _ctx = ctx if ctx else _context.context()
  filenames = _ops.convert_to_tensor(filenames, _dtypes.string)
  header_bytes = _ops.convert_to_tensor(header_bytes, _dtypes.int64)
  record_bytes = _ops.convert_to_tensor(record_bytes, _dtypes.int64)
  footer_bytes = _ops.convert_to_tensor(footer_bytes, _dtypes.int64)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  compression_type = _ops.convert_to_tensor(compression_type, _dtypes.string)
  _inputs_flat = [filenames, header_bytes, record_bytes, footer_bytes, buffer_size, compression_type]
  _attrs = None
  _result = _execute.execute(b"FixedLengthRecordDatasetV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FixedLengthRecordDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def flat_map_dataset(input_dataset, other_arguments, f, output_types, output_shapes, name=None):
  r"""Creates a dataset that applies `f` to the outputs of `input_dataset`.

  Unlike MapDataset, the `f` in FlatMapDataset is expected to return a
  Dataset variant, and FlatMapDataset will flatten successive results
  into a single Dataset.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    other_arguments: A list of `Tensor` objects.
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
        "FlatMapDataset", name, _ctx._post_execution_callbacks, input_dataset,
        other_arguments, "f", f, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return flat_map_dataset_eager_fallback(
            input_dataset, other_arguments, f=f, output_types=output_types,
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
        "'flat_map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'flat_map_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "FlatMapDataset", input_dataset=input_dataset,
                          other_arguments=other_arguments, f=f,
                          output_types=output_types,
                          output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "FlatMapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def flat_map_dataset_eager_fallback(input_dataset, other_arguments, f, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function flat_map_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'flat_map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'flat_map_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset] + list(other_arguments)
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"FlatMapDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "FlatMapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def generator_dataset(init_func_other_args, next_func_other_args, finalize_func_other_args, init_func, next_func, finalize_func, output_types, output_shapes, name=None):
  r"""Creates a dataset that invokes a function to generate elements.

  Args:
    init_func_other_args: A list of `Tensor` objects.
    next_func_other_args: A list of `Tensor` objects.
    finalize_func_other_args: A list of `Tensor` objects.
    init_func: A function decorated with @Defun.
    next_func: A function decorated with @Defun.
    finalize_func: A function decorated with @Defun.
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
        "GeneratorDataset", name, _ctx._post_execution_callbacks,
        init_func_other_args, next_func_other_args, finalize_func_other_args,
        "init_func", init_func, "next_func", next_func, "finalize_func",
        finalize_func, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return generator_dataset_eager_fallback(
            init_func_other_args, next_func_other_args,
            finalize_func_other_args, init_func=init_func,
            next_func=next_func, finalize_func=finalize_func,
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
        "'generator_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'generator_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "GeneratorDataset", init_func_other_args=init_func_other_args,
                            next_func_other_args=next_func_other_args,
                            finalize_func_other_args=finalize_func_other_args,
                            init_func=init_func, next_func=next_func,
                            finalize_func=finalize_func,
                            output_types=output_types,
                            output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("init_func", _op.get_attr("init_func"), "next_func",
            _op.get_attr("next_func"), "finalize_func",
            _op.get_attr("finalize_func"), "Tinit_func_args",
            _op.get_attr("Tinit_func_args"), "Tnext_func_args",
            _op.get_attr("Tnext_func_args"), "Tfinalize_func_args",
            _op.get_attr("Tfinalize_func_args"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "GeneratorDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def generator_dataset_eager_fallback(init_func_other_args, next_func_other_args, finalize_func_other_args, init_func, next_func, finalize_func, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function generator_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'generator_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'generator_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Tinit_func_args, init_func_other_args = _execute.convert_to_mixed_eager_tensors(init_func_other_args, _ctx)
  _attr_Tnext_func_args, next_func_other_args = _execute.convert_to_mixed_eager_tensors(next_func_other_args, _ctx)
  _attr_Tfinalize_func_args, finalize_func_other_args = _execute.convert_to_mixed_eager_tensors(finalize_func_other_args, _ctx)
  _inputs_flat = list(init_func_other_args) + list(next_func_other_args) + list(finalize_func_other_args)
  _attrs = ("init_func", init_func, "next_func", next_func, "finalize_func",
  finalize_func, "Tinit_func_args", _attr_Tinit_func_args, "Tnext_func_args",
  _attr_Tnext_func_args, "Tfinalize_func_args", _attr_Tfinalize_func_args,
  "output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"GeneratorDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "GeneratorDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def interleave_dataset(input_dataset, other_arguments, cycle_length, block_length, f, output_types, output_shapes, name=None):
  r"""Creates a dataset that applies `f` to the outputs of `input_dataset`.

  Unlike MapDataset, the `f` in InterleaveDataset is expected to return
  a Dataset variant, and InterleaveDataset will flatten successive
  results into a single Dataset. Unlike FlatMapDataset,
  InterleaveDataset will interleave sequences of up to `block_length`
  consecutive elements from `cycle_length` input elements.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    other_arguments: A list of `Tensor` objects.
    cycle_length: A `Tensor` of type `int64`.
    block_length: A `Tensor` of type `int64`.
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
        "InterleaveDataset", name, _ctx._post_execution_callbacks,
        input_dataset, other_arguments, cycle_length, block_length, "f", f,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return interleave_dataset_eager_fallback(
            input_dataset, other_arguments, cycle_length, block_length, f=f,
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
        "'interleave_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'interleave_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "InterleaveDataset", input_dataset=input_dataset,
                             other_arguments=other_arguments,
                             cycle_length=cycle_length,
                             block_length=block_length, f=f,
                             output_types=output_types,
                             output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "InterleaveDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def interleave_dataset_eager_fallback(input_dataset, other_arguments, cycle_length, block_length, f, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function interleave_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'interleave_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'interleave_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  cycle_length = _ops.convert_to_tensor(cycle_length, _dtypes.int64)
  block_length = _ops.convert_to_tensor(block_length, _dtypes.int64)
  _inputs_flat = [input_dataset] + list(other_arguments) + [cycle_length, block_length]
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"InterleaveDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "InterleaveDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def iterator(shared_name, container, output_types, output_shapes, name=None):
  r"""A container for an iterator resource.

  Args:
    shared_name: A `string`.
    container: A `string`.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "Iterator",
        name, _ctx._post_execution_callbacks, "shared_name", shared_name,
        "container", container, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return iterator_eager_fallback(
            shared_name=shared_name, container=container,
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
  shared_name = _execute.make_str(shared_name, "shared_name")
  container = _execute.make_str(container, "container")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "Iterator", shared_name=shared_name, container=container,
                    output_types=output_types, output_shapes=output_shapes,
                    name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("shared_name", _op.get_attr("shared_name"), "container",
            _op.get_attr("container"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "Iterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def iterator_eager_fallback(shared_name, container, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator
  """
  _ctx = ctx if ctx else _context.context()
  shared_name = _execute.make_str(shared_name, "shared_name")
  container = _execute.make_str(container, "container")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _inputs_flat = []
  _attrs = ("shared_name", shared_name, "container", container,
  "output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"Iterator", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "Iterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def iterator_from_string_handle(string_handle, output_types=[], output_shapes=[], name=None):
  r"""Converts the given string representing a handle to an iterator to a resource.

  Args:
    string_handle: A `Tensor` of type `string`.
      A string representation of the given handle.
    output_types: An optional list of `tf.DTypes`. Defaults to `[]`.
      If specified, defines the type of each tuple component in an
      element produced by the resulting iterator.
    output_shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      If specified, defines the shape of each tuple component in an
      element produced by the resulting iterator.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IteratorFromStringHandle", name, _ctx._post_execution_callbacks,
        string_handle, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return iterator_from_string_handle_eager_fallback(
            string_handle, output_types=output_types,
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
  if output_types is None:
    output_types = []
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_from_string_handle' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_from_string_handle' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "IteratorFromStringHandle", string_handle=string_handle,
                                    output_types=output_types,
                                    output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "IteratorFromStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def iterator_from_string_handle_eager_fallback(string_handle, output_types=[], output_shapes=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator_from_string_handle
  """
  _ctx = ctx if ctx else _context.context()
  if output_types is None:
    output_types = []
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_from_string_handle' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_from_string_handle' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  string_handle = _ops.convert_to_tensor(string_handle, _dtypes.string)
  _inputs_flat = [string_handle]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"IteratorFromStringHandle", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IteratorFromStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def iterator_from_string_handle_v2(string_handle, output_types=[], output_shapes=[], name=None):
  r"""TODO: add doc.

  Args:
    string_handle: A `Tensor` of type `string`.
    output_types: An optional list of `tf.DTypes`. Defaults to `[]`.
    output_shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IteratorFromStringHandleV2", name, _ctx._post_execution_callbacks,
        string_handle, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return iterator_from_string_handle_v2_eager_fallback(
            string_handle, output_types=output_types,
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
  if output_types is None:
    output_types = []
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_from_string_handle_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_from_string_handle_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "IteratorFromStringHandleV2", string_handle=string_handle,
                                      output_types=output_types,
                                      output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "IteratorFromStringHandleV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def iterator_from_string_handle_v2_eager_fallback(string_handle, output_types=[], output_shapes=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator_from_string_handle_v2
  """
  _ctx = ctx if ctx else _context.context()
  if output_types is None:
    output_types = []
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_from_string_handle_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_from_string_handle_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  string_handle = _ops.convert_to_tensor(string_handle, _dtypes.string)
  _inputs_flat = [string_handle]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"IteratorFromStringHandleV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IteratorFromStringHandleV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def iterator_get_next(iterator, output_types, output_shapes, name=None):
  r"""Gets the next output from the given iterator .

  Args:
    iterator: A `Tensor` of type `resource`.
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
        "IteratorGetNext", name, _ctx._post_execution_callbacks, iterator,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return iterator_get_next_eager_fallback(
            iterator, output_types=output_types, output_shapes=output_shapes,
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
        "'iterator_get_next' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_get_next' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "IteratorGetNext", iterator=iterator, output_types=output_types,
                           output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "IteratorGetNext", _inputs_flat, _attrs, _result, name)
  return _result



def iterator_get_next_eager_fallback(iterator, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator_get_next
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_get_next' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_get_next' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  iterator = _ops.convert_to_tensor(iterator, _dtypes.resource)
  _inputs_flat = [iterator]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"IteratorGetNext", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IteratorGetNext", _inputs_flat, _attrs, _result, name)
  return _result


def iterator_get_next_as_optional(iterator, output_types, output_shapes, name=None):
  r"""Gets the next output from the given iterator as an Optional variant.

  Args:
    iterator: A `Tensor` of type `resource`.
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
        "IteratorGetNextAsOptional", name, _ctx._post_execution_callbacks,
        iterator, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return iterator_get_next_as_optional_eager_fallback(
            iterator, output_types=output_types, output_shapes=output_shapes,
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
        "'iterator_get_next_as_optional' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_get_next_as_optional' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "IteratorGetNextAsOptional", iterator=iterator,
                                     output_types=output_types,
                                     output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "IteratorGetNextAsOptional", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def iterator_get_next_as_optional_eager_fallback(iterator, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator_get_next_as_optional
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_get_next_as_optional' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_get_next_as_optional' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  iterator = _ops.convert_to_tensor(iterator, _dtypes.resource)
  _inputs_flat = [iterator]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"IteratorGetNextAsOptional", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IteratorGetNextAsOptional", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def iterator_get_next_sync(iterator, output_types, output_shapes, name=None):
  r"""Gets the next output from the given iterator.

  This operation is a synchronous version IteratorGetNext. It should only be used
  in situations where the iterator does not block the calling thread, or where
  the calling thread is not a member of the thread pool used to execute parallel
  operations (e.g. in eager mode).

  Args:
    iterator: A `Tensor` of type `resource`.
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
        "IteratorGetNextSync", name, _ctx._post_execution_callbacks, iterator,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return iterator_get_next_sync_eager_fallback(
            iterator, output_types=output_types, output_shapes=output_shapes,
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
        "'iterator_get_next_sync' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_get_next_sync' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "IteratorGetNextSync", iterator=iterator, output_types=output_types,
                               output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "IteratorGetNextSync", _inputs_flat, _attrs, _result, name)
  return _result



def iterator_get_next_sync_eager_fallback(iterator, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator_get_next_sync
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_get_next_sync' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_get_next_sync' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  iterator = _ops.convert_to_tensor(iterator, _dtypes.resource)
  _inputs_flat = [iterator]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"IteratorGetNextSync", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IteratorGetNextSync", _inputs_flat, _attrs, _result, name)
  return _result


def iterator_to_string_handle(resource_handle, name=None):
  r"""Converts the given `resource_handle` representing an iterator to a string.

  Args:
    resource_handle: A `Tensor` of type `resource`.
      A handle to an iterator resource.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IteratorToStringHandle", name, _ctx._post_execution_callbacks,
        resource_handle)
      return _result
    except _core._FallbackException:
      try:
        return iterator_to_string_handle_eager_fallback(
            resource_handle, name=name, ctx=_ctx)
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
        "IteratorToStringHandle", resource_handle=resource_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "IteratorToStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def iterator_to_string_handle_eager_fallback(resource_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator_to_string_handle
  """
  _ctx = ctx if ctx else _context.context()
  resource_handle = _ops.convert_to_tensor(resource_handle, _dtypes.resource)
  _inputs_flat = [resource_handle]
  _attrs = None
  _result = _execute.execute(b"IteratorToStringHandle", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "IteratorToStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def iterator_v2(shared_name, container, output_types, output_shapes, name=None):
  r"""TODO: add doc.

  Args:
    shared_name: A `string`.
    container: A `string`.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "IteratorV2",
        name, _ctx._post_execution_callbacks, "shared_name", shared_name,
        "container", container, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return iterator_v2_eager_fallback(
            shared_name=shared_name, container=container,
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
  shared_name = _execute.make_str(shared_name, "shared_name")
  container = _execute.make_str(container, "container")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "IteratorV2", shared_name=shared_name, container=container,
                      output_types=output_types, output_shapes=output_shapes,
                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("shared_name", _op.get_attr("shared_name"), "container",
            _op.get_attr("container"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "IteratorV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def iterator_v2_eager_fallback(shared_name, container, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function iterator_v2
  """
  _ctx = ctx if ctx else _context.context()
  shared_name = _execute.make_str(shared_name, "shared_name")
  container = _execute.make_str(container, "container")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'iterator_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'iterator_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _inputs_flat = []
  _attrs = ("shared_name", shared_name, "container", container,
  "output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"IteratorV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "IteratorV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def make_iterator(dataset, iterator, name=None):
  r"""Makes a new iterator from the given `dataset` and stores it in `iterator`.

  This operation may be executed multiple times. Each execution will reset the
  iterator in `iterator` to the first element of `dataset`.

  Args:
    dataset: A `Tensor` of type `variant`.
    iterator: A `Tensor` of type `resource`.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MakeIterator",
        name, _ctx._post_execution_callbacks, dataset, iterator)
      return _result
    except _core._FallbackException:
      try:
        return make_iterator_eager_fallback(
            dataset, iterator, name=name, ctx=_ctx)
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
        "MakeIterator", dataset=dataset, iterator=iterator, name=name)
  return _op
  _result = None
  return _result



def make_iterator_eager_fallback(dataset, iterator, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function make_iterator
  """
  _ctx = ctx if ctx else _context.context()
  dataset = _ops.convert_to_tensor(dataset, _dtypes.variant)
  iterator = _ops.convert_to_tensor(iterator, _dtypes.resource)
  _inputs_flat = [dataset, iterator]
  _attrs = None
  _result = _execute.execute(b"MakeIterator", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def map_dataset(input_dataset, other_arguments, f, output_types, output_shapes, use_inter_op_parallelism=True, preserve_cardinality=False, name=None):
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
        _ctx._context_handle, _ctx._eager_context.device_name, "MapDataset",
        name, _ctx._post_execution_callbacks, input_dataset, other_arguments,
        "f", f, "output_types", output_types, "output_shapes", output_shapes,
        "use_inter_op_parallelism", use_inter_op_parallelism,
        "preserve_cardinality", preserve_cardinality)
      return _result
    except _core._FallbackException:
      try:
        return map_dataset_eager_fallback(
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
        "'map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'map_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if use_inter_op_parallelism is None:
    use_inter_op_parallelism = True
  use_inter_op_parallelism = _execute.make_bool(use_inter_op_parallelism, "use_inter_op_parallelism")
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapDataset", input_dataset=input_dataset,
                      other_arguments=other_arguments, f=f,
                      output_types=output_types, output_shapes=output_shapes,
                      use_inter_op_parallelism=use_inter_op_parallelism,
                      preserve_cardinality=preserve_cardinality, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "use_inter_op_parallelism",
            _op.get_attr("use_inter_op_parallelism"), "preserve_cardinality",
            _op.get_attr("preserve_cardinality"))
  _execute.record_gradient(
      "MapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def map_dataset_eager_fallback(input_dataset, other_arguments, f, output_types, output_shapes, use_inter_op_parallelism=True, preserve_cardinality=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'map_dataset' Op, not %r." % output_shapes)
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
  _result = _execute.execute(b"MapDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def map_defun(arguments, captured_inputs, output_types, output_shapes, f, name=None):
  r"""  Maps a function on the list of tensors unpacked from arguments on dimension 0.

  Args:
    arguments: A list of `Tensor` objects.
          A list of tensors whose types are `Targuments`, corresponding to the inputs
          the function should be mapped over.
    captured_inputs: A list of `Tensor` objects.
          A list of tensors whose types are `Tcaptured`, corresponding to the captured
          inputs of the defun.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
      A list of types.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
      A list of shapes.
    f: A function decorated with @Defun.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `output_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "MapDefun",
        name, _ctx._post_execution_callbacks, arguments, captured_inputs,
        "output_types", output_types, "output_shapes", output_shapes, "f", f)
      return _result
    except _core._FallbackException:
      try:
        return map_defun_eager_fallback(
            arguments, captured_inputs, output_types=output_types,
            output_shapes=output_shapes, f=f, name=name, ctx=_ctx)
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
        "'map_defun' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'map_defun' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "MapDefun", arguments=arguments, captured_inputs=captured_inputs,
                    output_types=output_types, output_shapes=output_shapes,
                    f=f, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Targuments", _op.get_attr("Targuments"), "Tcaptured",
            _op.get_attr("Tcaptured"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "f", _op.get_attr("f"))
  _execute.record_gradient(
      "MapDefun", _inputs_flat, _attrs, _result, name)
  return _result



def map_defun_eager_fallback(arguments, captured_inputs, output_types, output_shapes, f, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function map_defun
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'map_defun' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'map_defun' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Targuments, arguments = _execute.convert_to_mixed_eager_tensors(arguments, _ctx)
  _attr_Tcaptured, captured_inputs = _execute.convert_to_mixed_eager_tensors(captured_inputs, _ctx)
  _inputs_flat = list(arguments) + list(captured_inputs)
  _attrs = ("Targuments", _attr_Targuments, "Tcaptured", _attr_Tcaptured,
  "output_types", output_types, "output_shapes", output_shapes, "f", f)
  _result = _execute.execute(b"MapDefun", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MapDefun", _inputs_flat, _attrs, _result, name)
  return _result


def model_dataset(input_dataset, output_types, output_shapes, name=None):
  r"""Identity transformation that models performance.

  Identity transformation that models performance.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the input dataset.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "ModelDataset",
        name, _ctx._post_execution_callbacks, input_dataset, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return model_dataset_eager_fallback(
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
        "'model_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'model_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ModelDataset", input_dataset=input_dataset,
                        output_types=output_types,
                        output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ModelDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def model_dataset_eager_fallback(input_dataset, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function model_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'model_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'model_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ModelDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ModelDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def multi_device_iterator(devices, shared_name, container, output_types, output_shapes, name=None):
  r"""Creates a MultiDeviceIterator resource.

  Args:
    devices: A list of `strings` that has length `>= 1`.
      A list of devices the iterator works across.
    shared_name: A `string`.
      If non-empty, this resource will be shared under the given name
      across multiple sessions.
    container: A `string`.
      If non-empty, this resource is placed in the given container.
      Otherwise, a default container is used.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
      The type list for the return values.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
      The list of shapes being produced.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MultiDeviceIterator", name, _ctx._post_execution_callbacks,
        "devices", devices, "shared_name", shared_name, "container",
        container, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return multi_device_iterator_eager_fallback(
            devices=devices, shared_name=shared_name, container=container,
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
  if not isinstance(devices, (list, tuple)):
    raise TypeError(
        "Expected list for 'devices' argument to "
        "'multi_device_iterator' Op, not %r." % devices)
  devices = [_execute.make_str(_s, "devices") for _s in devices]
  shared_name = _execute.make_str(shared_name, "shared_name")
  container = _execute.make_str(container, "container")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'multi_device_iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'multi_device_iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "MultiDeviceIterator", devices=devices, shared_name=shared_name,
                               container=container, output_types=output_types,
                               output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("devices", _op.get_attr("devices"), "shared_name",
            _op.get_attr("shared_name"), "container",
            _op.get_attr("container"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "MultiDeviceIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def multi_device_iterator_eager_fallback(devices, shared_name, container, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function multi_device_iterator
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(devices, (list, tuple)):
    raise TypeError(
        "Expected list for 'devices' argument to "
        "'multi_device_iterator' Op, not %r." % devices)
  devices = [_execute.make_str(_s, "devices") for _s in devices]
  shared_name = _execute.make_str(shared_name, "shared_name")
  container = _execute.make_str(container, "container")
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'multi_device_iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'multi_device_iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _inputs_flat = []
  _attrs = ("devices", devices, "shared_name", shared_name, "container",
  container, "output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"MultiDeviceIterator", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MultiDeviceIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def multi_device_iterator_from_string_handle(string_handle, output_types=[], output_shapes=[], name=None):
  r"""Generates a MultiDeviceIterator resource from its provided string handle.

  Args:
    string_handle: A `Tensor` of type `string`.
      String representing the resource.
    output_types: An optional list of `tf.DTypes`. Defaults to `[]`.
      The type list for the return values.
    output_shapes: An optional list of shapes (each a `tf.TensorShape` or list of `ints`). Defaults to `[]`.
      The list of shapes being produced.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MultiDeviceIteratorFromStringHandle", name,
        _ctx._post_execution_callbacks, string_handle, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return multi_device_iterator_from_string_handle_eager_fallback(
            string_handle, output_types=output_types,
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
  if output_types is None:
    output_types = []
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'multi_device_iterator_from_string_handle' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'multi_device_iterator_from_string_handle' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "MultiDeviceIteratorFromStringHandle", string_handle=string_handle,
                                               output_types=output_types,
                                               output_shapes=output_shapes,
                                               name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "MultiDeviceIteratorFromStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def multi_device_iterator_from_string_handle_eager_fallback(string_handle, output_types=[], output_shapes=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function multi_device_iterator_from_string_handle
  """
  _ctx = ctx if ctx else _context.context()
  if output_types is None:
    output_types = []
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'multi_device_iterator_from_string_handle' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if output_shapes is None:
    output_shapes = []
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'multi_device_iterator_from_string_handle' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  string_handle = _ops.convert_to_tensor(string_handle, _dtypes.string)
  _inputs_flat = [string_handle]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"MultiDeviceIteratorFromStringHandle", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MultiDeviceIteratorFromStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def multi_device_iterator_get_next_from_shard(multi_device_iterator, shard_num, incarnation_id, output_types, output_shapes, name=None):
  r"""Gets next element for the provided shard number.

  Args:
    multi_device_iterator: A `Tensor` of type `resource`.
      A MultiDeviceIterator resource.
    shard_num: A `Tensor` of type `int32`.
      Integer representing which shard to fetch data for.
    incarnation_id: A `Tensor` of type `int64`.
      Which incarnation of the MultiDeviceIterator is running.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
      The type list for the return values.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
      The list of shapes being produced.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `output_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MultiDeviceIteratorGetNextFromShard", name,
        _ctx._post_execution_callbacks, multi_device_iterator, shard_num,
        incarnation_id, "output_types", output_types, "output_shapes",
        output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return multi_device_iterator_get_next_from_shard_eager_fallback(
            multi_device_iterator, shard_num, incarnation_id,
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
        "'multi_device_iterator_get_next_from_shard' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'multi_device_iterator_get_next_from_shard' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "MultiDeviceIteratorGetNextFromShard", multi_device_iterator=multi_device_iterator,
                                               shard_num=shard_num,
                                               incarnation_id=incarnation_id,
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
      "MultiDeviceIteratorGetNextFromShard", _inputs_flat, _attrs, _result, name)
  return _result



def multi_device_iterator_get_next_from_shard_eager_fallback(multi_device_iterator, shard_num, incarnation_id, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function multi_device_iterator_get_next_from_shard
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'multi_device_iterator_get_next_from_shard' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'multi_device_iterator_get_next_from_shard' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  multi_device_iterator = _ops.convert_to_tensor(multi_device_iterator, _dtypes.resource)
  shard_num = _ops.convert_to_tensor(shard_num, _dtypes.int32)
  incarnation_id = _ops.convert_to_tensor(incarnation_id, _dtypes.int64)
  _inputs_flat = [multi_device_iterator, shard_num, incarnation_id]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"MultiDeviceIteratorGetNextFromShard",
                             len(output_types), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MultiDeviceIteratorGetNextFromShard", _inputs_flat, _attrs, _result, name)
  return _result


def multi_device_iterator_init(dataset, multi_device_iterator, max_buffer_size, name=None):
  r"""Initializes the multi device iterator with the given dataset.

  Args:
    dataset: A `Tensor` of type `variant`. Dataset to be iterated upon.
    multi_device_iterator: A `Tensor` of type `resource`.
      A MultiDeviceIteratorResource.
    max_buffer_size: A `Tensor` of type `int64`.
      The maximum size of the host side per device buffer to keep.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MultiDeviceIteratorInit", name, _ctx._post_execution_callbacks,
        dataset, multi_device_iterator, max_buffer_size)
      return _result
    except _core._FallbackException:
      try:
        return multi_device_iterator_init_eager_fallback(
            dataset, multi_device_iterator, max_buffer_size, name=name,
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "MultiDeviceIteratorInit", dataset=dataset,
                                   multi_device_iterator=multi_device_iterator,
                                   max_buffer_size=max_buffer_size, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "MultiDeviceIteratorInit", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def multi_device_iterator_init_eager_fallback(dataset, multi_device_iterator, max_buffer_size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function multi_device_iterator_init
  """
  _ctx = ctx if ctx else _context.context()
  dataset = _ops.convert_to_tensor(dataset, _dtypes.variant)
  multi_device_iterator = _ops.convert_to_tensor(multi_device_iterator, _dtypes.resource)
  max_buffer_size = _ops.convert_to_tensor(max_buffer_size, _dtypes.int64)
  _inputs_flat = [dataset, multi_device_iterator, max_buffer_size]
  _attrs = None
  _result = _execute.execute(b"MultiDeviceIteratorInit", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MultiDeviceIteratorInit", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def multi_device_iterator_to_string_handle(multi_device_iterator, name=None):
  r"""Produces a string handle for the given MultiDeviceIterator.

  Args:
    multi_device_iterator: A `Tensor` of type `resource`.
      A MultiDeviceIterator resource.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MultiDeviceIteratorToStringHandle", name,
        _ctx._post_execution_callbacks, multi_device_iterator)
      return _result
    except _core._FallbackException:
      try:
        return multi_device_iterator_to_string_handle_eager_fallback(
            multi_device_iterator, name=name, ctx=_ctx)
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
        "MultiDeviceIteratorToStringHandle", multi_device_iterator=multi_device_iterator,
                                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "MultiDeviceIteratorToStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def multi_device_iterator_to_string_handle_eager_fallback(multi_device_iterator, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function multi_device_iterator_to_string_handle
  """
  _ctx = ctx if ctx else _context.context()
  multi_device_iterator = _ops.convert_to_tensor(multi_device_iterator, _dtypes.resource)
  _inputs_flat = [multi_device_iterator]
  _attrs = None
  _result = _execute.execute(b"MultiDeviceIteratorToStringHandle", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MultiDeviceIteratorToStringHandle", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def one_shot_iterator(dataset_factory, output_types, output_shapes, container="", shared_name="", name=None):
  r"""Makes a "one-shot" iterator that can be iterated only once.

  A one-shot iterator bundles the logic for defining the dataset and
  the state of the iterator in a single op, which allows simple input
  pipelines to be defined without an additional initialization
  ("MakeIterator") step.

  One-shot iterators have the following limitations:

  * They do not support parameterization: all logic for creating the underlying
    dataset must be bundled in the `dataset_factory` function.
  * They are not resettable. Once a one-shot iterator reaches the end of its
    underlying dataset, subsequent "IteratorGetNext" operations on that
    iterator will always produce an `OutOfRange` error.

  For greater flexibility, use "Iterator" and "MakeIterator" to define
  an iterator using an arbitrary subgraph, which may capture tensors
  (including fed values) as parameters, and which may be reset multiple
  times by rerunning "MakeIterator".

  Args:
    dataset_factory: A function decorated with @Defun.
      A function of type `() -> DT_VARIANT`, where the returned
      DT_VARIANT is a dataset.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
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
        "OneShotIterator", name, _ctx._post_execution_callbacks,
        "dataset_factory", dataset_factory, "output_types", output_types,
        "output_shapes", output_shapes, "container", container, "shared_name",
        shared_name)
      return _result
    except _core._FallbackException:
      try:
        return one_shot_iterator_eager_fallback(
            dataset_factory=dataset_factory, output_types=output_types,
            output_shapes=output_shapes, container=container,
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
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'one_shot_iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'one_shot_iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "OneShotIterator", dataset_factory=dataset_factory,
                           output_types=output_types,
                           output_shapes=output_shapes, container=container,
                           shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dataset_factory", _op.get_attr("dataset_factory"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "container",
            _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "OneShotIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def one_shot_iterator_eager_fallback(dataset_factory, output_types, output_shapes, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function one_shot_iterator
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'one_shot_iterator' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'one_shot_iterator' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("dataset_factory", dataset_factory, "output_types", output_types,
  "output_shapes", output_shapes, "container", container, "shared_name",
  shared_name)
  _result = _execute.execute(b"OneShotIterator", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OneShotIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def optimize_dataset(input_dataset, optimizations, output_types, output_shapes, name=None):
  r"""Creates a dataset by applying optimizations to `input_dataset`.

  Creates a dataset by applying optimizations to `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the input dataset.
    optimizations: A `Tensor` of type `string`.
      A `tf.string` vector `tf.Tensor` identifying optimizations to use.
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
        "OptimizeDataset", name, _ctx._post_execution_callbacks,
        input_dataset, optimizations, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return optimize_dataset_eager_fallback(
            input_dataset, optimizations, output_types=output_types,
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
        "'optimize_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'optimize_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "OptimizeDataset", input_dataset=input_dataset,
                           optimizations=optimizations,
                           output_types=output_types,
                           output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "OptimizeDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def optimize_dataset_eager_fallback(input_dataset, optimizations, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function optimize_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'optimize_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'optimize_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  optimizations = _ops.convert_to_tensor(optimizations, _dtypes.string)
  _inputs_flat = [input_dataset, optimizations]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"OptimizeDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OptimizeDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def optional_from_value(components, name=None):
  r"""Constructs an Optional variant from a tuple of tensors.

  Args:
    components: A list of `Tensor` objects.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OptionalFromValue", name, _ctx._post_execution_callbacks, components)
      return _result
    except _core._FallbackException:
      try:
        return optional_from_value_eager_fallback(
            components, name=name, ctx=_ctx)
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
        "OptionalFromValue", components=components, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Toutput_types", _op.get_attr("Toutput_types"))
  _execute.record_gradient(
      "OptionalFromValue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def optional_from_value_eager_fallback(components, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function optional_from_value
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Toutput_types, components = _execute.convert_to_mixed_eager_tensors(components, _ctx)
  _inputs_flat = list(components)
  _attrs = ("Toutput_types", _attr_Toutput_types)
  _result = _execute.execute(b"OptionalFromValue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OptionalFromValue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def optional_get_value(optional, output_types, output_shapes, name=None):
  r"""Returns the value stored in an Optional variant or raises an error if none exists.

  Args:
    optional: A `Tensor` of type `variant`.
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
        "OptionalGetValue", name, _ctx._post_execution_callbacks, optional,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return optional_get_value_eager_fallback(
            optional, output_types=output_types, output_shapes=output_shapes,
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
        "'optional_get_value' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'optional_get_value' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "OptionalGetValue", optional=optional, output_types=output_types,
                            output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "OptionalGetValue", _inputs_flat, _attrs, _result, name)
  return _result



def optional_get_value_eager_fallback(optional, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function optional_get_value
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'optional_get_value' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'optional_get_value' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  optional = _ops.convert_to_tensor(optional, _dtypes.variant)
  _inputs_flat = [optional]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"OptionalGetValue", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "OptionalGetValue", _inputs_flat, _attrs, _result, name)
  return _result


def optional_has_value(optional, name=None):
  r"""Returns true if and only if the given Optional variant has a value.

  Args:
    optional: A `Tensor` of type `variant`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `bool`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "OptionalHasValue", name, _ctx._post_execution_callbacks, optional)
      return _result
    except _core._FallbackException:
      try:
        return optional_has_value_eager_fallback(
            optional, name=name, ctx=_ctx)
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
        "OptionalHasValue", optional=optional, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "OptionalHasValue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def optional_has_value_eager_fallback(optional, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function optional_has_value
  """
  _ctx = ctx if ctx else _context.context()
  optional = _ops.convert_to_tensor(optional, _dtypes.variant)
  _inputs_flat = [optional]
  _attrs = None
  _result = _execute.execute(b"OptionalHasValue", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OptionalHasValue", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def optional_none(name=None):
  r"""Creates an Optional variant with no value.

  Args:
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "OptionalNone",
        name, _ctx._post_execution_callbacks)
      return _result
    except _core._FallbackException:
      try:
        return optional_none_eager_fallback(
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
  _, _, _op = _op_def_lib._apply_op_helper(
        "OptionalNone", name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "OptionalNone", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def optional_none_eager_fallback(name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function optional_none
  """
  _ctx = ctx if ctx else _context.context()
  _inputs_flat = []
  _attrs = None
  _result = _execute.execute(b"OptionalNone", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "OptionalNone", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def padded_batch_dataset(input_dataset, batch_size, padded_shapes, padding_values, output_shapes, name=None):
  r"""Creates a dataset that batches and pads `batch_size` elements from the input.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    batch_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a
      batch.
    padded_shapes: A list of at least 1 `Tensor` objects with type `int64`.
      A list of int64 tensors representing the desired padded shapes
      of the corresponding output components. These shapes may be partially
      specified, using `-1` to indicate that a particular dimension should be
      padded to the maximum size of all batch elements.
    padding_values: A list of `Tensor` objects.
      A list of scalars containing the padding value to use for
      each of the outputs.
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
        "PaddedBatchDataset", name, _ctx._post_execution_callbacks,
        input_dataset, batch_size, padded_shapes, padding_values,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return padded_batch_dataset_eager_fallback(
            input_dataset, batch_size, padded_shapes, padding_values,
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
  if not isinstance(padded_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'padded_shapes' argument to "
        "'padded_batch_dataset' Op, not %r." % padded_shapes)
  _attr_N = len(padded_shapes)
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'padded_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "PaddedBatchDataset", input_dataset=input_dataset,
                              batch_size=batch_size,
                              padded_shapes=padded_shapes,
                              padding_values=padding_values,
                              output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Toutput_types", _op.get_attr("Toutput_types"), "output_shapes",
            _op.get_attr("output_shapes"), "N", _op.get_attr("N"))
  _execute.record_gradient(
      "PaddedBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def padded_batch_dataset_eager_fallback(input_dataset, batch_size, padded_shapes, padding_values, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function padded_batch_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(padded_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'padded_shapes' argument to "
        "'padded_batch_dataset' Op, not %r." % padded_shapes)
  _attr_N = len(padded_shapes)
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'padded_batch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Toutput_types, padding_values = _execute.convert_to_mixed_eager_tensors(padding_values, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  batch_size = _ops.convert_to_tensor(batch_size, _dtypes.int64)
  padded_shapes = _ops.convert_n_to_tensor(padded_shapes, _dtypes.int64)
  _inputs_flat = [input_dataset, batch_size] + list(padded_shapes) + list(padding_values)
  _attrs = ("Toutput_types", _attr_Toutput_types, "output_shapes",
  output_shapes, "N", _attr_N)
  _result = _execute.execute(b"PaddedBatchDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PaddedBatchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def padded_batch_dataset_v2(input_dataset, batch_size, padded_shapes, padding_values, drop_remainder, output_shapes, name=None):
  r"""Creates a dataset that batches and pads `batch_size` elements from the input.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    batch_size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a
      batch.
    padded_shapes: A list of at least 1 `Tensor` objects with type `int64`.
      A list of int64 tensors representing the desired padded shapes
      of the corresponding output components. These shapes may be partially
      specified, using `-1` to indicate that a particular dimension should be
      padded to the maximum size of all batch elements.
    padding_values: A list of `Tensor` objects.
      A list of scalars containing the padding value to use for
      each of the outputs.
    drop_remainder: A `Tensor` of type `bool`.
      A scalar representing whether the last batch should be dropped in case its size
      is smaller than desired.
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
        "PaddedBatchDatasetV2", name, _ctx._post_execution_callbacks,
        input_dataset, batch_size, padded_shapes, padding_values,
        drop_remainder, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return padded_batch_dataset_v2_eager_fallback(
            input_dataset, batch_size, padded_shapes, padding_values,
            drop_remainder, output_shapes=output_shapes, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if not isinstance(padded_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'padded_shapes' argument to "
        "'padded_batch_dataset_v2' Op, not %r." % padded_shapes)
  _attr_N = len(padded_shapes)
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'padded_batch_dataset_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "PaddedBatchDatasetV2", input_dataset=input_dataset,
                                batch_size=batch_size,
                                padded_shapes=padded_shapes,
                                padding_values=padding_values,
                                drop_remainder=drop_remainder,
                                output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Toutput_types", _op.get_attr("Toutput_types"), "output_shapes",
            _op.get_attr("output_shapes"), "N", _op.get_attr("N"))
  _execute.record_gradient(
      "PaddedBatchDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def padded_batch_dataset_v2_eager_fallback(input_dataset, batch_size, padded_shapes, padding_values, drop_remainder, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function padded_batch_dataset_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(padded_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'padded_shapes' argument to "
        "'padded_batch_dataset_v2' Op, not %r." % padded_shapes)
  _attr_N = len(padded_shapes)
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'padded_batch_dataset_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Toutput_types, padding_values = _execute.convert_to_mixed_eager_tensors(padding_values, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  batch_size = _ops.convert_to_tensor(batch_size, _dtypes.int64)
  padded_shapes = _ops.convert_n_to_tensor(padded_shapes, _dtypes.int64)
  drop_remainder = _ops.convert_to_tensor(drop_remainder, _dtypes.bool)
  _inputs_flat = [input_dataset, batch_size] + list(padded_shapes) + list(padding_values) + [drop_remainder]
  _attrs = ("Toutput_types", _attr_Toutput_types, "output_shapes",
  output_shapes, "N", _attr_N)
  _result = _execute.execute(b"PaddedBatchDatasetV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PaddedBatchDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def parallel_interleave_dataset_v2(input_dataset, other_arguments, cycle_length, block_length, num_parallel_calls, f, output_types, output_shapes, sloppy=False, name=None):
  r"""Creates a dataset that applies `f` to the outputs of `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    other_arguments: A list of `Tensor` objects.
    cycle_length: A `Tensor` of type `int64`.
    block_length: A `Tensor` of type `int64`.
    num_parallel_calls: A `Tensor` of type `int64`.
    f: A function decorated with @Defun.
      A function mapping elements of `input_dataset`, concatenated with
      `other_arguments`, to a Dataset variant that contains elements matching
      `output_types` and `output_shapes`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
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
        "ParallelInterleaveDatasetV2", name, _ctx._post_execution_callbacks,
        input_dataset, other_arguments, cycle_length, block_length,
        num_parallel_calls, "f", f, "output_types", output_types,
        "output_shapes", output_shapes, "sloppy", sloppy)
      return _result
    except _core._FallbackException:
      try:
        return parallel_interleave_dataset_v2_eager_fallback(
            input_dataset, other_arguments, cycle_length, block_length,
            num_parallel_calls, f=f, output_types=output_types,
            output_shapes=output_shapes, sloppy=sloppy, name=name, ctx=_ctx)
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
        "'parallel_interleave_dataset_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'parallel_interleave_dataset_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if sloppy is None:
    sloppy = False
  sloppy = _execute.make_bool(sloppy, "sloppy")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ParallelInterleaveDatasetV2", input_dataset=input_dataset,
                                       other_arguments=other_arguments,
                                       cycle_length=cycle_length,
                                       block_length=block_length,
                                       num_parallel_calls=num_parallel_calls,
                                       f=f, output_types=output_types,
                                       output_shapes=output_shapes,
                                       sloppy=sloppy, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "sloppy", _op.get_attr("sloppy"))
  _execute.record_gradient(
      "ParallelInterleaveDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def parallel_interleave_dataset_v2_eager_fallback(input_dataset, other_arguments, cycle_length, block_length, num_parallel_calls, f, output_types, output_shapes, sloppy=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function parallel_interleave_dataset_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'parallel_interleave_dataset_v2' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'parallel_interleave_dataset_v2' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if sloppy is None:
    sloppy = False
  sloppy = _execute.make_bool(sloppy, "sloppy")
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  cycle_length = _ops.convert_to_tensor(cycle_length, _dtypes.int64)
  block_length = _ops.convert_to_tensor(block_length, _dtypes.int64)
  num_parallel_calls = _ops.convert_to_tensor(num_parallel_calls, _dtypes.int64)
  _inputs_flat = [input_dataset] + list(other_arguments) + [cycle_length, block_length, num_parallel_calls]
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes, "sloppy", sloppy)
  _result = _execute.execute(b"ParallelInterleaveDatasetV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ParallelInterleaveDatasetV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def parallel_map_dataset(input_dataset, other_arguments, num_parallel_calls, f, output_types, output_shapes, use_inter_op_parallelism=True, sloppy=False, preserve_cardinality=False, name=None):
  r"""Creates a dataset that applies `f` to the outputs of `input_dataset`.

  Unlike a "MapDataset", which applies `f` sequentially, this dataset invokes up
  to `num_parallel_calls` copies of `f` in parallel.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    other_arguments: A list of `Tensor` objects.
    num_parallel_calls: A `Tensor` of type `int32`.
      The number of concurrent invocations of `f` that process
      elements from `input_dataset` in parallel.
    f: A function decorated with @Defun.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    use_inter_op_parallelism: An optional `bool`. Defaults to `True`.
    sloppy: An optional `bool`. Defaults to `False`.
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
        "ParallelMapDataset", name, _ctx._post_execution_callbacks,
        input_dataset, other_arguments, num_parallel_calls, "f", f,
        "output_types", output_types, "output_shapes", output_shapes,
        "use_inter_op_parallelism", use_inter_op_parallelism, "sloppy",
        sloppy, "preserve_cardinality", preserve_cardinality)
      return _result
    except _core._FallbackException:
      try:
        return parallel_map_dataset_eager_fallback(
            input_dataset, other_arguments, num_parallel_calls, f=f,
            output_types=output_types, output_shapes=output_shapes,
            use_inter_op_parallelism=use_inter_op_parallelism, sloppy=sloppy,
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
        "'parallel_map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'parallel_map_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if use_inter_op_parallelism is None:
    use_inter_op_parallelism = True
  use_inter_op_parallelism = _execute.make_bool(use_inter_op_parallelism, "use_inter_op_parallelism")
  if sloppy is None:
    sloppy = False
  sloppy = _execute.make_bool(sloppy, "sloppy")
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ParallelMapDataset", input_dataset=input_dataset,
                              other_arguments=other_arguments,
                              num_parallel_calls=num_parallel_calls, f=f,
                              output_types=output_types,
                              output_shapes=output_shapes,
                              use_inter_op_parallelism=use_inter_op_parallelism,
                              sloppy=sloppy,
                              preserve_cardinality=preserve_cardinality,
                              name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Targuments", _op.get_attr("Targuments"),
            "output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "use_inter_op_parallelism",
            _op.get_attr("use_inter_op_parallelism"), "sloppy",
            _op.get_attr("sloppy"), "preserve_cardinality",
            _op.get_attr("preserve_cardinality"))
  _execute.record_gradient(
      "ParallelMapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def parallel_map_dataset_eager_fallback(input_dataset, other_arguments, num_parallel_calls, f, output_types, output_shapes, use_inter_op_parallelism=True, sloppy=False, preserve_cardinality=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function parallel_map_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'parallel_map_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'parallel_map_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if use_inter_op_parallelism is None:
    use_inter_op_parallelism = True
  use_inter_op_parallelism = _execute.make_bool(use_inter_op_parallelism, "use_inter_op_parallelism")
  if sloppy is None:
    sloppy = False
  sloppy = _execute.make_bool(sloppy, "sloppy")
  if preserve_cardinality is None:
    preserve_cardinality = False
  preserve_cardinality = _execute.make_bool(preserve_cardinality, "preserve_cardinality")
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  num_parallel_calls = _ops.convert_to_tensor(num_parallel_calls, _dtypes.int32)
  _inputs_flat = [input_dataset] + list(other_arguments) + [num_parallel_calls]
  _attrs = ("f", f, "Targuments", _attr_Targuments, "output_types",
  output_types, "output_shapes", output_shapes, "use_inter_op_parallelism",
  use_inter_op_parallelism, "sloppy", sloppy, "preserve_cardinality",
  preserve_cardinality)
  _result = _execute.execute(b"ParallelMapDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ParallelMapDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def prefetch_dataset(input_dataset, buffer_size, output_types, output_shapes, name=None):
  r"""Creates a dataset that asynchronously prefetches elements from `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    buffer_size: A `Tensor` of type `int64`.
      The maximum number of elements to buffer in an iterator over
      this dataset.
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
        "PrefetchDataset", name, _ctx._post_execution_callbacks,
        input_dataset, buffer_size, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return prefetch_dataset_eager_fallback(
            input_dataset, buffer_size, output_types=output_types,
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
        "'prefetch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'prefetch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "PrefetchDataset", input_dataset=input_dataset,
                           buffer_size=buffer_size, output_types=output_types,
                           output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "PrefetchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def prefetch_dataset_eager_fallback(input_dataset, buffer_size, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function prefetch_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'prefetch_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'prefetch_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  _inputs_flat = [input_dataset, buffer_size]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"PrefetchDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "PrefetchDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def range_dataset(start, stop, step, output_types, output_shapes, name=None):
  r"""Creates a dataset with a range of values. Corresponds to python's xrange.

  Args:
    start: A `Tensor` of type `int64`.
      corresponds to start in python's xrange().
    stop: A `Tensor` of type `int64`.
      corresponds to stop in python's xrange().
    step: A `Tensor` of type `int64`.
      corresponds to step in python's xrange().
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
        _ctx._context_handle, _ctx._eager_context.device_name, "RangeDataset",
        name, _ctx._post_execution_callbacks, start, stop, step,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return range_dataset_eager_fallback(
            start, stop, step, output_types=output_types,
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
        "'range_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'range_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "RangeDataset", start=start, stop=stop, step=step,
                        output_types=output_types,
                        output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "RangeDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def range_dataset_eager_fallback(start, stop, step, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function range_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'range_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'range_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  start = _ops.convert_to_tensor(start, _dtypes.int64)
  stop = _ops.convert_to_tensor(stop, _dtypes.int64)
  step = _ops.convert_to_tensor(step, _dtypes.int64)
  _inputs_flat = [start, stop, step]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"RangeDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RangeDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def reduce_dataset(input_dataset, initial_state, other_arguments, f, output_types, output_shapes, use_inter_op_parallelism=True, name=None):
  r"""Reduces the input dataset to a singleton using a reduce function.

  Args:
    input_dataset: A `Tensor` of type `variant`.
      A variant tensor representing the input dataset.
    initial_state: A list of `Tensor` objects.
      A nested structure of tensors, representing the initial state of the
      transformation.
    other_arguments: A list of `Tensor` objects.
    f: A function decorated with @Defun.
      A function that maps `(old_state, input_element)` to `new_state`. It must take
      two arguments and return a nested structures of tensors. The structure of
      `new_state` must match the structure of `initial_state`.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    use_inter_op_parallelism: An optional `bool`. Defaults to `True`.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `output_types`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReduceDataset", name, _ctx._post_execution_callbacks, input_dataset,
        initial_state, other_arguments, "f", f, "output_types", output_types,
        "output_shapes", output_shapes, "use_inter_op_parallelism",
        use_inter_op_parallelism)
      return _result
    except _core._FallbackException:
      try:
        return reduce_dataset_eager_fallback(
            input_dataset, initial_state, other_arguments, f=f,
            output_types=output_types, output_shapes=output_shapes,
            use_inter_op_parallelism=use_inter_op_parallelism, name=name,
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
        "'reduce_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'reduce_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if use_inter_op_parallelism is None:
    use_inter_op_parallelism = True
  use_inter_op_parallelism = _execute.make_bool(use_inter_op_parallelism, "use_inter_op_parallelism")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReduceDataset", input_dataset=input_dataset,
                         initial_state=initial_state,
                         other_arguments=other_arguments, f=f,
                         output_types=output_types,
                         output_shapes=output_shapes,
                         use_inter_op_parallelism=use_inter_op_parallelism,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("f", _op.get_attr("f"), "Tstate", _op.get_attr("Tstate"),
            "Targuments", _op.get_attr("Targuments"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "use_inter_op_parallelism",
            _op.get_attr("use_inter_op_parallelism"))
  _execute.record_gradient(
      "ReduceDataset", _inputs_flat, _attrs, _result, name)
  return _result



def reduce_dataset_eager_fallback(input_dataset, initial_state, other_arguments, f, output_types, output_shapes, use_inter_op_parallelism=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reduce_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'reduce_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'reduce_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if use_inter_op_parallelism is None:
    use_inter_op_parallelism = True
  use_inter_op_parallelism = _execute.make_bool(use_inter_op_parallelism, "use_inter_op_parallelism")
  _attr_Tstate, initial_state = _execute.convert_to_mixed_eager_tensors(initial_state, _ctx)
  _attr_Targuments, other_arguments = _execute.convert_to_mixed_eager_tensors(other_arguments, _ctx)
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  _inputs_flat = [input_dataset] + list(initial_state) + list(other_arguments)
  _attrs = ("f", f, "Tstate", _attr_Tstate, "Targuments", _attr_Targuments,
  "output_types", output_types, "output_shapes", output_shapes,
  "use_inter_op_parallelism", use_inter_op_parallelism)
  _result = _execute.execute(b"ReduceDataset", len(output_types),
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ReduceDataset", _inputs_flat, _attrs, _result, name)
  return _result


def repeat_dataset(input_dataset, count, output_types, output_shapes, name=None):
  r"""Creates a dataset that emits the outputs of `input_dataset` `count` times.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    count: A `Tensor` of type `int64`.
      A scalar representing the number of times that `input_dataset` should
      be repeated. A value of `-1` indicates that it should be repeated infinitely.
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
        "RepeatDataset", name, _ctx._post_execution_callbacks, input_dataset,
        count, "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return repeat_dataset_eager_fallback(
            input_dataset, count, output_types=output_types,
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
        "'repeat_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'repeat_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "RepeatDataset", input_dataset=input_dataset, count=count,
                         output_types=output_types,
                         output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "RepeatDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def repeat_dataset_eager_fallback(input_dataset, count, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function repeat_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'repeat_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'repeat_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  count = _ops.convert_to_tensor(count, _dtypes.int64)
  _inputs_flat = [input_dataset, count]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"RepeatDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RepeatDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def serialize_iterator(resource_handle, name=None):
  r"""Converts the given `resource_handle` representing an iterator to a variant tensor.

  Args:
    resource_handle: A `Tensor` of type `resource`.
      A handle to an iterator resource.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SerializeIterator", name, _ctx._post_execution_callbacks,
        resource_handle)
      return _result
    except _core._FallbackException:
      try:
        return serialize_iterator_eager_fallback(
            resource_handle, name=name, ctx=_ctx)
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
        "SerializeIterator", resource_handle=resource_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "SerializeIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def serialize_iterator_eager_fallback(resource_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function serialize_iterator
  """
  _ctx = ctx if ctx else _context.context()
  resource_handle = _ops.convert_to_tensor(resource_handle, _dtypes.resource)
  _inputs_flat = [resource_handle]
  _attrs = None
  _result = _execute.execute(b"SerializeIterator", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SerializeIterator", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def shuffle_and_repeat_dataset(input_dataset, buffer_size, seed, seed2, count, output_types, output_shapes, name=None):
  r"""Creates a dataset that shuffles and repeats elements from `input_dataset`

  pseudorandomly.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    buffer_size: A `Tensor` of type `int64`.
      The number of output elements to buffer in an iterator over
      this dataset. Compare with the `min_after_dequeue` attr when creating a
      `RandomShuffleQueue`.
    seed: A `Tensor` of type `int64`.
      A scalar seed for the random number generator. If either `seed` or
      `seed2` is set to be non-zero, the random number generator is seeded
      by the given seed.  Otherwise, a random seed is used.
    seed2: A `Tensor` of type `int64`.
      A second scalar seed to avoid seed collision.
    count: A `Tensor` of type `int64`.
      A scalar representing the number of times the underlying dataset
      should be repeated. The default is `-1`, which results in infinite repetition.
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
        "ShuffleAndRepeatDataset", name, _ctx._post_execution_callbacks,
        input_dataset, buffer_size, seed, seed2, count, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return shuffle_and_repeat_dataset_eager_fallback(
            input_dataset, buffer_size, seed, seed2, count,
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
        "'shuffle_and_repeat_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'shuffle_and_repeat_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ShuffleAndRepeatDataset", input_dataset=input_dataset,
                                   buffer_size=buffer_size, seed=seed,
                                   seed2=seed2, count=count,
                                   output_types=output_types,
                                   output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ShuffleAndRepeatDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def shuffle_and_repeat_dataset_eager_fallback(input_dataset, buffer_size, seed, seed2, count, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function shuffle_and_repeat_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'shuffle_and_repeat_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'shuffle_and_repeat_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  seed = _ops.convert_to_tensor(seed, _dtypes.int64)
  seed2 = _ops.convert_to_tensor(seed2, _dtypes.int64)
  count = _ops.convert_to_tensor(count, _dtypes.int64)
  _inputs_flat = [input_dataset, buffer_size, seed, seed2, count]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ShuffleAndRepeatDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ShuffleAndRepeatDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def shuffle_dataset(input_dataset, buffer_size, seed, seed2, output_types, output_shapes, reshuffle_each_iteration=True, name=None):
  r"""Creates a dataset that shuffles elements from `input_dataset` pseudorandomly.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    buffer_size: A `Tensor` of type `int64`.
      The number of output elements to buffer in an iterator over
      this dataset. Compare with the `min_after_dequeue` attr when creating a
      `RandomShuffleQueue`.
    seed: A `Tensor` of type `int64`.
      A scalar seed for the random number generator. If either `seed` or
      `seed2` is set to be non-zero, the random number generator is seeded
      by the given seed.  Otherwise, a random seed is used.
    seed2: A `Tensor` of type `int64`.
      A second scalar seed to avoid seed collision.
    output_types: A list of `tf.DTypes` that has length `>= 1`.
    output_shapes: A list of shapes (each a `tf.TensorShape` or list of `ints`) that has length `>= 1`.
    reshuffle_each_iteration: An optional `bool`. Defaults to `True`.
      If true, each iterator over this dataset will be given
      a different pseudorandomly generated seed, based on a sequence seeded by the
      `seed` and `seed2` inputs. If false, each iterator will be given the same
      seed, and repeated iteration over this dataset will yield the exact same
      sequence of results.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ShuffleDataset", name, _ctx._post_execution_callbacks, input_dataset,
        buffer_size, seed, seed2, "reshuffle_each_iteration",
        reshuffle_each_iteration, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return shuffle_dataset_eager_fallback(
            input_dataset, buffer_size, seed, seed2,
            reshuffle_each_iteration=reshuffle_each_iteration,
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
        "'shuffle_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'shuffle_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if reshuffle_each_iteration is None:
    reshuffle_each_iteration = True
  reshuffle_each_iteration = _execute.make_bool(reshuffle_each_iteration, "reshuffle_each_iteration")
  _, _, _op = _op_def_lib._apply_op_helper(
        "ShuffleDataset", input_dataset=input_dataset,
                          buffer_size=buffer_size, seed=seed, seed2=seed2,
                          output_types=output_types,
                          output_shapes=output_shapes,
                          reshuffle_each_iteration=reshuffle_each_iteration,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("reshuffle_each_iteration",
            _op.get_attr("reshuffle_each_iteration"), "output_types",
            _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "ShuffleDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def shuffle_dataset_eager_fallback(input_dataset, buffer_size, seed, seed2, output_types, output_shapes, reshuffle_each_iteration=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function shuffle_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'shuffle_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'shuffle_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  if reshuffle_each_iteration is None:
    reshuffle_each_iteration = True
  reshuffle_each_iteration = _execute.make_bool(reshuffle_each_iteration, "reshuffle_each_iteration")
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  seed = _ops.convert_to_tensor(seed, _dtypes.int64)
  seed2 = _ops.convert_to_tensor(seed2, _dtypes.int64)
  _inputs_flat = [input_dataset, buffer_size, seed, seed2]
  _attrs = ("reshuffle_each_iteration", reshuffle_each_iteration,
  "output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"ShuffleDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ShuffleDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def skip_dataset(input_dataset, count, output_types, output_shapes, name=None):
  r"""Creates a dataset that skips `count` elements from the `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    count: A `Tensor` of type `int64`.
      A scalar representing the number of elements from the `input_dataset`
      that should be skipped.  If count is -1, skips everything.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "SkipDataset",
        name, _ctx._post_execution_callbacks, input_dataset, count,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return skip_dataset_eager_fallback(
            input_dataset, count, output_types=output_types,
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
        "'skip_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'skip_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "SkipDataset", input_dataset=input_dataset, count=count,
                       output_types=output_types, output_shapes=output_shapes,
                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "SkipDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def skip_dataset_eager_fallback(input_dataset, count, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function skip_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'skip_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'skip_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  count = _ops.convert_to_tensor(count, _dtypes.int64)
  _inputs_flat = [input_dataset, count]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"SkipDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "SkipDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sparse_tensor_slice_dataset(indices, values, dense_shape, name=None):
  r"""Creates a dataset that splits a SparseTensor into elements row-wise.

  Args:
    indices: A `Tensor` of type `int64`.
    values: A `Tensor`.
    dense_shape: A `Tensor` of type `int64`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "SparseTensorSliceDataset", name, _ctx._post_execution_callbacks,
        indices, values, dense_shape)
      return _result
    except _core._FallbackException:
      try:
        return sparse_tensor_slice_dataset_eager_fallback(
            indices, values, dense_shape, name=name, ctx=_ctx)
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
        "SparseTensorSliceDataset", indices=indices, values=values,
                                    dense_shape=dense_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tvalues", _op.get_attr("Tvalues"))
  _execute.record_gradient(
      "SparseTensorSliceDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sparse_tensor_slice_dataset_eager_fallback(indices, values, dense_shape, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sparse_tensor_slice_dataset
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tvalues, (values,) = _execute.args_to_matching_eager([values], _ctx)
  indices = _ops.convert_to_tensor(indices, _dtypes.int64)
  dense_shape = _ops.convert_to_tensor(dense_shape, _dtypes.int64)
  _inputs_flat = [indices, values, dense_shape]
  _attrs = ("Tvalues", _attr_Tvalues)
  _result = _execute.execute(b"SparseTensorSliceDataset", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "SparseTensorSliceDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tf_record_dataset(filenames, compression_type, buffer_size, name=None):
  r"""Creates a dataset that emits the records from one or more TFRecord files.

  Args:
    filenames: A `Tensor` of type `string`.
      A scalar or vector containing the name(s) of the file(s) to be
      read.
    compression_type: A `Tensor` of type `string`.
      A scalar containing either (i) the empty string (no
      compression), (ii) "ZLIB", or (iii) "GZIP".
    buffer_size: A `Tensor` of type `int64`.
      A scalar representing the number of bytes to buffer. A value of
      0 means no buffering will be performed.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TFRecordDataset", name, _ctx._post_execution_callbacks, filenames,
        compression_type, buffer_size)
      return _result
    except _core._FallbackException:
      try:
        return tf_record_dataset_eager_fallback(
            filenames, compression_type, buffer_size, name=name, ctx=_ctx)
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
        "TFRecordDataset", filenames=filenames,
                           compression_type=compression_type,
                           buffer_size=buffer_size, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TFRecordDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tf_record_dataset_eager_fallback(filenames, compression_type, buffer_size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tf_record_dataset
  """
  _ctx = ctx if ctx else _context.context()
  filenames = _ops.convert_to_tensor(filenames, _dtypes.string)
  compression_type = _ops.convert_to_tensor(compression_type, _dtypes.string)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  _inputs_flat = [filenames, compression_type, buffer_size]
  _attrs = None
  _result = _execute.execute(b"TFRecordDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TFRecordDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def take_dataset(input_dataset, count, output_types, output_shapes, name=None):
  r"""Creates a dataset that contains `count` elements from the `input_dataset`.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    count: A `Tensor` of type `int64`.
      A scalar representing the number of elements from the `input_dataset`
      that should be taken. A value of `-1` indicates that all of `input_dataset`
      is taken.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "TakeDataset",
        name, _ctx._post_execution_callbacks, input_dataset, count,
        "output_types", output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return take_dataset_eager_fallback(
            input_dataset, count, output_types=output_types,
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
        "'take_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'take_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "TakeDataset", input_dataset=input_dataset, count=count,
                       output_types=output_types, output_shapes=output_shapes,
                       name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "TakeDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def take_dataset_eager_fallback(input_dataset, count, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function take_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'take_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'take_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  count = _ops.convert_to_tensor(count, _dtypes.int64)
  _inputs_flat = [input_dataset, count]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"TakeDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TakeDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_dataset(components, output_shapes, name=None):
  r"""Creates a dataset that emits `components` as a tuple of tensors once.

  Args:
    components: A list of `Tensor` objects.
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
        "TensorDataset", name, _ctx._post_execution_callbacks, components,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return tensor_dataset_eager_fallback(
            components, output_shapes=output_shapes, name=name, ctx=_ctx)
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
        "'tensor_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorDataset", components=components, output_shapes=output_shapes,
                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Toutput_types", _op.get_attr("Toutput_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "TensorDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_dataset_eager_fallback(components, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'tensor_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Toutput_types, components = _execute.convert_to_mixed_eager_tensors(components, _ctx)
  _inputs_flat = list(components)
  _attrs = ("Toutput_types", _attr_Toutput_types, "output_shapes",
  output_shapes)
  _result = _execute.execute(b"TensorDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tensor_slice_dataset(components, output_shapes, name=None):
  r"""Creates a dataset that emits each dim-0 slice of `components` once.

  Args:
    components: A list of `Tensor` objects.
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
        "TensorSliceDataset", name, _ctx._post_execution_callbacks,
        components, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return tensor_slice_dataset_eager_fallback(
            components, output_shapes=output_shapes, name=name, ctx=_ctx)
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
        "'tensor_slice_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "TensorSliceDataset", components=components,
                              output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Toutput_types", _op.get_attr("Toutput_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "TensorSliceDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tensor_slice_dataset_eager_fallback(components, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tensor_slice_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'tensor_slice_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _attr_Toutput_types, components = _execute.convert_to_mixed_eager_tensors(components, _ctx)
  _inputs_flat = list(components)
  _attrs = ("Toutput_types", _attr_Toutput_types, "output_shapes",
  output_shapes)
  _result = _execute.execute(b"TensorSliceDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TensorSliceDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def text_line_dataset(filenames, compression_type, buffer_size, name=None):
  r"""Creates a dataset that emits the lines of one or more text files.

  Args:
    filenames: A `Tensor` of type `string`.
      A scalar or a vector containing the name(s) of the file(s) to be
      read.
    compression_type: A `Tensor` of type `string`.
      A scalar containing either (i) the empty string (no
      compression), (ii) "ZLIB", or (iii) "GZIP".
    buffer_size: A `Tensor` of type `int64`.
      A scalar containing the number of bytes to buffer.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TextLineDataset", name, _ctx._post_execution_callbacks, filenames,
        compression_type, buffer_size)
      return _result
    except _core._FallbackException:
      try:
        return text_line_dataset_eager_fallback(
            filenames, compression_type, buffer_size, name=name, ctx=_ctx)
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
        "TextLineDataset", filenames=filenames,
                           compression_type=compression_type,
                           buffer_size=buffer_size, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "TextLineDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def text_line_dataset_eager_fallback(filenames, compression_type, buffer_size, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function text_line_dataset
  """
  _ctx = ctx if ctx else _context.context()
  filenames = _ops.convert_to_tensor(filenames, _dtypes.string)
  compression_type = _ops.convert_to_tensor(compression_type, _dtypes.string)
  buffer_size = _ops.convert_to_tensor(buffer_size, _dtypes.int64)
  _inputs_flat = [filenames, compression_type, buffer_size]
  _attrs = None
  _result = _execute.execute(b"TextLineDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TextLineDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def unwrap_dataset_variant(input_handle, name=None):
  r"""TODO: add doc.

  Args:
    input_handle: A `Tensor` of type `variant`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "UnwrapDatasetVariant", name, _ctx._post_execution_callbacks,
        input_handle)
      return _result
    except _core._FallbackException:
      try:
        return unwrap_dataset_variant_eager_fallback(
            input_handle, name=name, ctx=_ctx)
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
        "UnwrapDatasetVariant", input_handle=input_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "UnwrapDatasetVariant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def unwrap_dataset_variant_eager_fallback(input_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function unwrap_dataset_variant
  """
  _ctx = ctx if ctx else _context.context()
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  _inputs_flat = [input_handle]
  _attrs = None
  _result = _execute.execute(b"UnwrapDatasetVariant", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "UnwrapDatasetVariant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def window_dataset(input_dataset, size, shift, stride, drop_remainder, output_types, output_shapes, name=None):
  r"""A dataset that creates window datasets from the input dataset.

  Args:
    input_dataset: A `Tensor` of type `variant`.
    size: A `Tensor` of type `int64`.
      A scalar representing the number of elements to accumulate in a window.
    shift: A `Tensor` of type `int64`.
      A scalar representing the steps moving the sliding window forward in one
      iteration. It must be positive.
    stride: A `Tensor` of type `int64`.
      A scalar representing the stride of the input elements of the sliding window.
      It must be positive.
    drop_remainder: A `Tensor` of type `bool`.
      A scalar representing whether a window should be dropped in case its size is
      smaller than desired.
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
        "WindowDataset", name, _ctx._post_execution_callbacks, input_dataset,
        size, shift, stride, drop_remainder, "output_types", output_types,
        "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return window_dataset_eager_fallback(
            input_dataset, size, shift, stride, drop_remainder,
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
        "'window_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'window_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "WindowDataset", input_dataset=input_dataset, size=size, shift=shift,
                         stride=stride, drop_remainder=drop_remainder,
                         output_types=output_types,
                         output_shapes=output_shapes, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"))
  _execute.record_gradient(
      "WindowDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def window_dataset_eager_fallback(input_dataset, size, shift, stride, drop_remainder, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function window_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'window_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'window_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_dataset = _ops.convert_to_tensor(input_dataset, _dtypes.variant)
  size = _ops.convert_to_tensor(size, _dtypes.int64)
  shift = _ops.convert_to_tensor(shift, _dtypes.int64)
  stride = _ops.convert_to_tensor(stride, _dtypes.int64)
  drop_remainder = _ops.convert_to_tensor(drop_remainder, _dtypes.bool)
  _inputs_flat = [input_dataset, size, shift, stride, drop_remainder]
  _attrs = ("output_types", output_types, "output_shapes", output_shapes)
  _result = _execute.execute(b"WindowDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "WindowDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def wrap_dataset_variant(input_handle, name=None):
  r"""TODO: add doc.

  Args:
    input_handle: A `Tensor` of type `variant`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `variant`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "WrapDatasetVariant", name, _ctx._post_execution_callbacks,
        input_handle)
      return _result
    except _core._FallbackException:
      try:
        return wrap_dataset_variant_eager_fallback(
            input_handle, name=name, ctx=_ctx)
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
        "WrapDatasetVariant", input_handle=input_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "WrapDatasetVariant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def wrap_dataset_variant_eager_fallback(input_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function wrap_dataset_variant
  """
  _ctx = ctx if ctx else _context.context()
  input_handle = _ops.convert_to_tensor(input_handle, _dtypes.variant)
  _inputs_flat = [input_handle]
  _attrs = None
  _result = _execute.execute(b"WrapDatasetVariant", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "WrapDatasetVariant", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def zip_dataset(input_datasets, output_types, output_shapes, name=None):
  r"""Creates a dataset that zips together `input_datasets`.

  Args:
    input_datasets: A list of at least 1 `Tensor` objects with type `variant`.
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
        _ctx._context_handle, _ctx._eager_context.device_name, "ZipDataset",
        name, _ctx._post_execution_callbacks, input_datasets, "output_types",
        output_types, "output_shapes", output_shapes)
      return _result
    except _core._FallbackException:
      try:
        return zip_dataset_eager_fallback(
            input_datasets, output_types=output_types,
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
  if not isinstance(input_datasets, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_datasets' argument to "
        "'zip_dataset' Op, not %r." % input_datasets)
  _attr_N = len(input_datasets)
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'zip_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'zip_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "ZipDataset", input_datasets=input_datasets,
                      output_types=output_types, output_shapes=output_shapes,
                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("output_types", _op.get_attr("output_types"), "output_shapes",
            _op.get_attr("output_shapes"), "N", _op.get_attr("N"))
  _execute.record_gradient(
      "ZipDataset", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def zip_dataset_eager_fallback(input_datasets, output_types, output_shapes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function zip_dataset
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(input_datasets, (list, tuple)):
    raise TypeError(
        "Expected list for 'input_datasets' argument to "
        "'zip_dataset' Op, not %r." % input_datasets)
  _attr_N = len(input_datasets)
  if not isinstance(output_types, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_types' argument to "
        "'zip_dataset' Op, not %r." % output_types)
  output_types = [_execute.make_type(_t, "output_types") for _t in output_types]
  if not isinstance(output_shapes, (list, tuple)):
    raise TypeError(
        "Expected list for 'output_shapes' argument to "
        "'zip_dataset' Op, not %r." % output_shapes)
  output_shapes = [_execute.make_shape(_s, "output_shapes") for _s in output_shapes]
  input_datasets = _ops.convert_n_to_tensor(input_datasets, _dtypes.variant)
  _inputs_flat = list(input_datasets)
  _attrs = ("output_types", output_types, "output_shapes", output_shapes, "N",
  _attr_N)
  _result = _execute.execute(b"ZipDataset", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ZipDataset", _inputs_flat, _attrs, _result, name)
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
#   name: "AnonymousIterator"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
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
#   name: "BatchDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "batch_size"
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
#   name: "BatchDatasetV2"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "batch_size"
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
#   name: "CacheDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "filename"
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
#   name: "ConcatenateDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "another_dataset"
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
#   name: "DatasetToGraph"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "graph"
#     type: DT_STRING
#   }
# }
# op {
#   name: "DatasetToSingleElement"
#   input_arg {
#     name: "dataset"
#     type: DT_VARIANT
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
# }
# op {
#   name: "DeserializeIterator"
#   input_arg {
#     name: "resource_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "serialized"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "FilterByLastComponentDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "output"
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
#   name: "FilterDataset"
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
#     name: "predicate"
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
#   name: "FixedLengthRecordDataset"
#   input_arg {
#     name: "filenames"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "header_bytes"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "record_bytes"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "footer_bytes"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "buffer_size"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "FixedLengthRecordDatasetV2"
#   input_arg {
#     name: "filenames"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "header_bytes"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "record_bytes"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "footer_bytes"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "buffer_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "compression_type"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "FlatMapDataset"
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
# }
# op {
#   name: "GeneratorDataset"
#   input_arg {
#     name: "init_func_other_args"
#     type_list_attr: "Tinit_func_args"
#   }
#   input_arg {
#     name: "next_func_other_args"
#     type_list_attr: "Tnext_func_args"
#   }
#   input_arg {
#     name: "finalize_func_other_args"
#     type_list_attr: "Tfinalize_func_args"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "init_func"
#     type: "func"
#   }
#   attr {
#     name: "next_func"
#     type: "func"
#   }
#   attr {
#     name: "finalize_func"
#     type: "func"
#   }
#   attr {
#     name: "Tinit_func_args"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tnext_func_args"
#     type: "list(type)"
#     has_minimum: true
#   }
#   attr {
#     name: "Tfinalize_func_args"
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
#   name: "InterleaveDataset"
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
#   name: "Iterator"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#   }
#   attr {
#     name: "container"
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
#   name: "IteratorFromStringHandle"
#   input_arg {
#     name: "string_handle"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "resource_handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "IteratorFromStringHandleV2"
#   input_arg {
#     name: "string_handle"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "resource_handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "IteratorGetNext"
#   input_arg {
#     name: "iterator"
#     type: DT_RESOURCE
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
#   name: "IteratorGetNextAsOptional"
#   input_arg {
#     name: "iterator"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "optional"
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
#   name: "IteratorGetNextSync"
#   input_arg {
#     name: "iterator"
#     type: DT_RESOURCE
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
#   name: "IteratorToStringHandle"
#   input_arg {
#     name: "resource_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "string_handle"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "IteratorV2"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#   }
#   attr {
#     name: "container"
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
#   name: "MakeIterator"
#   input_arg {
#     name: "dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "iterator"
#     type: DT_RESOURCE
#   }
#   is_stateful: true
# }
# op {
#   name: "MapDataset"
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
#   name: "MapDefun"
#   input_arg {
#     name: "arguments"
#     type_list_attr: "Targuments"
#   }
#   input_arg {
#     name: "captured_inputs"
#     type_list_attr: "Tcaptured"
#   }
#   output_arg {
#     name: "output"
#     type_list_attr: "output_types"
#   }
#   attr {
#     name: "Targuments"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "Tcaptured"
#     type: "list(type)"
#     default_value {
#       list {
#       }
#     }
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
#     name: "f"
#     type: "func"
#   }
# }
# op {
#   name: "ModelDataset"
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
#   name: "MultiDeviceIterator"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "devices"
#     type: "list(string)"
#     has_minimum: true
#     minimum: 1
#   }
#   attr {
#     name: "shared_name"
#     type: "string"
#   }
#   attr {
#     name: "container"
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
#   name: "MultiDeviceIteratorFromStringHandle"
#   input_arg {
#     name: "string_handle"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "multi_device_iterator"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "output_types"
#     type: "list(type)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   attr {
#     name: "output_shapes"
#     type: "list(shape)"
#     default_value {
#       list {
#       }
#     }
#     has_minimum: true
#   }
#   is_stateful: true
# }
# op {
#   name: "MultiDeviceIteratorGetNextFromShard"
#   input_arg {
#     name: "multi_device_iterator"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "shard_num"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "incarnation_id"
#     type: DT_INT64
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
#   name: "MultiDeviceIteratorInit"
#   input_arg {
#     name: "dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "multi_device_iterator"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "max_buffer_size"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "incarnation_id"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "MultiDeviceIteratorToStringHandle"
#   input_arg {
#     name: "multi_device_iterator"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "string_handle"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "OneShotIterator"
#   output_arg {
#     name: "handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "dataset_factory"
#     type: "func"
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
#   name: "OptimizeDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "optimizations"
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
#   name: "OptionalFromValue"
#   input_arg {
#     name: "components"
#     type_list_attr: "Toutput_types"
#   }
#   output_arg {
#     name: "optional"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "Toutput_types"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
# }
# op {
#   name: "OptionalGetValue"
#   input_arg {
#     name: "optional"
#     type: DT_VARIANT
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
# }
# op {
#   name: "OptionalHasValue"
#   input_arg {
#     name: "optional"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "has_value"
#     type: DT_BOOL
#   }
# }
# op {
#   name: "OptionalNone"
#   output_arg {
#     name: "optional"
#     type: DT_VARIANT
#   }
# }
# op {
#   name: "PaddedBatchDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "batch_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "padded_shapes"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "padding_values"
#     type_list_attr: "Toutput_types"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "Toutput_types"
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
#   name: "PaddedBatchDatasetV2"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "batch_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "padded_shapes"
#     type: DT_INT64
#     number_attr: "N"
#   }
#   input_arg {
#     name: "padding_values"
#     type_list_attr: "Toutput_types"
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
#     name: "Toutput_types"
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
#   name: "ParallelInterleaveDatasetV2"
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
#     name: "num_parallel_calls"
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
#   attr {
#     name: "sloppy"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
# }
# op {
#   name: "ParallelMapDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "other_arguments"
#     type_list_attr: "Targuments"
#   }
#   input_arg {
#     name: "num_parallel_calls"
#     type: DT_INT32
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
#     name: "sloppy"
#     type: "bool"
#     default_value {
#       b: false
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
#   name: "PrefetchDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "buffer_size"
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
#   name: "RangeDataset"
#   input_arg {
#     name: "start"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "stop"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "step"
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
#   name: "ReduceDataset"
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
#     name: "components"
#     type_list_attr: "output_types"
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
#     name: "use_inter_op_parallelism"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
# }
# op {
#   name: "RepeatDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "count"
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
#   name: "SerializeIterator"
#   input_arg {
#     name: "resource_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "serialized"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "ShuffleAndRepeatDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "buffer_size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "seed"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "seed2"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "count"
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
#   name: "ShuffleDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "buffer_size"
#     type: DT_INT64
#   }
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
#     name: "reshuffle_each_iteration"
#     type: "bool"
#     default_value {
#       b: true
#     }
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
#   name: "SkipDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "count"
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
#   name: "SparseTensorSliceDataset"
#   input_arg {
#     name: "indices"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "values"
#     type_attr: "Tvalues"
#   }
#   input_arg {
#     name: "dense_shape"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "Tvalues"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "TFRecordDataset"
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
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "TakeDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "count"
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
#   name: "TensorDataset"
#   input_arg {
#     name: "components"
#     type_list_attr: "Toutput_types"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "Toutput_types"
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
#   name: "TensorSliceDataset"
#   input_arg {
#     name: "components"
#     type_list_attr: "Toutput_types"
#   }
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   attr {
#     name: "Toutput_types"
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
#   name: "TextLineDataset"
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
#   output_arg {
#     name: "handle"
#     type: DT_VARIANT
#   }
#   is_stateful: true
# }
# op {
#   name: "UnwrapDatasetVariant"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "output_handle"
#     type: DT_VARIANT
#   }
# }
# op {
#   name: "WindowDataset"
#   input_arg {
#     name: "input_dataset"
#     type: DT_VARIANT
#   }
#   input_arg {
#     name: "size"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "shift"
#     type: DT_INT64
#   }
#   input_arg {
#     name: "stride"
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
#   name: "WrapDatasetVariant"
#   input_arg {
#     name: "input_handle"
#     type: DT_VARIANT
#   }
#   output_arg {
#     name: "output_handle"
#     type: DT_VARIANT
#   }
# }
# op {
#   name: "ZipDataset"
#   input_arg {
#     name: "input_datasets"
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
_op_def_lib = _InitOpDefLibrary(b"\nd\n\021AnonymousIterator\032\n\n\006handle\030\024\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\177\n\014BatchDataset\022\021\n\rinput_dataset\030\025\022\016\n\nbatch_size\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\225\001\n\016BatchDatasetV2\022\021\n\rinput_dataset\030\025\022\016\n\nbatch_size\030\t\022\022\n\016drop_remainder\030\n\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n}\n\014CacheDataset\022\021\n\rinput_dataset\030\025\022\014\n\010filename\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\212\001\n\022ConcatenateDataset\022\021\n\rinput_dataset\030\025\022\023\n\017another_dataset\030\025\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n.\n\016DatasetToGraph\022\021\n\rinput_dataset\030\025\032\t\n\005graph\030\007\n\203\001\n\026DatasetToSingleElement\022\013\n\007dataset\030\025\032\032\n\ncomponents2\014output_types\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n=\n\023DeserializeIterator\022\023\n\017resource_handle\030\024\022\016\n\nserialized\030\025\210\001\001\n\177\n\034FilterByLastComponentDataset\022\021\n\rinput_dataset\030\025\032\n\n\006output\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\276\001\n\rFilterDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\032\n\n\006handle\030\025\"\021\n\tpredicate\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\177\n\030FixedLengthRecordDataset\022\r\n\tfilenames\030\007\022\020\n\014header_bytes\030\t\022\020\n\014record_bytes\030\t\022\020\n\014footer_bytes\030\t\022\017\n\013buffer_size\030\t\032\n\n\006handle\030\025\210\001\001\n\227\001\n\032FixedLengthRecordDatasetV2\022\r\n\tfilenames\030\007\022\020\n\014header_bytes\030\t\022\020\n\014record_bytes\030\t\022\020\n\014footer_bytes\030\t\022\017\n\013buffer_size\030\t\022\024\n\020compression_type\030\007\032\n\n\006handle\030\025\210\001\001\n\267\001\n\016FlatMapDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\212\003\n\020GeneratorDataset\022\'\n\024init_func_other_args2\017Tinit_func_args\022\'\n\024next_func_other_args2\017Tnext_func_args\022/\n\030finalize_func_other_args2\023Tfinalize_func_args\032\n\n\006handle\030\025\"\021\n\tinit_func\022\004func\"\021\n\tnext_func\022\004func\"\025\n\rfinalize_func\022\004func\"\037\n\017Tinit_func_args\022\nlist(type)(\001\"\037\n\017Tnext_func_args\022\nlist(type)(\001\"#\n\023Tfinalize_func_args\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\336\001\n\021InterleaveDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\022\020\n\014cycle_length\030\t\022\020\n\014block_length\030\t\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\207\001\n\010Iterator\032\n\n\006handle\030\024\"\025\n\013shared_name\022\006string\"\023\n\tcontainer\022\006string\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\213\001\n\030IteratorFromStringHandle\022\021\n\rstring_handle\030\007\032\023\n\017resource_handle\030\024\" \n\014output_types\022\nlist(type)\032\002\n\000(\001\"\"\n\routput_shapes\022\013list(shape)\032\002\n\000(\001\210\001\001\n\215\001\n\032IteratorFromStringHandleV2\022\021\n\rstring_handle\030\007\032\023\n\017resource_handle\030\024\" \n\014output_types\022\nlist(type)\032\002\n\000(\001\"\"\n\routput_shapes\022\013list(shape)\032\002\n\000(\001\210\001\001\n\200\001\n\017IteratorGetNext\022\014\n\010iterator\030\024\032\032\n\ncomponents2\014output_types\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n|\n\031IteratorGetNextAsOptional\022\014\n\010iterator\030\024\032\014\n\010optional\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\204\001\n\023IteratorGetNextSync\022\014\n\010iterator\030\024\032\032\n\ncomponents2\014output_types\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\nC\n\026IteratorToStringHandle\022\023\n\017resource_handle\030\024\032\021\n\rstring_handle\030\007\210\001\001\n\211\001\n\nIteratorV2\032\n\n\006handle\030\024\"\025\n\013shared_name\022\006string\"\023\n\tcontainer\022\006string\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n,\n\014MakeIterator\022\013\n\007dataset\030\025\022\014\n\010iterator\030\024\210\001\001\n\373\001\n\nMapDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"$\n\030use_inter_op_parallelism\022\004bool\032\002(\001\" \n\024preserve_cardinality\022\004bool\032\002(\000\n\343\001\n\010MapDefun\022\027\n\targuments2\nTarguments\022\034\n\017captured_inputs2\tTcaptured\032\026\n\006output2\014output_types\"\034\n\nTarguments\022\nlist(type)(\0010\001\"\035\n\tTcaptured\022\nlist(type)\032\002\n\000(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\t\n\001f\022\004func\no\n\014ModelDataset\022\021\n\rinput_dataset\030\025\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\257\001\n\023MultiDeviceIterator\032\n\n\006handle\030\024\"\033\n\007devices\022\014list(string)(\0010\001\"\025\n\013shared_name\022\006string\"\023\n\tcontainer\022\006string\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\234\001\n#MultiDeviceIteratorFromStringHandle\022\021\n\rstring_handle\030\007\032\031\n\025multi_device_iterator\030\024\" \n\014output_types\022\nlist(type)\032\002\n\000(\001\"\"\n\routput_shapes\022\013list(shape)\032\002\n\000(\001\210\001\001\n\304\001\n#MultiDeviceIteratorGetNextFromShard\022\031\n\025multi_device_iterator\030\024\022\r\n\tshard_num\030\003\022\022\n\016incarnation_id\030\t\032\032\n\ncomponents2\014output_types\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\nm\n\027MultiDeviceIteratorInit\022\013\n\007dataset\030\025\022\031\n\025multi_device_iterator\030\024\022\023\n\017max_buffer_size\030\t\032\022\n\016incarnation_id\030\t\210\001\001\nT\n!MultiDeviceIteratorToStringHandle\022\031\n\025multi_device_iterator\030\024\032\021\n\rstring_handle\030\007\210\001\001\n\257\001\n\017OneShotIterator\032\n\n\006handle\030\024\"\027\n\017dataset_factory\022\004func\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\205\001\n\017OptimizeDataset\022\021\n\rinput_dataset\030\025\022\021\n\roptimizations\030\007\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n_\n\021OptionalFromValue\022\033\n\ncomponents2\rToutput_types\032\014\n\010optional\030\025\"\037\n\rToutput_types\022\nlist(type)(\0010\001\n~\n\020OptionalGetValue\022\014\n\010optional\030\025\032\032\n\ncomponents2\014output_types\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n/\n\020OptionalHasValue\022\014\n\010optional\030\025\032\r\n\thas_value\030\n\n\034\n\014OptionalNone\032\014\n\010optional\030\025\n\313\001\n\022PaddedBatchDataset\022\021\n\rinput_dataset\030\025\022\016\n\nbatch_size\030\t\022\024\n\rpadded_shapes\030\t*\001N\022\037\n\016padding_values2\rToutput_types\032\n\n\006handle\030\025\"\037\n\rToutput_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\014\n\001N\022\003int(\0010\001\n\341\001\n\024PaddedBatchDatasetV2\022\021\n\rinput_dataset\030\025\022\016\n\nbatch_size\030\t\022\024\n\rpadded_shapes\030\t*\001N\022\037\n\016padding_values2\rToutput_types\022\022\n\016drop_remainder\030\n\032\n\n\006handle\030\025\"\037\n\rToutput_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\014\n\001N\022\003int(\0010\001\n\224\002\n\033ParallelInterleaveDatasetV2\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\022\020\n\014cycle_length\030\t\022\020\n\014block_length\030\t\022\026\n\022num_parallel_calls\030\t\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\022\n\006sloppy\022\004bool\032\002(\000\n\257\002\n\022ParallelMapDataset\022\021\n\rinput_dataset\030\025\022\035\n\017other_arguments2\nTarguments\022\026\n\022num_parallel_calls\030\003\032\n\n\006handle\030\025\"\t\n\001f\022\004func\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"$\n\030use_inter_op_parallelism\022\004bool\032\002(\001\"\022\n\006sloppy\022\004bool\032\002(\000\" \n\024preserve_cardinality\022\004bool\032\002(\000\n\203\001\n\017PrefetchDataset\022\021\n\rinput_dataset\030\025\022\017\n\013buffer_size\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n~\n\014RangeDataset\022\t\n\005start\030\t\022\010\n\004stop\030\t\022\010\n\004step\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\237\002\n\rReduceDataset\022\021\n\rinput_dataset\030\025\022\027\n\rinitial_state2\006Tstate\022\035\n\017other_arguments2\nTarguments\032\032\n\ncomponents2\014output_types\"\t\n\001f\022\004func\"\030\n\006Tstate\022\nlist(type)(\0010\001\"\032\n\nTarguments\022\nlist(type)(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"$\n\030use_inter_op_parallelism\022\004bool\032\002(\001\n{\n\rRepeatDataset\022\021\n\rinput_dataset\030\025\022\t\n\005count\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n;\n\021SerializeIterator\022\023\n\017resource_handle\030\024\032\016\n\nserialized\030\025\210\001\001\n\253\001\n\027ShuffleAndRepeatDataset\022\021\n\rinput_dataset\030\025\022\017\n\013buffer_size\030\t\022\010\n\004seed\030\t\022\t\n\005seed2\030\t\022\t\n\005count\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n\275\001\n\016ShuffleDataset\022\021\n\rinput_dataset\030\025\022\017\n\013buffer_size\030\t\022\010\n\004seed\030\t\022\t\n\005seed2\030\t\032\n\n\006handle\030\025\"$\n\030reshuffle_each_iteration\022\004bool\032\002(\001\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\ny\n\013SkipDataset\022\021\n\rinput_dataset\030\025\022\t\n\005count\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\nk\n\030SparseTensorSliceDataset\022\013\n\007indices\030\t\022\021\n\006values\"\007Tvalues\022\017\n\013dense_shape\030\t\032\n\n\006handle\030\025\"\017\n\007Tvalues\022\004type\210\001\001\nV\n\017TFRecordDataset\022\r\n\tfilenames\030\007\022\024\n\020compression_type\030\007\022\017\n\013buffer_size\030\t\032\n\n\006handle\030\025\210\001\001\ny\n\013TakeDataset\022\021\n\rinput_dataset\030\025\022\t\n\005count\030\t\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n~\n\rTensorDataset\022\033\n\ncomponents2\rToutput_types\032\n\n\006handle\030\025\"\037\n\rToutput_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\n\203\001\n\022TensorSliceDataset\022\033\n\ncomponents2\rToutput_types\032\n\n\006handle\030\025\"\037\n\rToutput_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\210\001\001\nV\n\017TextLineDataset\022\r\n\tfilenames\030\007\022\024\n\020compression_type\030\007\022\017\n\013buffer_size\030\t\032\n\n\006handle\030\025\210\001\001\n;\n\024UnwrapDatasetVariant\022\020\n\014input_handle\030\025\032\021\n\routput_handle\030\025\n\245\001\n\rWindowDataset\022\021\n\rinput_dataset\030\025\022\010\n\004size\030\t\022\t\n\005shift\030\t\022\n\n\006stride\030\t\022\022\n\016drop_remainder\030\n\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\n9\n\022WrapDatasetVariant\022\020\n\014input_handle\030\025\032\021\n\routput_handle\030\025\n\177\n\nZipDataset\022\025\n\016input_datasets\030\025*\001N\032\n\n\006handle\030\025\"\036\n\014output_types\022\nlist(type)(\0010\001\" \n\routput_shapes\022\013list(shape)(\0010\001\"\014\n\001N\022\003int(\0010\001")
