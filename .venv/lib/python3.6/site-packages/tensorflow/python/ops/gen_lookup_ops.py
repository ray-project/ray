"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: lookup_ops.cc
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


def hash_table(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None):
  r"""Creates a non-initialized hash table.

  This op creates a hash table, specifying the type of its keys and values.
  Before using the table you will have to initialize it.  After initialization the
  table will be immutable.

  Args:
    key_dtype: A `tf.DType`. Type of the table keys.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
      If true and shared_name is empty, the table is shared
      using the node name.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("hash_table op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  _, _, _op = _op_def_lib._apply_op_helper(
        "HashTable", key_dtype=key_dtype, value_dtype=value_dtype,
                     container=container, shared_name=shared_name,
                     use_node_name_sharing=use_node_name_sharing, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"))
  _execute.record_gradient(
      "HashTable", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def hash_table_eager_fallback(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None, ctx=None):
  raise RuntimeError("hash_table op does not support eager execution. Arg 'table_handle' is a ref.")

def hash_table_v2(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None):
  r"""Creates a non-initialized hash table.

  This op creates a hash table, specifying the type of its keys and values.
  Before using the table you will have to initialize it.  After initialization the
  table will be immutable.

  Args:
    key_dtype: A `tf.DType`. Type of the table keys.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
      If true and shared_name is empty, the table is shared
      using the node name.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "HashTableV2",
        name, _ctx._post_execution_callbacks, "container", container,
        "shared_name", shared_name, "use_node_name_sharing",
        use_node_name_sharing, "key_dtype", key_dtype, "value_dtype",
        value_dtype)
      return _result
    except _core._FallbackException:
      try:
        return hash_table_v2_eager_fallback(
            container=container, shared_name=shared_name,
            use_node_name_sharing=use_node_name_sharing, key_dtype=key_dtype,
            value_dtype=value_dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  _, _, _op = _op_def_lib._apply_op_helper(
        "HashTableV2", key_dtype=key_dtype, value_dtype=value_dtype,
                       container=container, shared_name=shared_name,
                       use_node_name_sharing=use_node_name_sharing, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"))
  _execute.record_gradient(
      "HashTableV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def hash_table_v2_eager_fallback(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function hash_table_v2
  """
  _ctx = ctx if ctx else _context.context()
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name,
  "use_node_name_sharing", use_node_name_sharing, "key_dtype", key_dtype,
  "value_dtype", value_dtype)
  _result = _execute.execute(b"HashTableV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "HashTableV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def initialize_table(table_handle, keys, values, name=None):
  r"""Table initializer that takes two tensors for keys and values respectively.

  Args:
    table_handle: A `Tensor` of type mutable `string`.
      Handle to a table which will be initialized.
    keys: A `Tensor`. Keys of type Tkey.
    values: A `Tensor`. Values of type Tval.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("initialize_table op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "InitializeTable", table_handle=table_handle, keys=keys,
                           values=values, name=name)
  return _op
  _result = None
  return _result



def initialize_table_eager_fallback(table_handle, keys, values, name=None, ctx=None):
  raise RuntimeError("initialize_table op does not support eager execution. Arg 'table_handle' is a ref.")

def initialize_table_from_text_file(table_handle, filename, key_index, value_index, vocab_size=-1, delimiter="\t", name=None):
  r"""Initializes a table from a text file.

  It inserts one key-value pair into the table for each line of the file.
  The key and value is extracted from the whole line content, elements from the
  split line based on `delimiter` or the line number (starting from zero).
  Where to extract the key and value from a line is specified by `key_index` and
  `value_index`.

  - A value of -1 means use the line number(starting from zero), expects `int64`.
  - A value of -2 means use the whole line content, expects `string`.
  - A value >= 0 means use the index (starting at zero) of the split line based
    on `delimiter`.

  Args:
    table_handle: A `Tensor` of type mutable `string`.
      Handle to a table which will be initialized.
    filename: A `Tensor` of type `string`. Filename of a vocabulary text file.
    key_index: An `int` that is `>= -2`.
      Column index in a line to get the table `key` values from.
    value_index: An `int` that is `>= -2`.
      Column index that represents information of a line to get the table
      `value` values from.
    vocab_size: An optional `int` that is `>= -1`. Defaults to `-1`.
      Number of elements of the file, use -1 if unknown.
    delimiter: An optional `string`. Defaults to `"\t"`.
      Delimiter to separate fields in a line.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("initialize_table_from_text_file op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  key_index = _execute.make_int(key_index, "key_index")
  value_index = _execute.make_int(value_index, "value_index")
  if vocab_size is None:
    vocab_size = -1
  vocab_size = _execute.make_int(vocab_size, "vocab_size")
  if delimiter is None:
    delimiter = "\t"
  delimiter = _execute.make_str(delimiter, "delimiter")
  _, _, _op = _op_def_lib._apply_op_helper(
        "InitializeTableFromTextFile", table_handle=table_handle,
                                       filename=filename, key_index=key_index,
                                       value_index=value_index,
                                       vocab_size=vocab_size,
                                       delimiter=delimiter, name=name)
  return _op
  _result = None
  return _result



def initialize_table_from_text_file_eager_fallback(table_handle, filename, key_index, value_index, vocab_size=-1, delimiter="\t", name=None, ctx=None):
  raise RuntimeError("initialize_table_from_text_file op does not support eager execution. Arg 'table_handle' is a ref.")

def initialize_table_from_text_file_v2(table_handle, filename, key_index, value_index, vocab_size=-1, delimiter="\t", name=None):
  r"""Initializes a table from a text file.

  It inserts one key-value pair into the table for each line of the file.
  The key and value is extracted from the whole line content, elements from the
  split line based on `delimiter` or the line number (starting from zero).
  Where to extract the key and value from a line is specified by `key_index` and
  `value_index`.

  - A value of -1 means use the line number(starting from zero), expects `int64`.
  - A value of -2 means use the whole line content, expects `string`.
  - A value >= 0 means use the index (starting at zero) of the split line based
    on `delimiter`.

  Args:
    table_handle: A `Tensor` of type `resource`.
      Handle to a table which will be initialized.
    filename: A `Tensor` of type `string`. Filename of a vocabulary text file.
    key_index: An `int` that is `>= -2`.
      Column index in a line to get the table `key` values from.
    value_index: An `int` that is `>= -2`.
      Column index that represents information of a line to get the table
      `value` values from.
    vocab_size: An optional `int` that is `>= -1`. Defaults to `-1`.
      Number of elements of the file, use -1 if unknown.
    delimiter: An optional `string`. Defaults to `"\t"`.
      Delimiter to separate fields in a line.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InitializeTableFromTextFileV2", name, _ctx._post_execution_callbacks,
        table_handle, filename, "key_index", key_index, "value_index",
        value_index, "vocab_size", vocab_size, "delimiter", delimiter)
      return _result
    except _core._FallbackException:
      try:
        return initialize_table_from_text_file_v2_eager_fallback(
            table_handle, filename, key_index=key_index,
            value_index=value_index, vocab_size=vocab_size,
            delimiter=delimiter, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  key_index = _execute.make_int(key_index, "key_index")
  value_index = _execute.make_int(value_index, "value_index")
  if vocab_size is None:
    vocab_size = -1
  vocab_size = _execute.make_int(vocab_size, "vocab_size")
  if delimiter is None:
    delimiter = "\t"
  delimiter = _execute.make_str(delimiter, "delimiter")
  _, _, _op = _op_def_lib._apply_op_helper(
        "InitializeTableFromTextFileV2", table_handle=table_handle,
                                         filename=filename,
                                         key_index=key_index,
                                         value_index=value_index,
                                         vocab_size=vocab_size,
                                         delimiter=delimiter, name=name)
  return _op
  _result = None
  return _result



def initialize_table_from_text_file_v2_eager_fallback(table_handle, filename, key_index, value_index, vocab_size=-1, delimiter="\t", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function initialize_table_from_text_file_v2
  """
  _ctx = ctx if ctx else _context.context()
  key_index = _execute.make_int(key_index, "key_index")
  value_index = _execute.make_int(value_index, "value_index")
  if vocab_size is None:
    vocab_size = -1
  vocab_size = _execute.make_int(vocab_size, "vocab_size")
  if delimiter is None:
    delimiter = "\t"
  delimiter = _execute.make_str(delimiter, "delimiter")
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  filename = _ops.convert_to_tensor(filename, _dtypes.string)
  _inputs_flat = [table_handle, filename]
  _attrs = ("key_index", key_index, "value_index", value_index, "vocab_size",
  vocab_size, "delimiter", delimiter)
  _result = _execute.execute(b"InitializeTableFromTextFileV2", 0,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _result = None
  return _result


def initialize_table_v2(table_handle, keys, values, name=None):
  r"""Table initializer that takes two tensors for keys and values respectively.

  Args:
    table_handle: A `Tensor` of type `resource`.
      Handle to a table which will be initialized.
    keys: A `Tensor`. Keys of type Tkey.
    values: A `Tensor`. Values of type Tval.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "InitializeTableV2", name, _ctx._post_execution_callbacks,
        table_handle, keys, values)
      return _result
    except _core._FallbackException:
      try:
        return initialize_table_v2_eager_fallback(
            table_handle, keys, values, name=name, ctx=_ctx)
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
        "InitializeTableV2", table_handle=table_handle, keys=keys,
                             values=values, name=name)
  return _op
  _result = None
  return _result



def initialize_table_v2_eager_fallback(table_handle, keys, values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function initialize_table_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tkey, (keys,) = _execute.args_to_matching_eager([keys], _ctx)
  _attr_Tval, (values,) = _execute.args_to_matching_eager([values], _ctx)
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  _inputs_flat = [table_handle, keys, values]
  _attrs = ("Tkey", _attr_Tkey, "Tval", _attr_Tval)
  _result = _execute.execute(b"InitializeTableV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


_lookup_table_export_outputs = ["keys", "values"]
_LookupTableExportOutput = _collections.namedtuple(
    "LookupTableExport", _lookup_table_export_outputs)


def lookup_table_export(table_handle, Tkeys, Tvalues, name=None):
  r"""Outputs all keys and values in the table.

  Args:
    table_handle: A `Tensor` of type mutable `string`. Handle to the table.
    Tkeys: A `tf.DType`.
    Tvalues: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (keys, values).

    keys: A `Tensor` of type `Tkeys`.
    values: A `Tensor` of type `Tvalues`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("lookup_table_export op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  Tkeys = _execute.make_type(Tkeys, "Tkeys")
  Tvalues = _execute.make_type(Tvalues, "Tvalues")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LookupTableExport", table_handle=table_handle, Tkeys=Tkeys,
                             Tvalues=Tvalues, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tkeys", _op.get_attr("Tkeys"), "Tvalues",
            _op.get_attr("Tvalues"))
  _execute.record_gradient(
      "LookupTableExport", _inputs_flat, _attrs, _result, name)
  _result = _LookupTableExportOutput._make(_result)
  return _result



def lookup_table_export_eager_fallback(table_handle, Tkeys, Tvalues, name=None, ctx=None):
  raise RuntimeError("lookup_table_export op does not support eager execution. Arg 'table_handle' is a ref.")

_lookup_table_export_v2_outputs = ["keys", "values"]
_LookupTableExportV2Output = _collections.namedtuple(
    "LookupTableExportV2", _lookup_table_export_v2_outputs)


def lookup_table_export_v2(table_handle, Tkeys, Tvalues, name=None):
  r"""Outputs all keys and values in the table.

  Args:
    table_handle: A `Tensor` of type `resource`. Handle to the table.
    Tkeys: A `tf.DType`.
    Tvalues: A `tf.DType`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (keys, values).

    keys: A `Tensor` of type `Tkeys`.
    values: A `Tensor` of type `Tvalues`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LookupTableExportV2", name, _ctx._post_execution_callbacks,
        table_handle, "Tkeys", Tkeys, "Tvalues", Tvalues)
      _result = _LookupTableExportV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return lookup_table_export_v2_eager_fallback(
            table_handle, Tkeys=Tkeys, Tvalues=Tvalues, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  Tkeys = _execute.make_type(Tkeys, "Tkeys")
  Tvalues = _execute.make_type(Tvalues, "Tvalues")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LookupTableExportV2", table_handle=table_handle, Tkeys=Tkeys,
                               Tvalues=Tvalues, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tkeys", _op.get_attr("Tkeys"), "Tvalues",
            _op.get_attr("Tvalues"))
  _execute.record_gradient(
      "LookupTableExportV2", _inputs_flat, _attrs, _result, name)
  _result = _LookupTableExportV2Output._make(_result)
  return _result



def lookup_table_export_v2_eager_fallback(table_handle, Tkeys, Tvalues, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lookup_table_export_v2
  """
  _ctx = ctx if ctx else _context.context()
  Tkeys = _execute.make_type(Tkeys, "Tkeys")
  Tvalues = _execute.make_type(Tvalues, "Tvalues")
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  _inputs_flat = [table_handle]
  _attrs = ("Tkeys", Tkeys, "Tvalues", Tvalues)
  _result = _execute.execute(b"LookupTableExportV2", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LookupTableExportV2", _inputs_flat, _attrs, _result, name)
  _result = _LookupTableExportV2Output._make(_result)
  return _result


def lookup_table_find(table_handle, keys, default_value, name=None):
  r"""Looks up keys in a table, outputs the corresponding values.

  The tensor `keys` must of the same type as the keys of the table.
  The output `values` is of the type of the table values.

  The scalar `default_value` is the value output for keys not present in the
  table. It must also be of the same type as the table values.

  Args:
    table_handle: A `Tensor` of type mutable `string`. Handle to the table.
    keys: A `Tensor`. Any shape.  Keys to look up.
    default_value: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `default_value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("lookup_table_find op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "LookupTableFind", table_handle=table_handle, keys=keys,
                           default_value=default_value, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tin", _op.get_attr("Tin"), "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "LookupTableFind", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lookup_table_find_eager_fallback(table_handle, keys, default_value, name=None, ctx=None):
  raise RuntimeError("lookup_table_find op does not support eager execution. Arg 'table_handle' is a ref.")

def lookup_table_find_v2(table_handle, keys, default_value, name=None):
  r"""Looks up keys in a table, outputs the corresponding values.

  The tensor `keys` must of the same type as the keys of the table.
  The output `values` is of the type of the table values.

  The scalar `default_value` is the value output for keys not present in the
  table. It must also be of the same type as the table values.

  Args:
    table_handle: A `Tensor` of type `resource`. Handle to the table.
    keys: A `Tensor`. Any shape.  Keys to look up.
    default_value: A `Tensor`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor`. Has the same type as `default_value`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LookupTableFindV2", name, _ctx._post_execution_callbacks,
        table_handle, keys, default_value)
      return _result
    except _core._FallbackException:
      try:
        return lookup_table_find_v2_eager_fallback(
            table_handle, keys, default_value, name=name, ctx=_ctx)
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
        "LookupTableFindV2", table_handle=table_handle, keys=keys,
                             default_value=default_value, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("Tin", _op.get_attr("Tin"), "Tout", _op.get_attr("Tout"))
  _execute.record_gradient(
      "LookupTableFindV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lookup_table_find_v2_eager_fallback(table_handle, keys, default_value, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lookup_table_find_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tin, (keys,) = _execute.args_to_matching_eager([keys], _ctx)
  _attr_Tout, (default_value,) = _execute.args_to_matching_eager([default_value], _ctx)
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  _inputs_flat = [table_handle, keys, default_value]
  _attrs = ("Tin", _attr_Tin, "Tout", _attr_Tout)
  _result = _execute.execute(b"LookupTableFindV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LookupTableFindV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def lookup_table_import(table_handle, keys, values, name=None):
  r"""Replaces the contents of the table with the specified keys and values.

  The tensor `keys` must be of the same type as the keys of the table.
  The tensor `values` must be of the type of the table values.

  Args:
    table_handle: A `Tensor` of type mutable `string`. Handle to the table.
    keys: A `Tensor`. Any shape.  Keys to look up.
    values: A `Tensor`. Values to associate with keys.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("lookup_table_import op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "LookupTableImport", table_handle=table_handle, keys=keys,
                             values=values, name=name)
  return _op
  _result = None
  return _result



def lookup_table_import_eager_fallback(table_handle, keys, values, name=None, ctx=None):
  raise RuntimeError("lookup_table_import op does not support eager execution. Arg 'table_handle' is a ref.")

def lookup_table_import_v2(table_handle, keys, values, name=None):
  r"""Replaces the contents of the table with the specified keys and values.

  The tensor `keys` must be of the same type as the keys of the table.
  The tensor `values` must be of the type of the table values.

  Args:
    table_handle: A `Tensor` of type `resource`. Handle to the table.
    keys: A `Tensor`. Any shape.  Keys to look up.
    values: A `Tensor`. Values to associate with keys.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LookupTableImportV2", name, _ctx._post_execution_callbacks,
        table_handle, keys, values)
      return _result
    except _core._FallbackException:
      try:
        return lookup_table_import_v2_eager_fallback(
            table_handle, keys, values, name=name, ctx=_ctx)
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
        "LookupTableImportV2", table_handle=table_handle, keys=keys,
                               values=values, name=name)
  return _op
  _result = None
  return _result



def lookup_table_import_v2_eager_fallback(table_handle, keys, values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lookup_table_import_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tin, (keys,) = _execute.args_to_matching_eager([keys], _ctx)
  _attr_Tout, (values,) = _execute.args_to_matching_eager([values], _ctx)
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  _inputs_flat = [table_handle, keys, values]
  _attrs = ("Tin", _attr_Tin, "Tout", _attr_Tout)
  _result = _execute.execute(b"LookupTableImportV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def lookup_table_insert(table_handle, keys, values, name=None):
  r"""Updates the table to associates keys with values.

  The tensor `keys` must be of the same type as the keys of the table.
  The tensor `values` must be of the type of the table values.

  Args:
    table_handle: A `Tensor` of type mutable `string`. Handle to the table.
    keys: A `Tensor`. Any shape.  Keys to look up.
    values: A `Tensor`. Values to associate with keys.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("lookup_table_insert op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "LookupTableInsert", table_handle=table_handle, keys=keys,
                             values=values, name=name)
  return _op
  _result = None
  return _result



def lookup_table_insert_eager_fallback(table_handle, keys, values, name=None, ctx=None):
  raise RuntimeError("lookup_table_insert op does not support eager execution. Arg 'table_handle' is a ref.")

def lookup_table_insert_v2(table_handle, keys, values, name=None):
  r"""Updates the table to associates keys with values.

  The tensor `keys` must be of the same type as the keys of the table.
  The tensor `values` must be of the type of the table values.

  Args:
    table_handle: A `Tensor` of type `resource`. Handle to the table.
    keys: A `Tensor`. Any shape.  Keys to look up.
    values: A `Tensor`. Values to associate with keys.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LookupTableInsertV2", name, _ctx._post_execution_callbacks,
        table_handle, keys, values)
      return _result
    except _core._FallbackException:
      try:
        return lookup_table_insert_v2_eager_fallback(
            table_handle, keys, values, name=name, ctx=_ctx)
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
        "LookupTableInsertV2", table_handle=table_handle, keys=keys,
                               values=values, name=name)
  return _op
  _result = None
  return _result



def lookup_table_insert_v2_eager_fallback(table_handle, keys, values, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lookup_table_insert_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tin, (keys,) = _execute.args_to_matching_eager([keys], _ctx)
  _attr_Tout, (values,) = _execute.args_to_matching_eager([values], _ctx)
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  _inputs_flat = [table_handle, keys, values]
  _attrs = ("Tin", _attr_Tin, "Tout", _attr_Tout)
  _result = _execute.execute(b"LookupTableInsertV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def lookup_table_remove_v2(table_handle, keys, name=None):
  r"""Removes keys and its associated values from a table.

  The tensor `keys` must of the same type as the keys of the table. Keys not
  already in the table are silently ignored.

  Args:
    table_handle: A `Tensor` of type `resource`. Handle to the table.
    keys: A `Tensor`. Any shape.  Keys of the elements to remove.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LookupTableRemoveV2", name, _ctx._post_execution_callbacks,
        table_handle, keys)
      return _result
    except _core._FallbackException:
      try:
        return lookup_table_remove_v2_eager_fallback(
            table_handle, keys, name=name, ctx=_ctx)
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
        "LookupTableRemoveV2", table_handle=table_handle, keys=keys,
                               name=name)
  return _op
  _result = None
  return _result



def lookup_table_remove_v2_eager_fallback(table_handle, keys, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lookup_table_remove_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_Tin, (keys,) = _execute.args_to_matching_eager([keys], _ctx)
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  _inputs_flat = [table_handle, keys]
  _attrs = ("Tin", _attr_Tin)
  _result = _execute.execute(b"LookupTableRemoveV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def lookup_table_size(table_handle, name=None):
  r"""Computes the number of elements in the given table.

  Args:
    table_handle: A `Tensor` of type mutable `string`. Handle to the table.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("lookup_table_size op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "LookupTableSize", table_handle=table_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "LookupTableSize", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lookup_table_size_eager_fallback(table_handle, name=None, ctx=None):
  raise RuntimeError("lookup_table_size op does not support eager execution. Arg 'table_handle' is a ref.")

def lookup_table_size_v2(table_handle, name=None):
  r"""Computes the number of elements in the given table.

  Args:
    table_handle: A `Tensor` of type `resource`. Handle to the table.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "LookupTableSizeV2", name, _ctx._post_execution_callbacks,
        table_handle)
      return _result
    except _core._FallbackException:
      try:
        return lookup_table_size_v2_eager_fallback(
            table_handle, name=name, ctx=_ctx)
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
        "LookupTableSizeV2", table_handle=table_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "LookupTableSizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lookup_table_size_v2_eager_fallback(table_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function lookup_table_size_v2
  """
  _ctx = ctx if ctx else _context.context()
  table_handle = _ops.convert_to_tensor(table_handle, _dtypes.resource)
  _inputs_flat = [table_handle]
  _attrs = None
  _result = _execute.execute(b"LookupTableSizeV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "LookupTableSizeV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mutable_dense_hash_table(empty_key, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], initial_num_buckets=131072, max_load_factor=0.8, name=None):
  r"""Creates an empty hash table that uses tensors as the backing store.

  It uses "open addressing" with quadratic reprobing to resolve
  collisions.

  This op creates a mutable hash table, specifying the type of its keys and
  values. Each value must be a scalar. Data can be inserted into the table using
  the insert operations. It does not support the initialization operation.

  Args:
    empty_key: A `Tensor`.
      The key used to represent empty key buckets internally. Must not
      be used in insert or lookup operations.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
    value_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `[]`.
      The shape of each value.
    initial_num_buckets: An optional `int`. Defaults to `131072`.
      The initial number of hash table buckets. Must be a power
      to 2.
    max_load_factor: An optional `float`. Defaults to `0.8`.
      The maximum ratio between number of entries and number of
      buckets before growing the table. Must be between 0 and 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("mutable_dense_hash_table op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  if value_shape is None:
    value_shape = []
  value_shape = _execute.make_shape(value_shape, "value_shape")
  if initial_num_buckets is None:
    initial_num_buckets = 131072
  initial_num_buckets = _execute.make_int(initial_num_buckets, "initial_num_buckets")
  if max_load_factor is None:
    max_load_factor = 0.8
  max_load_factor = _execute.make_float(max_load_factor, "max_load_factor")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MutableDenseHashTable", empty_key=empty_key, value_dtype=value_dtype,
                                 container=container, shared_name=shared_name,
                                 use_node_name_sharing=use_node_name_sharing,
                                 value_shape=value_shape,
                                 initial_num_buckets=initial_num_buckets,
                                 max_load_factor=max_load_factor, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"), "value_shape",
            _op.get_attr("value_shape"), "initial_num_buckets",
            _op.get_attr("initial_num_buckets"), "max_load_factor",
            _op.get_attr("max_load_factor"))
  _execute.record_gradient(
      "MutableDenseHashTable", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutable_dense_hash_table_eager_fallback(empty_key, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], initial_num_buckets=131072, max_load_factor=0.8, name=None, ctx=None):
  raise RuntimeError("mutable_dense_hash_table op does not support eager execution. Arg 'table_handle' is a ref.")

def mutable_dense_hash_table_v2(empty_key, deleted_key, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], initial_num_buckets=131072, max_load_factor=0.8, name=None):
  r"""Creates an empty hash table that uses tensors as the backing store.

  It uses "open addressing" with quadratic reprobing to resolve
  collisions.

  This op creates a mutable hash table, specifying the type of its keys and
  values. Each value must be a scalar. Data can be inserted into the table using
  the insert operations. It does not support the initialization operation.

  Args:
    empty_key: A `Tensor`.
      The key used to represent empty key buckets internally. Must not
      be used in insert or lookup operations.
    deleted_key: A `Tensor`. Must have the same type as `empty_key`.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
    value_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `[]`.
      The shape of each value.
    initial_num_buckets: An optional `int`. Defaults to `131072`.
      The initial number of hash table buckets. Must be a power
      to 2.
    max_load_factor: An optional `float`. Defaults to `0.8`.
      The maximum ratio between number of entries and number of
      buckets before growing the table. Must be between 0 and 1.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MutableDenseHashTableV2", name, _ctx._post_execution_callbacks,
        empty_key, deleted_key, "container", container, "shared_name",
        shared_name, "use_node_name_sharing", use_node_name_sharing,
        "value_dtype", value_dtype, "value_shape", value_shape,
        "initial_num_buckets", initial_num_buckets, "max_load_factor",
        max_load_factor)
      return _result
    except _core._FallbackException:
      try:
        return mutable_dense_hash_table_v2_eager_fallback(
            empty_key, deleted_key, container=container,
            shared_name=shared_name,
            use_node_name_sharing=use_node_name_sharing,
            value_dtype=value_dtype, value_shape=value_shape,
            initial_num_buckets=initial_num_buckets,
            max_load_factor=max_load_factor, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  if value_shape is None:
    value_shape = []
  value_shape = _execute.make_shape(value_shape, "value_shape")
  if initial_num_buckets is None:
    initial_num_buckets = 131072
  initial_num_buckets = _execute.make_int(initial_num_buckets, "initial_num_buckets")
  if max_load_factor is None:
    max_load_factor = 0.8
  max_load_factor = _execute.make_float(max_load_factor, "max_load_factor")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MutableDenseHashTableV2", empty_key=empty_key,
                                   deleted_key=deleted_key,
                                   value_dtype=value_dtype,
                                   container=container,
                                   shared_name=shared_name,
                                   use_node_name_sharing=use_node_name_sharing,
                                   value_shape=value_shape,
                                   initial_num_buckets=initial_num_buckets,
                                   max_load_factor=max_load_factor, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"), "value_shape",
            _op.get_attr("value_shape"), "initial_num_buckets",
            _op.get_attr("initial_num_buckets"), "max_load_factor",
            _op.get_attr("max_load_factor"))
  _execute.record_gradient(
      "MutableDenseHashTableV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutable_dense_hash_table_v2_eager_fallback(empty_key, deleted_key, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], initial_num_buckets=131072, max_load_factor=0.8, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mutable_dense_hash_table_v2
  """
  _ctx = ctx if ctx else _context.context()
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  if value_shape is None:
    value_shape = []
  value_shape = _execute.make_shape(value_shape, "value_shape")
  if initial_num_buckets is None:
    initial_num_buckets = 131072
  initial_num_buckets = _execute.make_int(initial_num_buckets, "initial_num_buckets")
  if max_load_factor is None:
    max_load_factor = 0.8
  max_load_factor = _execute.make_float(max_load_factor, "max_load_factor")
  _attr_key_dtype, _inputs_key_dtype = _execute.args_to_matching_eager([empty_key, deleted_key], _ctx)
  (empty_key, deleted_key) = _inputs_key_dtype
  _inputs_flat = [empty_key, deleted_key]
  _attrs = ("container", container, "shared_name", shared_name,
  "use_node_name_sharing", use_node_name_sharing, "key_dtype",
  _attr_key_dtype, "value_dtype", value_dtype, "value_shape", value_shape,
  "initial_num_buckets", initial_num_buckets, "max_load_factor",
  max_load_factor)
  _result = _execute.execute(b"MutableDenseHashTableV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MutableDenseHashTableV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mutable_hash_table(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None):
  r"""Creates an empty hash table.

  This op creates a mutable hash table, specifying the type of its keys and
  values. Each value must be a scalar. Data can be inserted into the table using
  the insert operations. It does not support the initialization operation.

  Args:
    key_dtype: A `tf.DType`. Type of the table keys.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
      If true and shared_name is empty, the table is shared
      using the node name.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("mutable_hash_table op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MutableHashTable", key_dtype=key_dtype, value_dtype=value_dtype,
                            container=container, shared_name=shared_name,
                            use_node_name_sharing=use_node_name_sharing,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"))
  _execute.record_gradient(
      "MutableHashTable", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutable_hash_table_eager_fallback(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None, ctx=None):
  raise RuntimeError("mutable_hash_table op does not support eager execution. Arg 'table_handle' is a ref.")

def mutable_hash_table_of_tensors(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], name=None):
  r"""Creates an empty hash table.

  This op creates a mutable hash table, specifying the type of its keys and
  values. Each value must be a vector. Data can be inserted into the table using
  the insert operations. It does not support the initialization operation.

  Args:
    key_dtype: A `tf.DType`. Type of the table keys.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
    value_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("mutable_hash_table_of_tensors op does not support eager execution. Arg 'table_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  if value_shape is None:
    value_shape = []
  value_shape = _execute.make_shape(value_shape, "value_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MutableHashTableOfTensors", key_dtype=key_dtype,
                                     value_dtype=value_dtype,
                                     container=container,
                                     shared_name=shared_name,
                                     use_node_name_sharing=use_node_name_sharing,
                                     value_shape=value_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"), "value_shape",
            _op.get_attr("value_shape"))
  _execute.record_gradient(
      "MutableHashTableOfTensors", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutable_hash_table_of_tensors_eager_fallback(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], name=None, ctx=None):
  raise RuntimeError("mutable_hash_table_of_tensors op does not support eager execution. Arg 'table_handle' is a ref.")

def mutable_hash_table_of_tensors_v2(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], name=None):
  r"""Creates an empty hash table.

  This op creates a mutable hash table, specifying the type of its keys and
  values. Each value must be a vector. Data can be inserted into the table using
  the insert operations. It does not support the initialization operation.

  Args:
    key_dtype: A `tf.DType`. Type of the table keys.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
    value_shape: An optional `tf.TensorShape` or list of `ints`. Defaults to `[]`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MutableHashTableOfTensorsV2", name, _ctx._post_execution_callbacks,
        "container", container, "shared_name", shared_name,
        "use_node_name_sharing", use_node_name_sharing, "key_dtype",
        key_dtype, "value_dtype", value_dtype, "value_shape", value_shape)
      return _result
    except _core._FallbackException:
      try:
        return mutable_hash_table_of_tensors_v2_eager_fallback(
            container=container, shared_name=shared_name,
            use_node_name_sharing=use_node_name_sharing, key_dtype=key_dtype,
            value_dtype=value_dtype, value_shape=value_shape, name=name,
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
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  if value_shape is None:
    value_shape = []
  value_shape = _execute.make_shape(value_shape, "value_shape")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MutableHashTableOfTensorsV2", key_dtype=key_dtype,
                                       value_dtype=value_dtype,
                                       container=container,
                                       shared_name=shared_name,
                                       use_node_name_sharing=use_node_name_sharing,
                                       value_shape=value_shape, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"), "value_shape",
            _op.get_attr("value_shape"))
  _execute.record_gradient(
      "MutableHashTableOfTensorsV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutable_hash_table_of_tensors_v2_eager_fallback(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, value_shape=[], name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mutable_hash_table_of_tensors_v2
  """
  _ctx = ctx if ctx else _context.context()
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  if value_shape is None:
    value_shape = []
  value_shape = _execute.make_shape(value_shape, "value_shape")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name,
  "use_node_name_sharing", use_node_name_sharing, "key_dtype", key_dtype,
  "value_dtype", value_dtype, "value_shape", value_shape)
  _result = _execute.execute(b"MutableHashTableOfTensorsV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "MutableHashTableOfTensorsV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def mutable_hash_table_v2(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None):
  r"""Creates an empty hash table.

  This op creates a mutable hash table, specifying the type of its keys and
  values. Each value must be a scalar. Data can be inserted into the table using
  the insert operations. It does not support the initialization operation.

  Args:
    key_dtype: A `tf.DType`. Type of the table keys.
    value_dtype: A `tf.DType`. Type of the table values.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this table is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this table is shared under the given name across
      multiple sessions.
    use_node_name_sharing: An optional `bool`. Defaults to `False`.
      If true and shared_name is empty, the table is shared
      using the node name.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MutableHashTableV2", name, _ctx._post_execution_callbacks,
        "container", container, "shared_name", shared_name,
        "use_node_name_sharing", use_node_name_sharing, "key_dtype",
        key_dtype, "value_dtype", value_dtype)
      return _result
    except _core._FallbackException:
      try:
        return mutable_hash_table_v2_eager_fallback(
            container=container, shared_name=shared_name,
            use_node_name_sharing=use_node_name_sharing, key_dtype=key_dtype,
            value_dtype=value_dtype, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MutableHashTableV2", key_dtype=key_dtype, value_dtype=value_dtype,
                              container=container, shared_name=shared_name,
                              use_node_name_sharing=use_node_name_sharing,
                              name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "use_node_name_sharing",
            _op.get_attr("use_node_name_sharing"), "key_dtype",
            _op.get_attr("key_dtype"), "value_dtype",
            _op.get_attr("value_dtype"))
  _execute.record_gradient(
      "MutableHashTableV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def mutable_hash_table_v2_eager_fallback(key_dtype, value_dtype, container="", shared_name="", use_node_name_sharing=False, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function mutable_hash_table_v2
  """
  _ctx = ctx if ctx else _context.context()
  key_dtype = _execute.make_type(key_dtype, "key_dtype")
  value_dtype = _execute.make_type(value_dtype, "value_dtype")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if use_node_name_sharing is None:
    use_node_name_sharing = False
  use_node_name_sharing = _execute.make_bool(use_node_name_sharing, "use_node_name_sharing")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name,
  "use_node_name_sharing", use_node_name_sharing, "key_dtype", key_dtype,
  "value_dtype", value_dtype)
  _result = _execute.execute(b"MutableHashTableV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MutableHashTableV2", _inputs_flat, _attrs, _result, name)
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
#   name: "HashTable"
#   output_arg {
#     name: "table_handle"
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
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "HashTableV2"
#   output_arg {
#     name: "table_handle"
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
#   attr {
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "InitializeTable"
#   input_arg {
#     name: "table_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tkey"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "Tval"
#   }
#   attr {
#     name: "Tkey"
#     type: "type"
#   }
#   attr {
#     name: "Tval"
#     type: "type"
#   }
# }
# op {
#   name: "InitializeTableFromTextFile"
#   input_arg {
#     name: "table_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "filename"
#     type: DT_STRING
#   }
#   attr {
#     name: "key_index"
#     type: "int"
#     has_minimum: true
#     minimum: -2
#   }
#   attr {
#     name: "value_index"
#     type: "int"
#     has_minimum: true
#     minimum: -2
#   }
#   attr {
#     name: "vocab_size"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "delimiter"
#     type: "string"
#     default_value {
#       s: "\t"
#     }
#   }
# }
# op {
#   name: "InitializeTableFromTextFileV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "filename"
#     type: DT_STRING
#   }
#   attr {
#     name: "key_index"
#     type: "int"
#     has_minimum: true
#     minimum: -2
#   }
#   attr {
#     name: "value_index"
#     type: "int"
#     has_minimum: true
#     minimum: -2
#   }
#   attr {
#     name: "vocab_size"
#     type: "int"
#     default_value {
#       i: -1
#     }
#     has_minimum: true
#     minimum: -1
#   }
#   attr {
#     name: "delimiter"
#     type: "string"
#     default_value {
#       s: "\t"
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "InitializeTableV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tkey"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "Tval"
#   }
#   attr {
#     name: "Tkey"
#     type: "type"
#   }
#   attr {
#     name: "Tval"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "LookupTableExport"
#   input_arg {
#     name: "table_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "keys"
#     type_attr: "Tkeys"
#   }
#   output_arg {
#     name: "values"
#     type_attr: "Tvalues"
#   }
#   attr {
#     name: "Tkeys"
#     type: "type"
#   }
#   attr {
#     name: "Tvalues"
#     type: "type"
#   }
# }
# op {
#   name: "LookupTableExportV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "keys"
#     type_attr: "Tkeys"
#   }
#   output_arg {
#     name: "values"
#     type_attr: "Tvalues"
#   }
#   attr {
#     name: "Tkeys"
#     type: "type"
#   }
#   attr {
#     name: "Tvalues"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "LookupTableFind"
#   input_arg {
#     name: "table_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tin"
#   }
#   input_arg {
#     name: "default_value"
#     type_attr: "Tout"
#   }
#   output_arg {
#     name: "values"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "type"
#   }
#   attr {
#     name: "Tout"
#     type: "type"
#   }
# }
# op {
#   name: "LookupTableFindV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tin"
#   }
#   input_arg {
#     name: "default_value"
#     type_attr: "Tout"
#   }
#   output_arg {
#     name: "values"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "type"
#   }
#   attr {
#     name: "Tout"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "LookupTableImport"
#   input_arg {
#     name: "table_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tin"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "type"
#   }
#   attr {
#     name: "Tout"
#     type: "type"
#   }
# }
# op {
#   name: "LookupTableImportV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tin"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "type"
#   }
#   attr {
#     name: "Tout"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "LookupTableInsert"
#   input_arg {
#     name: "table_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tin"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "type"
#   }
#   attr {
#     name: "Tout"
#     type: "type"
#   }
# }
# op {
#   name: "LookupTableInsertV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tin"
#   }
#   input_arg {
#     name: "values"
#     type_attr: "Tout"
#   }
#   attr {
#     name: "Tin"
#     type: "type"
#   }
#   attr {
#     name: "Tout"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "LookupTableRemoveV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "keys"
#     type_attr: "Tin"
#   }
#   attr {
#     name: "Tin"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "LookupTableSize"
#   input_arg {
#     name: "table_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT64
#   }
# }
# op {
#   name: "LookupTableSizeV2"
#   input_arg {
#     name: "table_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "size"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "MutableDenseHashTable"
#   input_arg {
#     name: "empty_key"
#     type_attr: "key_dtype"
#   }
#   output_arg {
#     name: "table_handle"
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
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_shape"
#     type: "shape"
#     default_value {
#       shape {
#       }
#     }
#   }
#   attr {
#     name: "initial_num_buckets"
#     type: "int"
#     default_value {
#       i: 131072
#     }
#   }
#   attr {
#     name: "max_load_factor"
#     type: "float"
#     default_value {
#       f: 0.8
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "MutableDenseHashTableV2"
#   input_arg {
#     name: "empty_key"
#     type_attr: "key_dtype"
#   }
#   input_arg {
#     name: "deleted_key"
#     type_attr: "key_dtype"
#   }
#   output_arg {
#     name: "table_handle"
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
#   attr {
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_shape"
#     type: "shape"
#     default_value {
#       shape {
#       }
#     }
#   }
#   attr {
#     name: "initial_num_buckets"
#     type: "int"
#     default_value {
#       i: 131072
#     }
#   }
#   attr {
#     name: "max_load_factor"
#     type: "float"
#     default_value {
#       f: 0.8
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "MutableHashTable"
#   output_arg {
#     name: "table_handle"
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
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
# op {
#   name: "MutableHashTableOfTensors"
#   output_arg {
#     name: "table_handle"
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
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_shape"
#     type: "shape"
#     default_value {
#       shape {
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "MutableHashTableOfTensorsV2"
#   output_arg {
#     name: "table_handle"
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
#   attr {
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_shape"
#     type: "shape"
#     default_value {
#       shape {
#       }
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "MutableHashTableV2"
#   output_arg {
#     name: "table_handle"
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
#   attr {
#     name: "use_node_name_sharing"
#     type: "bool"
#     default_value {
#       b: false
#     }
#   }
#   attr {
#     name: "key_dtype"
#     type: "type"
#   }
#   attr {
#     name: "value_dtype"
#     type: "type"
#   }
#   is_stateful: true
# }
_op_def_lib = _InitOpDefLibrary(b"\n\242\001\n\tHashTable\032\023\n\014table_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\210\001\001\n\241\001\n\013HashTableV2\032\020\n\014table_handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\210\001\001\n`\n\017InitializeTable\022\023\n\014table_handle\030\007\200\001\001\022\014\n\004keys\"\004Tkey\022\016\n\006values\"\004Tval\"\014\n\004Tkey\022\004type\"\014\n\004Tval\022\004type\n\307\001\n\033InitializeTableFromTextFile\022\023\n\014table_handle\030\007\200\001\001\022\014\n\010filename\030\007\"\035\n\tkey_index\022\003int(\0010\376\377\377\377\377\377\377\377\377\001\"\037\n\013value_index\022\003int(\0010\376\377\377\377\377\377\377\377\377\001\"+\n\nvocab_size\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\tdelimiter\022\006string\032\003\022\001\t\n\311\001\n\035InitializeTableFromTextFileV2\022\020\n\014table_handle\030\024\022\014\n\010filename\030\007\"\035\n\tkey_index\022\003int(\0010\376\377\377\377\377\377\377\377\377\001\"\037\n\013value_index\022\003int(\0010\376\377\377\377\377\377\377\377\377\001\"+\n\nvocab_size\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001(\0010\377\377\377\377\377\377\377\377\377\001\"\030\n\tdelimiter\022\006string\032\003\022\001\t\210\001\001\nb\n\021InitializeTableV2\022\020\n\014table_handle\030\024\022\014\n\004keys\"\004Tkey\022\016\n\006values\"\004Tval\"\014\n\004Tkey\022\004type\"\014\n\004Tval\022\004type\210\001\001\nj\n\021LookupTableExport\022\023\n\014table_handle\030\007\200\001\001\032\r\n\004keys\"\005Tkeys\032\021\n\006values\"\007Tvalues\"\r\n\005Tkeys\022\004type\"\017\n\007Tvalues\022\004type\nl\n\023LookupTableExportV2\022\020\n\014table_handle\030\024\032\r\n\004keys\"\005Tkeys\032\021\n\006values\"\007Tvalues\"\r\n\005Tkeys\022\004type\"\017\n\007Tvalues\022\004type\210\001\001\nu\n\017LookupTableFind\022\023\n\014table_handle\030\007\200\001\001\022\013\n\004keys\"\003Tin\022\025\n\rdefault_value\"\004Tout\032\016\n\006values\"\004Tout\"\013\n\003Tin\022\004type\"\014\n\004Tout\022\004type\nw\n\021LookupTableFindV2\022\020\n\014table_handle\030\024\022\013\n\004keys\"\003Tin\022\025\n\rdefault_value\"\004Tout\032\016\n\006values\"\004Tout\"\013\n\003Tin\022\004type\"\014\n\004Tout\022\004type\210\001\001\n`\n\021LookupTableImport\022\023\n\014table_handle\030\007\200\001\001\022\013\n\004keys\"\003Tin\022\016\n\006values\"\004Tout\"\013\n\003Tin\022\004type\"\014\n\004Tout\022\004type\nb\n\023LookupTableImportV2\022\020\n\014table_handle\030\024\022\013\n\004keys\"\003Tin\022\016\n\006values\"\004Tout\"\013\n\003Tin\022\004type\"\014\n\004Tout\022\004type\210\001\001\n`\n\021LookupTableInsert\022\023\n\014table_handle\030\007\200\001\001\022\013\n\004keys\"\003Tin\022\016\n\006values\"\004Tout\"\013\n\003Tin\022\004type\"\014\n\004Tout\022\004type\nb\n\023LookupTableInsertV2\022\020\n\014table_handle\030\024\022\013\n\004keys\"\003Tin\022\016\n\006values\"\004Tout\"\013\n\003Tin\022\004type\"\014\n\004Tout\022\004type\210\001\001\nD\n\023LookupTableRemoveV2\022\020\n\014table_handle\030\024\022\013\n\004keys\"\003Tin\"\013\n\003Tin\022\004type\210\001\001\n0\n\017LookupTableSize\022\023\n\014table_handle\030\007\200\001\001\032\010\n\004size\030\t\n2\n\021LookupTableSizeV2\022\020\n\014table_handle\030\024\032\010\n\004size\030\t\210\001\001\n\243\002\n\025MutableDenseHashTable\022\026\n\tempty_key\"\tkey_dtype\032\023\n\014table_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\"\030\n\013value_shape\022\005shape\032\002:\000\" \n\023initial_num_buckets\022\003int\032\004\030\200\200\010\"\037\n\017max_load_factor\022\005float\032\005%\315\314L?\210\001\001\n\274\002\n\027MutableDenseHashTableV2\022\026\n\tempty_key\"\tkey_dtype\022\030\n\013deleted_key\"\tkey_dtype\032\020\n\014table_handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\"\030\n\013value_shape\022\005shape\032\002:\000\" \n\023initial_num_buckets\022\003int\032\004\030\200\200\010\"\037\n\017max_load_factor\022\005float\032\005%\315\314L?\210\001\001\n\251\001\n\020MutableHashTable\032\023\n\014table_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\210\001\001\n\314\001\n\031MutableHashTableOfTensors\032\023\n\014table_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\"\030\n\013value_shape\022\005shape\032\002:\000\210\001\001\n\313\001\n\033MutableHashTableOfTensorsV2\032\020\n\014table_handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\"\030\n\013value_shape\022\005shape\032\002:\000\210\001\001\n\250\001\n\022MutableHashTableV2\032\020\n\014table_handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"!\n\025use_node_name_sharing\022\004bool\032\002(\000\"\021\n\tkey_dtype\022\004type\"\023\n\013value_dtype\022\004type\210\001\001")
