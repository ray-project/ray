"""Python wrappers around TensorFlow ops.

This file is MACHINE GENERATED! Do not edit.
Original C++ source file: io_ops.cc
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


def fixed_length_record_reader(record_bytes, header_bytes=0, footer_bytes=0, hop_bytes=0, container="", shared_name="", name=None):
  r"""A Reader that outputs fixed-length records from a file.

  Args:
    record_bytes: An `int`. Number of bytes in the record.
    header_bytes: An optional `int`. Defaults to `0`.
      Number of bytes in the header, defaults to 0.
    footer_bytes: An optional `int`. Defaults to `0`.
      Number of bytes in the footer, defaults to 0.
    hop_bytes: An optional `int`. Defaults to `0`.
      Number of bytes to hop before each read. Default of 0 means using
      record_bytes.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("fixed_length_record_reader op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  record_bytes = _execute.make_int(record_bytes, "record_bytes")
  if header_bytes is None:
    header_bytes = 0
  header_bytes = _execute.make_int(header_bytes, "header_bytes")
  if footer_bytes is None:
    footer_bytes = 0
  footer_bytes = _execute.make_int(footer_bytes, "footer_bytes")
  if hop_bytes is None:
    hop_bytes = 0
  hop_bytes = _execute.make_int(hop_bytes, "hop_bytes")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FixedLengthRecordReader", record_bytes=record_bytes,
                                   header_bytes=header_bytes,
                                   footer_bytes=footer_bytes,
                                   hop_bytes=hop_bytes, container=container,
                                   shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("header_bytes", _op.get_attr("header_bytes"), "record_bytes",
            _op.get_attr("record_bytes"), "footer_bytes",
            _op.get_attr("footer_bytes"), "hop_bytes",
            _op.get_attr("hop_bytes"), "container", _op.get_attr("container"),
            "shared_name", _op.get_attr("shared_name"))
  _execute.record_gradient(
      "FixedLengthRecordReader", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fixed_length_record_reader_eager_fallback(record_bytes, header_bytes=0, footer_bytes=0, hop_bytes=0, container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("fixed_length_record_reader op does not support eager execution. Arg 'reader_handle' is a ref.")

def fixed_length_record_reader_v2(record_bytes, header_bytes=0, footer_bytes=0, hop_bytes=0, container="", shared_name="", encoding="", name=None):
  r"""A Reader that outputs fixed-length records from a file.

  Args:
    record_bytes: An `int`. Number of bytes in the record.
    header_bytes: An optional `int`. Defaults to `0`.
      Number of bytes in the header, defaults to 0.
    footer_bytes: An optional `int`. Defaults to `0`.
      Number of bytes in the footer, defaults to 0.
    hop_bytes: An optional `int`. Defaults to `0`.
      Number of bytes to hop before each read. Default of 0 means using
      record_bytes.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    encoding: An optional `string`. Defaults to `""`.
      The type of encoding for the file. Currently ZLIB and GZIP
      are supported. Defaults to none.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "FixedLengthRecordReaderV2", name, _ctx._post_execution_callbacks,
        "header_bytes", header_bytes, "record_bytes", record_bytes,
        "footer_bytes", footer_bytes, "hop_bytes", hop_bytes, "container",
        container, "shared_name", shared_name, "encoding", encoding)
      return _result
    except _core._FallbackException:
      try:
        return fixed_length_record_reader_v2_eager_fallback(
            header_bytes=header_bytes, record_bytes=record_bytes,
            footer_bytes=footer_bytes, hop_bytes=hop_bytes,
            container=container, shared_name=shared_name, encoding=encoding,
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
  record_bytes = _execute.make_int(record_bytes, "record_bytes")
  if header_bytes is None:
    header_bytes = 0
  header_bytes = _execute.make_int(header_bytes, "header_bytes")
  if footer_bytes is None:
    footer_bytes = 0
  footer_bytes = _execute.make_int(footer_bytes, "footer_bytes")
  if hop_bytes is None:
    hop_bytes = 0
  hop_bytes = _execute.make_int(hop_bytes, "hop_bytes")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if encoding is None:
    encoding = ""
  encoding = _execute.make_str(encoding, "encoding")
  _, _, _op = _op_def_lib._apply_op_helper(
        "FixedLengthRecordReaderV2", record_bytes=record_bytes,
                                     header_bytes=header_bytes,
                                     footer_bytes=footer_bytes,
                                     hop_bytes=hop_bytes, container=container,
                                     shared_name=shared_name,
                                     encoding=encoding, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("header_bytes", _op.get_attr("header_bytes"), "record_bytes",
            _op.get_attr("record_bytes"), "footer_bytes",
            _op.get_attr("footer_bytes"), "hop_bytes",
            _op.get_attr("hop_bytes"), "container", _op.get_attr("container"),
            "shared_name", _op.get_attr("shared_name"), "encoding",
            _op.get_attr("encoding"))
  _execute.record_gradient(
      "FixedLengthRecordReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def fixed_length_record_reader_v2_eager_fallback(record_bytes, header_bytes=0, footer_bytes=0, hop_bytes=0, container="", shared_name="", encoding="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function fixed_length_record_reader_v2
  """
  _ctx = ctx if ctx else _context.context()
  record_bytes = _execute.make_int(record_bytes, "record_bytes")
  if header_bytes is None:
    header_bytes = 0
  header_bytes = _execute.make_int(header_bytes, "header_bytes")
  if footer_bytes is None:
    footer_bytes = 0
  footer_bytes = _execute.make_int(footer_bytes, "footer_bytes")
  if hop_bytes is None:
    hop_bytes = 0
  hop_bytes = _execute.make_int(hop_bytes, "hop_bytes")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if encoding is None:
    encoding = ""
  encoding = _execute.make_str(encoding, "encoding")
  _inputs_flat = []
  _attrs = ("header_bytes", header_bytes, "record_bytes", record_bytes,
  "footer_bytes", footer_bytes, "hop_bytes", hop_bytes, "container",
  container, "shared_name", shared_name, "encoding", encoding)
  _result = _execute.execute(b"FixedLengthRecordReaderV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "FixedLengthRecordReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def identity_reader(container="", shared_name="", name=None):
  r"""A Reader that outputs the queued work as both the key and value.

  To use, enqueue strings in a Queue.  ReaderRead will take the front
  work string and output (work, work).

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("identity_reader op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "IdentityReader", container=container, shared_name=shared_name,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "IdentityReader", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def identity_reader_eager_fallback(container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("identity_reader op does not support eager execution. Arg 'reader_handle' is a ref.")

def identity_reader_v2(container="", shared_name="", name=None):
  r"""A Reader that outputs the queued work as both the key and value.

  To use, enqueue strings in a Queue.  ReaderRead will take the front
  work string and output (work, work).

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "IdentityReaderV2", name, _ctx._post_execution_callbacks, "container",
        container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return identity_reader_v2_eager_fallback(
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
        "IdentityReaderV2", container=container, shared_name=shared_name,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "IdentityReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def identity_reader_v2_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function identity_reader_v2
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
  _result = _execute.execute(b"IdentityReaderV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "IdentityReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def lmdb_reader(container="", shared_name="", name=None):
  r"""A Reader that outputs the records from a LMDB file.

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("lmdb_reader op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "LMDBReader", container=container, shared_name=shared_name, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "LMDBReader", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def lmdb_reader_eager_fallback(container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("lmdb_reader op does not support eager execution. Arg 'reader_handle' is a ref.")

@_dispatch.add_dispatch_list
@tf_export('io.matching_files', v1=['io.matching_files', 'matching_files'])
@deprecated_endpoints('matching_files')
def matching_files(pattern, name=None):
  r"""Returns the set of files matching one or more glob patterns.

  Note that this routine only supports wildcard characters in the
  basename portion of the pattern, not in the directory portion.
  Note also that the order of filenames returned can be non-deterministic.

  Args:
    pattern: A `Tensor` of type `string`.
      Shell wildcard pattern(s). Scalar or vector of type string.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MatchingFiles", name, _ctx._post_execution_callbacks, pattern)
      return _result
    except _core._FallbackException:
      try:
        return matching_files_eager_fallback(
            pattern, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              matching_files, pattern=pattern, name=name)
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
        "MatchingFiles", pattern=pattern, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          matching_files, pattern=pattern, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "MatchingFiles", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def matching_files_eager_fallback(pattern, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function matching_files
  """
  _ctx = ctx if ctx else _context.context()
  pattern = _ops.convert_to_tensor(pattern, _dtypes.string)
  _inputs_flat = [pattern]
  _attrs = None
  _result = _execute.execute(b"MatchingFiles", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "MatchingFiles", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def merge_v2_checkpoints(checkpoint_prefixes, destination_prefix, delete_old_dirs=True, name=None):
  r"""V2 format specific: merges the metadata files of sharded checkpoints.  The

  result is one logical checkpoint, with one physical metadata file and renamed
  data files.

  Intended for "grouping" multiple checkpoints in a sharded checkpoint setup.

  If delete_old_dirs is true, attempts to delete recursively the dirname of each
  path in the input checkpoint_prefixes.  This is useful when those paths are non
  user-facing temporary locations.

  Args:
    checkpoint_prefixes: A `Tensor` of type `string`.
      prefixes of V2 checkpoints to merge.
    destination_prefix: A `Tensor` of type `string`.
      scalar.  The desired final prefix.  Allowed to be the same
      as one of the checkpoint_prefixes.
    delete_old_dirs: An optional `bool`. Defaults to `True`. see above.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "MergeV2Checkpoints", name, _ctx._post_execution_callbacks,
        checkpoint_prefixes, destination_prefix, "delete_old_dirs",
        delete_old_dirs)
      return _result
    except _core._FallbackException:
      try:
        return merge_v2_checkpoints_eager_fallback(
            checkpoint_prefixes, destination_prefix,
            delete_old_dirs=delete_old_dirs, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  if delete_old_dirs is None:
    delete_old_dirs = True
  delete_old_dirs = _execute.make_bool(delete_old_dirs, "delete_old_dirs")
  _, _, _op = _op_def_lib._apply_op_helper(
        "MergeV2Checkpoints", checkpoint_prefixes=checkpoint_prefixes,
                              destination_prefix=destination_prefix,
                              delete_old_dirs=delete_old_dirs, name=name)
  return _op
  _result = None
  return _result



def merge_v2_checkpoints_eager_fallback(checkpoint_prefixes, destination_prefix, delete_old_dirs=True, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function merge_v2_checkpoints
  """
  _ctx = ctx if ctx else _context.context()
  if delete_old_dirs is None:
    delete_old_dirs = True
  delete_old_dirs = _execute.make_bool(delete_old_dirs, "delete_old_dirs")
  checkpoint_prefixes = _ops.convert_to_tensor(checkpoint_prefixes, _dtypes.string)
  destination_prefix = _ops.convert_to_tensor(destination_prefix, _dtypes.string)
  _inputs_flat = [checkpoint_prefixes, destination_prefix]
  _attrs = ("delete_old_dirs", delete_old_dirs)
  _result = _execute.execute(b"MergeV2Checkpoints", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


@_dispatch.add_dispatch_list
@tf_export('io.read_file', v1=['io.read_file', 'read_file'])
@deprecated_endpoints('read_file')
def read_file(filename, name=None):
  r"""Reads and outputs the entire contents of the input filename.

  Args:
    filename: A `Tensor` of type `string`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ReadFile",
        name, _ctx._post_execution_callbacks, filename)
      return _result
    except _core._FallbackException:
      try:
        return read_file_eager_fallback(
            filename, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              read_file, filename=filename, name=name)
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
        "ReadFile", filename=filename, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          read_file, filename=filename, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReadFile", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def read_file_eager_fallback(filename, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function read_file
  """
  _ctx = ctx if ctx else _context.context()
  filename = _ops.convert_to_tensor(filename, _dtypes.string)
  _inputs_flat = [filename]
  _attrs = None
  _result = _execute.execute(b"ReadFile", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReadFile", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def reader_num_records_produced(reader_handle, name=None):
  r"""Returns the number of records this Reader has produced.

  This is the same as the number of ReaderRead executions that have
  succeeded.

  Args:
    reader_handle: A `Tensor` of type mutable `string`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("reader_num_records_produced op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReaderNumRecordsProduced", reader_handle=reader_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderNumRecordsProduced", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reader_num_records_produced_eager_fallback(reader_handle, name=None, ctx=None):
  raise RuntimeError("reader_num_records_produced op does not support eager execution. Arg 'reader_handle' is a ref.")

def reader_num_records_produced_v2(reader_handle, name=None):
  r"""Returns the number of records this Reader has produced.

  This is the same as the number of ReaderRead executions that have
  succeeded.

  Args:
    reader_handle: A `Tensor` of type `resource`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReaderNumRecordsProducedV2", name, _ctx._post_execution_callbacks,
        reader_handle)
      return _result
    except _core._FallbackException:
      try:
        return reader_num_records_produced_v2_eager_fallback(
            reader_handle, name=name, ctx=_ctx)
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
        "ReaderNumRecordsProducedV2", reader_handle=reader_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderNumRecordsProducedV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reader_num_records_produced_v2_eager_fallback(reader_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reader_num_records_produced_v2
  """
  _ctx = ctx if ctx else _context.context()
  reader_handle = _ops.convert_to_tensor(reader_handle, _dtypes.resource)
  _inputs_flat = [reader_handle]
  _attrs = None
  _result = _execute.execute(b"ReaderNumRecordsProducedV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ReaderNumRecordsProducedV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def reader_num_work_units_completed(reader_handle, name=None):
  r"""Returns the number of work units this Reader has finished processing.

  Args:
    reader_handle: A `Tensor` of type mutable `string`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("reader_num_work_units_completed op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReaderNumWorkUnitsCompleted", reader_handle=reader_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderNumWorkUnitsCompleted", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reader_num_work_units_completed_eager_fallback(reader_handle, name=None, ctx=None):
  raise RuntimeError("reader_num_work_units_completed op does not support eager execution. Arg 'reader_handle' is a ref.")

def reader_num_work_units_completed_v2(reader_handle, name=None):
  r"""Returns the number of work units this Reader has finished processing.

  Args:
    reader_handle: A `Tensor` of type `resource`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `int64`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReaderNumWorkUnitsCompletedV2", name, _ctx._post_execution_callbacks,
        reader_handle)
      return _result
    except _core._FallbackException:
      try:
        return reader_num_work_units_completed_v2_eager_fallback(
            reader_handle, name=name, ctx=_ctx)
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
        "ReaderNumWorkUnitsCompletedV2", reader_handle=reader_handle,
                                         name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderNumWorkUnitsCompletedV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reader_num_work_units_completed_v2_eager_fallback(reader_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reader_num_work_units_completed_v2
  """
  _ctx = ctx if ctx else _context.context()
  reader_handle = _ops.convert_to_tensor(reader_handle, _dtypes.resource)
  _inputs_flat = [reader_handle]
  _attrs = None
  _result = _execute.execute(b"ReaderNumWorkUnitsCompletedV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ReaderNumWorkUnitsCompletedV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


_reader_read_outputs = ["key", "value"]
_ReaderReadOutput = _collections.namedtuple(
    "ReaderRead", _reader_read_outputs)


def reader_read(reader_handle, queue_handle, name=None):
  r"""Returns the next record (key, value pair) produced by a Reader.

  Will dequeue from the input queue if necessary (e.g. when the
  Reader needs to start reading from a new file since it has finished
  with the previous file).

  Args:
    reader_handle: A `Tensor` of type mutable `string`. Handle to a Reader.
    queue_handle: A `Tensor` of type mutable `string`.
      Handle to a Queue, with string work items.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (key, value).

    key: A `Tensor` of type `string`.
    value: A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("reader_read op does not support eager execution. Arg 'queue_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReaderRead", reader_handle=reader_handle, queue_handle=queue_handle,
                      name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderRead", _inputs_flat, _attrs, _result, name)
  _result = _ReaderReadOutput._make(_result)
  return _result



def reader_read_eager_fallback(reader_handle, queue_handle, name=None, ctx=None):
  raise RuntimeError("reader_read op does not support eager execution. Arg 'queue_handle' is a ref.")

_reader_read_up_to_outputs = ["keys", "values"]
_ReaderReadUpToOutput = _collections.namedtuple(
    "ReaderReadUpTo", _reader_read_up_to_outputs)


def reader_read_up_to(reader_handle, queue_handle, num_records, name=None):
  r"""Returns up to `num_records` (key, value) pairs produced by a Reader.

  Will dequeue from the input queue if necessary (e.g. when the
  Reader needs to start reading from a new file since it has finished
  with the previous file).
  It may return less than `num_records` even before the last batch.

  Args:
    reader_handle: A `Tensor` of type mutable `string`. Handle to a `Reader`.
    queue_handle: A `Tensor` of type mutable `string`.
      Handle to a `Queue`, with string work items.
    num_records: A `Tensor` of type `int64`.
      number of records to read from `Reader`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (keys, values).

    keys: A `Tensor` of type `string`.
    values: A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("reader_read_up_to op does not support eager execution. Arg 'queue_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReaderReadUpTo", reader_handle=reader_handle,
                          queue_handle=queue_handle, num_records=num_records,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderReadUpTo", _inputs_flat, _attrs, _result, name)
  _result = _ReaderReadUpToOutput._make(_result)
  return _result



def reader_read_up_to_eager_fallback(reader_handle, queue_handle, num_records, name=None, ctx=None):
  raise RuntimeError("reader_read_up_to op does not support eager execution. Arg 'queue_handle' is a ref.")

_reader_read_up_to_v2_outputs = ["keys", "values"]
_ReaderReadUpToV2Output = _collections.namedtuple(
    "ReaderReadUpToV2", _reader_read_up_to_v2_outputs)


def reader_read_up_to_v2(reader_handle, queue_handle, num_records, name=None):
  r"""Returns up to `num_records` (key, value) pairs produced by a Reader.

  Will dequeue from the input queue if necessary (e.g. when the
  Reader needs to start reading from a new file since it has finished
  with the previous file).
  It may return less than `num_records` even before the last batch.

  Args:
    reader_handle: A `Tensor` of type `resource`. Handle to a `Reader`.
    queue_handle: A `Tensor` of type `resource`.
      Handle to a `Queue`, with string work items.
    num_records: A `Tensor` of type `int64`.
      number of records to read from `Reader`.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (keys, values).

    keys: A `Tensor` of type `string`.
    values: A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReaderReadUpToV2", name, _ctx._post_execution_callbacks,
        reader_handle, queue_handle, num_records)
      _result = _ReaderReadUpToV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return reader_read_up_to_v2_eager_fallback(
            reader_handle, queue_handle, num_records, name=name, ctx=_ctx)
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
        "ReaderReadUpToV2", reader_handle=reader_handle,
                            queue_handle=queue_handle,
                            num_records=num_records, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderReadUpToV2", _inputs_flat, _attrs, _result, name)
  _result = _ReaderReadUpToV2Output._make(_result)
  return _result



def reader_read_up_to_v2_eager_fallback(reader_handle, queue_handle, num_records, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reader_read_up_to_v2
  """
  _ctx = ctx if ctx else _context.context()
  reader_handle = _ops.convert_to_tensor(reader_handle, _dtypes.resource)
  queue_handle = _ops.convert_to_tensor(queue_handle, _dtypes.resource)
  num_records = _ops.convert_to_tensor(num_records, _dtypes.int64)
  _inputs_flat = [reader_handle, queue_handle, num_records]
  _attrs = None
  _result = _execute.execute(b"ReaderReadUpToV2", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReaderReadUpToV2", _inputs_flat, _attrs, _result, name)
  _result = _ReaderReadUpToV2Output._make(_result)
  return _result


_reader_read_v2_outputs = ["key", "value"]
_ReaderReadV2Output = _collections.namedtuple(
    "ReaderReadV2", _reader_read_v2_outputs)


def reader_read_v2(reader_handle, queue_handle, name=None):
  r"""Returns the next record (key, value pair) produced by a Reader.

  Will dequeue from the input queue if necessary (e.g. when the
  Reader needs to start reading from a new file since it has finished
  with the previous file).

  Args:
    reader_handle: A `Tensor` of type `resource`. Handle to a Reader.
    queue_handle: A `Tensor` of type `resource`.
      Handle to a Queue, with string work items.
    name: A name for the operation (optional).

  Returns:
    A tuple of `Tensor` objects (key, value).

    key: A `Tensor` of type `string`.
    value: A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "ReaderReadV2",
        name, _ctx._post_execution_callbacks, reader_handle, queue_handle)
      _result = _ReaderReadV2Output._make(_result)
      return _result
    except _core._FallbackException:
      try:
        return reader_read_v2_eager_fallback(
            reader_handle, queue_handle, name=name, ctx=_ctx)
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
        "ReaderReadV2", reader_handle=reader_handle,
                        queue_handle=queue_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderReadV2", _inputs_flat, _attrs, _result, name)
  _result = _ReaderReadV2Output._make(_result)
  return _result



def reader_read_v2_eager_fallback(reader_handle, queue_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reader_read_v2
  """
  _ctx = ctx if ctx else _context.context()
  reader_handle = _ops.convert_to_tensor(reader_handle, _dtypes.resource)
  queue_handle = _ops.convert_to_tensor(queue_handle, _dtypes.resource)
  _inputs_flat = [reader_handle, queue_handle]
  _attrs = None
  _result = _execute.execute(b"ReaderReadV2", 2, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ReaderReadV2", _inputs_flat, _attrs, _result, name)
  _result = _ReaderReadV2Output._make(_result)
  return _result


def reader_reset(reader_handle, name=None):
  r"""Restore a Reader to its initial clean state.

  Args:
    reader_handle: A `Tensor` of type mutable `string`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("reader_reset op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReaderReset", reader_handle=reader_handle, name=name)
  return _op
  _result = None
  return _result



def reader_reset_eager_fallback(reader_handle, name=None, ctx=None):
  raise RuntimeError("reader_reset op does not support eager execution. Arg 'reader_handle' is a ref.")

def reader_reset_v2(reader_handle, name=None):
  r"""Restore a Reader to its initial clean state.

  Args:
    reader_handle: A `Tensor` of type `resource`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReaderResetV2", name, _ctx._post_execution_callbacks, reader_handle)
      return _result
    except _core._FallbackException:
      try:
        return reader_reset_v2_eager_fallback(
            reader_handle, name=name, ctx=_ctx)
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
        "ReaderResetV2", reader_handle=reader_handle, name=name)
  return _op
  _result = None
  return _result



def reader_reset_v2_eager_fallback(reader_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reader_reset_v2
  """
  _ctx = ctx if ctx else _context.context()
  reader_handle = _ops.convert_to_tensor(reader_handle, _dtypes.resource)
  _inputs_flat = [reader_handle]
  _attrs = None
  _result = _execute.execute(b"ReaderResetV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def reader_restore_state(reader_handle, state, name=None):
  r"""Restore a reader to a previously saved state.

  Not all Readers support being restored, so this can produce an
  Unimplemented error.

  Args:
    reader_handle: A `Tensor` of type mutable `string`. Handle to a Reader.
    state: A `Tensor` of type `string`.
      Result of a ReaderSerializeState of a Reader with type
      matching reader_handle.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("reader_restore_state op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReaderRestoreState", reader_handle=reader_handle, state=state,
                              name=name)
  return _op
  _result = None
  return _result



def reader_restore_state_eager_fallback(reader_handle, state, name=None, ctx=None):
  raise RuntimeError("reader_restore_state op does not support eager execution. Arg 'reader_handle' is a ref.")

def reader_restore_state_v2(reader_handle, state, name=None):
  r"""Restore a reader to a previously saved state.

  Not all Readers support being restored, so this can produce an
  Unimplemented error.

  Args:
    reader_handle: A `Tensor` of type `resource`. Handle to a Reader.
    state: A `Tensor` of type `string`.
      Result of a ReaderSerializeState of a Reader with type
      matching reader_handle.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReaderRestoreStateV2", name, _ctx._post_execution_callbacks,
        reader_handle, state)
      return _result
    except _core._FallbackException:
      try:
        return reader_restore_state_v2_eager_fallback(
            reader_handle, state, name=name, ctx=_ctx)
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
        "ReaderRestoreStateV2", reader_handle=reader_handle, state=state,
                                name=name)
  return _op
  _result = None
  return _result



def reader_restore_state_v2_eager_fallback(reader_handle, state, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reader_restore_state_v2
  """
  _ctx = ctx if ctx else _context.context()
  reader_handle = _ops.convert_to_tensor(reader_handle, _dtypes.resource)
  state = _ops.convert_to_tensor(state, _dtypes.string)
  _inputs_flat = [reader_handle, state]
  _attrs = None
  _result = _execute.execute(b"ReaderRestoreStateV2", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def reader_serialize_state(reader_handle, name=None):
  r"""Produce a string tensor that encodes the state of a Reader.

  Not all Readers support being serialized, so this can produce an
  Unimplemented error.

  Args:
    reader_handle: A `Tensor` of type mutable `string`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("reader_serialize_state op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  _, _, _op = _op_def_lib._apply_op_helper(
        "ReaderSerializeState", reader_handle=reader_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderSerializeState", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reader_serialize_state_eager_fallback(reader_handle, name=None, ctx=None):
  raise RuntimeError("reader_serialize_state op does not support eager execution. Arg 'reader_handle' is a ref.")

def reader_serialize_state_v2(reader_handle, name=None):
  r"""Produce a string tensor that encodes the state of a Reader.

  Not all Readers support being serialized, so this can produce an
  Unimplemented error.

  Args:
    reader_handle: A `Tensor` of type `resource`. Handle to a Reader.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ReaderSerializeStateV2", name, _ctx._post_execution_callbacks,
        reader_handle)
      return _result
    except _core._FallbackException:
      try:
        return reader_serialize_state_v2_eager_fallback(
            reader_handle, name=name, ctx=_ctx)
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
        "ReaderSerializeStateV2", reader_handle=reader_handle, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ReaderSerializeStateV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def reader_serialize_state_v2_eager_fallback(reader_handle, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function reader_serialize_state_v2
  """
  _ctx = ctx if ctx else _context.context()
  reader_handle = _ops.convert_to_tensor(reader_handle, _dtypes.resource)
  _inputs_flat = [reader_handle]
  _attrs = None
  _result = _execute.execute(b"ReaderSerializeStateV2", 1,
                             inputs=_inputs_flat, attrs=_attrs, ctx=_ctx,
                             name=name)
  _execute.record_gradient(
      "ReaderSerializeStateV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def restore(file_pattern, tensor_name, dt, preferred_shard=-1, name=None):
  r"""Restores a tensor from checkpoint files.

  Reads a tensor stored in one or several files. If there are several files (for
  instance because a tensor was saved as slices), `file_pattern` may contain
  wildcard symbols (`*` and `?`) in the filename portion only, not in the
  directory portion.

  If a `file_pattern` matches several files, `preferred_shard` can be used to hint
  in which file the requested tensor is likely to be found. This op will first
  open the file at index `preferred_shard` in the list of matching files and try
  to restore tensors from that file.  Only if some tensors or tensor slices are
  not found in that first file, then the Op opens all the files. Setting
  `preferred_shard` to match the value passed as the `shard` input
  of a matching `Save` Op may speed up Restore.  This attribute only affects
  performance, not correctness.  The default value -1 means files are processed in
  order.

  See also `RestoreSlice`.

  Args:
    file_pattern: A `Tensor` of type `string`.
      Must have a single element. The pattern of the files from
      which we read the tensor.
    tensor_name: A `Tensor` of type `string`.
      Must have a single element. The name of the tensor to be
      restored.
    dt: A `tf.DType`. The type of the tensor to be restored.
    preferred_shard: An optional `int`. Defaults to `-1`.
      Index of file to open first if multiple files match
      `file_pattern`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dt`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Restore",
        name, _ctx._post_execution_callbacks, file_pattern, tensor_name, "dt",
        dt, "preferred_shard", preferred_shard)
      return _result
    except _core._FallbackException:
      try:
        return restore_eager_fallback(
            file_pattern, tensor_name, dt=dt, preferred_shard=preferred_shard,
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
  dt = _execute.make_type(dt, "dt")
  if preferred_shard is None:
    preferred_shard = -1
  preferred_shard = _execute.make_int(preferred_shard, "preferred_shard")
  _, _, _op = _op_def_lib._apply_op_helper(
        "Restore", file_pattern=file_pattern, tensor_name=tensor_name, dt=dt,
                   preferred_shard=preferred_shard, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dt", _op.get_attr("dt"), "preferred_shard",
            _op.get_attr("preferred_shard"))
  _execute.record_gradient(
      "Restore", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def restore_eager_fallback(file_pattern, tensor_name, dt, preferred_shard=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function restore
  """
  _ctx = ctx if ctx else _context.context()
  dt = _execute.make_type(dt, "dt")
  if preferred_shard is None:
    preferred_shard = -1
  preferred_shard = _execute.make_int(preferred_shard, "preferred_shard")
  file_pattern = _ops.convert_to_tensor(file_pattern, _dtypes.string)
  tensor_name = _ops.convert_to_tensor(tensor_name, _dtypes.string)
  _inputs_flat = [file_pattern, tensor_name]
  _attrs = ("dt", dt, "preferred_shard", preferred_shard)
  _result = _execute.execute(b"Restore", 1, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _execute.record_gradient(
      "Restore", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def restore_slice(file_pattern, tensor_name, shape_and_slice, dt, preferred_shard=-1, name=None):
  r"""Restores a tensor from checkpoint files.

  This is like `Restore` except that restored tensor can be listed as filling
  only a slice of a larger tensor.  `shape_and_slice` specifies the shape of the
  larger tensor and the slice that the restored tensor covers.

  The `shape_and_slice` input has the same format as the
  elements of the `shapes_and_slices` input of the `SaveSlices` op.

  Args:
    file_pattern: A `Tensor` of type `string`.
      Must have a single element. The pattern of the files from
      which we read the tensor.
    tensor_name: A `Tensor` of type `string`.
      Must have a single element. The name of the tensor to be
      restored.
    shape_and_slice: A `Tensor` of type `string`.
      Scalar. The shapes and slice specifications to use when
      restoring a tensors.
    dt: A `tf.DType`. The type of the tensor to be restored.
    preferred_shard: An optional `int`. Defaults to `-1`.
      Index of file to open first if multiple files match
      `file_pattern`. See the documentation for `Restore`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `dt`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RestoreSlice",
        name, _ctx._post_execution_callbacks, file_pattern, tensor_name,
        shape_and_slice, "dt", dt, "preferred_shard", preferred_shard)
      return _result
    except _core._FallbackException:
      try:
        return restore_slice_eager_fallback(
            file_pattern, tensor_name, shape_and_slice, dt=dt,
            preferred_shard=preferred_shard, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
    except _core._NotOkStatusException as e:
      if name is not None:
        message = e.message + " name: " + name
      else:
        message = e.message
      _six.raise_from(_core._status_to_exception(e.code, message), None)
  # Add nodes to the TensorFlow graph.
  dt = _execute.make_type(dt, "dt")
  if preferred_shard is None:
    preferred_shard = -1
  preferred_shard = _execute.make_int(preferred_shard, "preferred_shard")
  _, _, _op = _op_def_lib._apply_op_helper(
        "RestoreSlice", file_pattern=file_pattern, tensor_name=tensor_name,
                        shape_and_slice=shape_and_slice, dt=dt,
                        preferred_shard=preferred_shard, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("dt", _op.get_attr("dt"), "preferred_shard",
            _op.get_attr("preferred_shard"))
  _execute.record_gradient(
      "RestoreSlice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def restore_slice_eager_fallback(file_pattern, tensor_name, shape_and_slice, dt, preferred_shard=-1, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function restore_slice
  """
  _ctx = ctx if ctx else _context.context()
  dt = _execute.make_type(dt, "dt")
  if preferred_shard is None:
    preferred_shard = -1
  preferred_shard = _execute.make_int(preferred_shard, "preferred_shard")
  file_pattern = _ops.convert_to_tensor(file_pattern, _dtypes.string)
  tensor_name = _ops.convert_to_tensor(tensor_name, _dtypes.string)
  shape_and_slice = _ops.convert_to_tensor(shape_and_slice, _dtypes.string)
  _inputs_flat = [file_pattern, tensor_name, shape_and_slice]
  _attrs = ("dt", dt, "preferred_shard", preferred_shard)
  _result = _execute.execute(b"RestoreSlice", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RestoreSlice", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def restore_v2(prefix, tensor_names, shape_and_slices, dtypes, name=None):
  r"""Restores tensors from a V2 checkpoint.

  For backward compatibility with the V1 format, this Op currently allows
  restoring from a V1 checkpoint as well:
    - This Op first attempts to find the V2 index file pointed to by "prefix", and
      if found proceed to read it as a V2 checkpoint;
    - Otherwise the V1 read path is invoked.
  Relying on this behavior is not recommended, as the ability to fall back to read
  V1 might be deprecated and eventually removed.

  By default, restores the named tensors in full.  If the caller wishes to restore
  specific slices of stored tensors, "shape_and_slices" should be non-empty
  strings and correspondingly well-formed.

  Callers must ensure all the named tensors are indeed stored in the checkpoint.

  Args:
    prefix: A `Tensor` of type `string`.
      Must have a single element.  The prefix of a V2 checkpoint.
    tensor_names: A `Tensor` of type `string`.
      shape {N}.  The names of the tensors to be restored.
    shape_and_slices: A `Tensor` of type `string`.
      shape {N}.  The slice specs of the tensors to be restored.
      Empty strings indicate that they are non-partitioned tensors.
    dtypes: A list of `tf.DTypes` that has length `>= 1`.
      shape {N}.  The list of expected dtype for the tensors.  Must match
      those stored in the checkpoint.
    name: A name for the operation (optional).

  Returns:
    A list of `Tensor` objects of type `dtypes`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "RestoreV2",
        name, _ctx._post_execution_callbacks, prefix, tensor_names,
        shape_and_slices, "dtypes", dtypes)
      return _result
    except _core._FallbackException:
      try:
        return restore_v2_eager_fallback(
            prefix, tensor_names, shape_and_slices, dtypes=dtypes, name=name,
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
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'restore_v2' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  _, _, _op = _op_def_lib._apply_op_helper(
        "RestoreV2", prefix=prefix, tensor_names=tensor_names,
                     shape_and_slices=shape_and_slices, dtypes=dtypes,
                     name=name)
  _result = _op.outputs[:]
  if not _result:
    return _op
  _inputs_flat = _op.inputs
  _attrs = ("dtypes", _op.get_attr("dtypes"))
  _execute.record_gradient(
      "RestoreV2", _inputs_flat, _attrs, _result, name)
  return _result



def restore_v2_eager_fallback(prefix, tensor_names, shape_and_slices, dtypes, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function restore_v2
  """
  _ctx = ctx if ctx else _context.context()
  if not isinstance(dtypes, (list, tuple)):
    raise TypeError(
        "Expected list for 'dtypes' argument to "
        "'restore_v2' Op, not %r." % dtypes)
  dtypes = [_execute.make_type(_t, "dtypes") for _t in dtypes]
  prefix = _ops.convert_to_tensor(prefix, _dtypes.string)
  tensor_names = _ops.convert_to_tensor(tensor_names, _dtypes.string)
  shape_and_slices = _ops.convert_to_tensor(shape_and_slices, _dtypes.string)
  _inputs_flat = [prefix, tensor_names, shape_and_slices]
  _attrs = ("dtypes", dtypes)
  _result = _execute.execute(b"RestoreV2", len(dtypes), inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "RestoreV2", _inputs_flat, _attrs, _result, name)
  return _result


def save(filename, tensor_names, data, name=None):
  r"""Saves the input tensors to disk.

  The size of `tensor_names` must match the number of tensors in `data`. `data[i]`
  is written to `filename` with name `tensor_names[i]`.

  See also `SaveSlices`.

  Args:
    filename: A `Tensor` of type `string`.
      Must have a single element. The name of the file to which we write
      the tensor.
    tensor_names: A `Tensor` of type `string`.
      Shape `[N]`. The names of the tensors to be saved.
    data: A list of `Tensor` objects. `N` tensors to save.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "Save", name,
        _ctx._post_execution_callbacks, filename, tensor_names, data)
      return _result
    except _core._FallbackException:
      try:
        return save_eager_fallback(
            filename, tensor_names, data, name=name, ctx=_ctx)
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
        "Save", filename=filename, tensor_names=tensor_names, data=data,
                name=name)
  return _op
  _result = None
  return _result



def save_eager_fallback(filename, tensor_names, data, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function save
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, data = _execute.convert_to_mixed_eager_tensors(data, _ctx)
  filename = _ops.convert_to_tensor(filename, _dtypes.string)
  tensor_names = _ops.convert_to_tensor(tensor_names, _dtypes.string)
  _inputs_flat = [filename, tensor_names] + list(data)
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"Save", 0, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _result = None
  return _result


def save_slices(filename, tensor_names, shapes_and_slices, data, name=None):
  r"""Saves input tensors slices to disk.

  This is like `Save` except that tensors can be listed in the saved file as being
  a slice of a larger tensor.  `shapes_and_slices` specifies the shape of the
  larger tensor and the slice that this tensor covers. `shapes_and_slices` must
  have as many elements as `tensor_names`.

  Elements of the `shapes_and_slices` input must either be:

  *  The empty string, in which case the corresponding tensor is
     saved normally.
  *  A string of the form `dim0 dim1 ... dimN-1 slice-spec` where the
     `dimI` are the dimensions of the larger tensor and `slice-spec`
     specifies what part is covered by the tensor to save.

  `slice-spec` itself is a `:`-separated list: `slice0:slice1:...:sliceN-1`
  where each `sliceI` is either:

  *  The string `-` meaning that the slice covers all indices of this dimension
  *  `start,length` where `start` and `length` are integers.  In that
     case the slice covers `length` indices starting at `start`.

  See also `Save`.

  Args:
    filename: A `Tensor` of type `string`.
      Must have a single element. The name of the file to which we write the
      tensor.
    tensor_names: A `Tensor` of type `string`.
      Shape `[N]`. The names of the tensors to be saved.
    shapes_and_slices: A `Tensor` of type `string`.
      Shape `[N]`.  The shapes and slice specifications to use when
      saving the tensors.
    data: A list of `Tensor` objects. `N` tensors to save.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SaveSlices",
        name, _ctx._post_execution_callbacks, filename, tensor_names,
        shapes_and_slices, data)
      return _result
    except _core._FallbackException:
      try:
        return save_slices_eager_fallback(
            filename, tensor_names, shapes_and_slices, data, name=name,
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
        "SaveSlices", filename=filename, tensor_names=tensor_names,
                      shapes_and_slices=shapes_and_slices, data=data,
                      name=name)
  return _op
  _result = None
  return _result



def save_slices_eager_fallback(filename, tensor_names, shapes_and_slices, data, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function save_slices
  """
  _ctx = ctx if ctx else _context.context()
  _attr_T, data = _execute.convert_to_mixed_eager_tensors(data, _ctx)
  filename = _ops.convert_to_tensor(filename, _dtypes.string)
  tensor_names = _ops.convert_to_tensor(tensor_names, _dtypes.string)
  shapes_and_slices = _ops.convert_to_tensor(shapes_and_slices, _dtypes.string)
  _inputs_flat = [filename, tensor_names, shapes_and_slices] + list(data)
  _attrs = ("T", _attr_T)
  _result = _execute.execute(b"SaveSlices", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result


def save_v2(prefix, tensor_names, shape_and_slices, tensors, name=None):
  r"""Saves tensors in V2 checkpoint format.

  By default, saves the named tensors in full.  If the caller wishes to save
  specific slices of full tensors, "shape_and_slices" should be non-empty strings
  and correspondingly well-formed.

  Args:
    prefix: A `Tensor` of type `string`.
      Must have a single element. The prefix of the V2 checkpoint to which we
      write the tensors.
    tensor_names: A `Tensor` of type `string`.
      shape {N}. The names of the tensors to be saved.
    shape_and_slices: A `Tensor` of type `string`.
      shape {N}.  The slice specs of the tensors to be saved.
      Empty strings indicate that they are non-partitioned tensors.
    tensors: A list of `Tensor` objects. `N` tensors to save.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "SaveV2", name,
        _ctx._post_execution_callbacks, prefix, tensor_names,
        shape_and_slices, tensors)
      return _result
    except _core._FallbackException:
      try:
        return save_v2_eager_fallback(
            prefix, tensor_names, shape_and_slices, tensors, name=name,
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
        "SaveV2", prefix=prefix, tensor_names=tensor_names,
                  shape_and_slices=shape_and_slices, tensors=tensors,
                  name=name)
  return _op
  _result = None
  return _result



def save_v2_eager_fallback(prefix, tensor_names, shape_and_slices, tensors, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function save_v2
  """
  _ctx = ctx if ctx else _context.context()
  _attr_dtypes, tensors = _execute.convert_to_mixed_eager_tensors(tensors, _ctx)
  prefix = _ops.convert_to_tensor(prefix, _dtypes.string)
  tensor_names = _ops.convert_to_tensor(tensor_names, _dtypes.string)
  shape_and_slices = _ops.convert_to_tensor(shape_and_slices, _dtypes.string)
  _inputs_flat = [prefix, tensor_names, shape_and_slices] + list(tensors)
  _attrs = ("dtypes", _attr_dtypes)
  _result = _execute.execute(b"SaveV2", 0, inputs=_inputs_flat, attrs=_attrs,
                             ctx=_ctx, name=name)
  _result = None
  return _result


def sharded_filename(basename, shard, num_shards, name=None):
  r"""Generate a sharded filename. The filename is printf formatted as

     %s-%05d-of-%05d, basename, shard, num_shards.

  Args:
    basename: A `Tensor` of type `string`.
    shard: A `Tensor` of type `int32`.
    num_shards: A `Tensor` of type `int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ShardedFilename", name, _ctx._post_execution_callbacks, basename,
        shard, num_shards)
      return _result
    except _core._FallbackException:
      try:
        return sharded_filename_eager_fallback(
            basename, shard, num_shards, name=name, ctx=_ctx)
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
        "ShardedFilename", basename=basename, shard=shard,
                           num_shards=num_shards, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ShardedFilename", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sharded_filename_eager_fallback(basename, shard, num_shards, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sharded_filename
  """
  _ctx = ctx if ctx else _context.context()
  basename = _ops.convert_to_tensor(basename, _dtypes.string)
  shard = _ops.convert_to_tensor(shard, _dtypes.int32)
  num_shards = _ops.convert_to_tensor(num_shards, _dtypes.int32)
  _inputs_flat = [basename, shard, num_shards]
  _attrs = None
  _result = _execute.execute(b"ShardedFilename", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ShardedFilename", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def sharded_filespec(basename, num_shards, name=None):
  r"""Generate a glob pattern matching all sharded file names.

  Args:
    basename: A `Tensor` of type `string`.
    num_shards: A `Tensor` of type `int32`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "ShardedFilespec", name, _ctx._post_execution_callbacks, basename,
        num_shards)
      return _result
    except _core._FallbackException:
      try:
        return sharded_filespec_eager_fallback(
            basename, num_shards, name=name, ctx=_ctx)
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
        "ShardedFilespec", basename=basename, num_shards=num_shards,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = None
  _execute.record_gradient(
      "ShardedFilespec", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def sharded_filespec_eager_fallback(basename, num_shards, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function sharded_filespec
  """
  _ctx = ctx if ctx else _context.context()
  basename = _ops.convert_to_tensor(basename, _dtypes.string)
  num_shards = _ops.convert_to_tensor(num_shards, _dtypes.int32)
  _inputs_flat = [basename, num_shards]
  _attrs = None
  _result = _execute.execute(b"ShardedFilespec", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "ShardedFilespec", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def tf_record_reader(container="", shared_name="", compression_type="", name=None):
  r"""A Reader that outputs the records from a TensorFlow Records file.

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    compression_type: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("tf_record_reader op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if compression_type is None:
    compression_type = ""
  compression_type = _execute.make_str(compression_type, "compression_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TFRecordReader", container=container, shared_name=shared_name,
                          compression_type=compression_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "compression_type",
            _op.get_attr("compression_type"))
  _execute.record_gradient(
      "TFRecordReader", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tf_record_reader_eager_fallback(container="", shared_name="", compression_type="", name=None, ctx=None):
  raise RuntimeError("tf_record_reader op does not support eager execution. Arg 'reader_handle' is a ref.")

def tf_record_reader_v2(container="", shared_name="", compression_type="", name=None):
  r"""A Reader that outputs the records from a TensorFlow Records file.

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    compression_type: An optional `string`. Defaults to `""`.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TFRecordReaderV2", name, _ctx._post_execution_callbacks, "container",
        container, "shared_name", shared_name, "compression_type",
        compression_type)
      return _result
    except _core._FallbackException:
      try:
        return tf_record_reader_v2_eager_fallback(
            container=container, shared_name=shared_name,
            compression_type=compression_type, name=name, ctx=_ctx)
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
  if compression_type is None:
    compression_type = ""
  compression_type = _execute.make_str(compression_type, "compression_type")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TFRecordReaderV2", container=container, shared_name=shared_name,
                            compression_type=compression_type, name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"), "compression_type",
            _op.get_attr("compression_type"))
  _execute.record_gradient(
      "TFRecordReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def tf_record_reader_v2_eager_fallback(container="", shared_name="", compression_type="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function tf_record_reader_v2
  """
  _ctx = ctx if ctx else _context.context()
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  if compression_type is None:
    compression_type = ""
  compression_type = _execute.make_str(compression_type, "compression_type")
  _inputs_flat = []
  _attrs = ("container", container, "shared_name", shared_name,
  "compression_type", compression_type)
  _result = _execute.execute(b"TFRecordReaderV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TFRecordReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def text_line_reader(skip_header_lines=0, container="", shared_name="", name=None):
  r"""A Reader that outputs the lines of a file delimited by '\n'.

  Args:
    skip_header_lines: An optional `int`. Defaults to `0`.
      Number of lines to skip from the beginning of every file.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("text_line_reader op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if skip_header_lines is None:
    skip_header_lines = 0
  skip_header_lines = _execute.make_int(skip_header_lines, "skip_header_lines")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TextLineReader", skip_header_lines=skip_header_lines,
                          container=container, shared_name=shared_name,
                          name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("skip_header_lines", _op.get_attr("skip_header_lines"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "TextLineReader", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def text_line_reader_eager_fallback(skip_header_lines=0, container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("text_line_reader op does not support eager execution. Arg 'reader_handle' is a ref.")

def text_line_reader_v2(skip_header_lines=0, container="", shared_name="", name=None):
  r"""A Reader that outputs the lines of a file delimited by '\n'.

  Args:
    skip_header_lines: An optional `int`. Defaults to `0`.
      Number of lines to skip from the beginning of every file.
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "TextLineReaderV2", name, _ctx._post_execution_callbacks,
        "skip_header_lines", skip_header_lines, "container", container,
        "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return text_line_reader_v2_eager_fallback(
            skip_header_lines=skip_header_lines, container=container,
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
  if skip_header_lines is None:
    skip_header_lines = 0
  skip_header_lines = _execute.make_int(skip_header_lines, "skip_header_lines")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "TextLineReaderV2", skip_header_lines=skip_header_lines,
                            container=container, shared_name=shared_name,
                            name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("skip_header_lines", _op.get_attr("skip_header_lines"),
            "container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "TextLineReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def text_line_reader_v2_eager_fallback(skip_header_lines=0, container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function text_line_reader_v2
  """
  _ctx = ctx if ctx else _context.context()
  if skip_header_lines is None:
    skip_header_lines = 0
  skip_header_lines = _execute.make_int(skip_header_lines, "skip_header_lines")
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _inputs_flat = []
  _attrs = ("skip_header_lines", skip_header_lines, "container", container,
  "shared_name", shared_name)
  _result = _execute.execute(b"TextLineReaderV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "TextLineReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


def whole_file_reader(container="", shared_name="", name=None):
  r"""A Reader that outputs the entire contents of a file as a value.

  To use, enqueue filenames in a Queue.  The output of ReaderRead will
  be a filename (key) and the contents of that file (value).

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type mutable `string`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    raise RuntimeError("whole_file_reader op does not support eager execution. Arg 'reader_handle' is a ref.")
  # Add nodes to the TensorFlow graph.
  if container is None:
    container = ""
  container = _execute.make_str(container, "container")
  if shared_name is None:
    shared_name = ""
  shared_name = _execute.make_str(shared_name, "shared_name")
  _, _, _op = _op_def_lib._apply_op_helper(
        "WholeFileReader", container=container, shared_name=shared_name,
                           name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "WholeFileReader", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def whole_file_reader_eager_fallback(container="", shared_name="", name=None, ctx=None):
  raise RuntimeError("whole_file_reader op does not support eager execution. Arg 'reader_handle' is a ref.")

def whole_file_reader_v2(container="", shared_name="", name=None):
  r"""A Reader that outputs the entire contents of a file as a value.

  To use, enqueue filenames in a Queue.  The output of ReaderRead will
  be a filename (key) and the contents of that file (value).

  Args:
    container: An optional `string`. Defaults to `""`.
      If non-empty, this reader is placed in the given container.
      Otherwise, a default container is used.
    shared_name: An optional `string`. Defaults to `""`.
      If non-empty, this reader is named in the given bucket
      with this shared_name. Otherwise, the node name is used instead.
    name: A name for the operation (optional).

  Returns:
    A `Tensor` of type `resource`.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name,
        "WholeFileReaderV2", name, _ctx._post_execution_callbacks,
        "container", container, "shared_name", shared_name)
      return _result
    except _core._FallbackException:
      try:
        return whole_file_reader_v2_eager_fallback(
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
        "WholeFileReaderV2", container=container, shared_name=shared_name,
                             name=name)
  _result = _op.outputs[:]
  _inputs_flat = _op.inputs
  _attrs = ("container", _op.get_attr("container"), "shared_name",
            _op.get_attr("shared_name"))
  _execute.record_gradient(
      "WholeFileReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result



def whole_file_reader_v2_eager_fallback(container="", shared_name="", name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function whole_file_reader_v2
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
  _result = _execute.execute(b"WholeFileReaderV2", 1, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _execute.record_gradient(
      "WholeFileReaderV2", _inputs_flat, _attrs, _result, name)
  _result, = _result
  return _result


@_dispatch.add_dispatch_list
@tf_export('io.write_file', v1=['io.write_file', 'write_file'])
@deprecated_endpoints('write_file')
def write_file(filename, contents, name=None):
  r"""Writes contents to the file at input filename. Creates file and recursively

  creates directory if not existing.

  Args:
    filename: A `Tensor` of type `string`.
      scalar. The name of the file to which we write the contents.
    contents: A `Tensor` of type `string`.
      scalar. The content to be written to the output file.
    name: A name for the operation (optional).

  Returns:
    The created Operation.
  """
  _ctx = _context._context
  if _ctx is not None and _ctx._eager_context.is_eager:
    try:
      _result = _pywrap_tensorflow.TFE_Py_FastPathExecute(
        _ctx._context_handle, _ctx._eager_context.device_name, "WriteFile",
        name, _ctx._post_execution_callbacks, filename, contents)
      return _result
    except _core._FallbackException:
      try:
        return write_file_eager_fallback(
            filename, contents, name=name, ctx=_ctx)
      except _core._SymbolicException:
        pass  # Add nodes to the TensorFlow graph.
      except (TypeError, ValueError):
        result = _dispatch.dispatch(
              write_file, filename=filename, contents=contents, name=name)
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
        "WriteFile", filename=filename, contents=contents, name=name)
  except (TypeError, ValueError):
    result = _dispatch.dispatch(
          write_file, filename=filename, contents=contents, name=name)
    if result is not _dispatch.OpDispatcher.NOT_SUPPORTED:
      return result
    raise
  return _op
  _result = None
  return _result



def write_file_eager_fallback(filename, contents, name=None, ctx=None):
  r"""This is the slowpath function for Eager mode.
  This is for function write_file
  """
  _ctx = ctx if ctx else _context.context()
  filename = _ops.convert_to_tensor(filename, _dtypes.string)
  contents = _ops.convert_to_tensor(contents, _dtypes.string)
  _inputs_flat = [filename, contents]
  _attrs = None
  _result = _execute.execute(b"WriteFile", 0, inputs=_inputs_flat,
                             attrs=_attrs, ctx=_ctx, name=name)
  _result = None
  return _result

def _InitOpDefLibrary(op_list_proto_bytes):
  op_list = _op_def_pb2.OpList()
  op_list.ParseFromString(op_list_proto_bytes)
  _op_def_registry.register_op_list(op_list)
  op_def_lib = _op_def_library.OpDefLibrary()
  op_def_lib.add_op_list(op_list)
  return op_def_lib
# op {
#   name: "FixedLengthRecordReader"
#   output_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "header_bytes"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "record_bytes"
#     type: "int"
#   }
#   attr {
#     name: "footer_bytes"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "hop_bytes"
#     type: "int"
#     default_value {
#       i: 0
#     }
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
#   deprecation {
#     version: 26
#     explanation: "Use FixedLengthRecordReaderV2"
#   }
#   is_stateful: true
# }
# op {
#   name: "FixedLengthRecordReaderV2"
#   output_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "header_bytes"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "record_bytes"
#     type: "int"
#   }
#   attr {
#     name: "footer_bytes"
#     type: "int"
#     default_value {
#       i: 0
#     }
#   }
#   attr {
#     name: "hop_bytes"
#     type: "int"
#     default_value {
#       i: 0
#     }
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
#     name: "encoding"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "IdentityReader"
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
#   deprecation {
#     version: 26
#     explanation: "Use IdentityReaderV2"
#   }
#   is_stateful: true
# }
# op {
#   name: "IdentityReaderV2"
#   output_arg {
#     name: "reader_handle"
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
#   name: "LMDBReader"
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
#   is_stateful: true
# }
# op {
#   name: "MatchingFiles"
#   input_arg {
#     name: "pattern"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "filenames"
#     type: DT_STRING
#   }
# }
# op {
#   name: "MergeV2Checkpoints"
#   input_arg {
#     name: "checkpoint_prefixes"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "destination_prefix"
#     type: DT_STRING
#   }
#   attr {
#     name: "delete_old_dirs"
#     type: "bool"
#     default_value {
#       b: true
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "ReadFile"
#   input_arg {
#     name: "filename"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "contents"
#     type: DT_STRING
#   }
# }
# op {
#   name: "ReaderNumRecordsProduced"
#   input_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "records_produced"
#     type: DT_INT64
#   }
# }
# op {
#   name: "ReaderNumRecordsProducedV2"
#   input_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "records_produced"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "ReaderNumWorkUnitsCompleted"
#   input_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "units_completed"
#     type: DT_INT64
#   }
# }
# op {
#   name: "ReaderNumWorkUnitsCompletedV2"
#   input_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "units_completed"
#     type: DT_INT64
#   }
#   is_stateful: true
# }
# op {
#   name: "ReaderRead"
#   input_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "queue_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "key"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "value"
#     type: DT_STRING
#   }
# }
# op {
#   name: "ReaderReadUpTo"
#   input_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "queue_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "num_records"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "keys"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "values"
#     type: DT_STRING
#   }
# }
# op {
#   name: "ReaderReadUpToV2"
#   input_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "queue_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "num_records"
#     type: DT_INT64
#   }
#   output_arg {
#     name: "keys"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "values"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "ReaderReadV2"
#   input_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "queue_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "key"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "value"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "ReaderReset"
#   input_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
# }
# op {
#   name: "ReaderResetV2"
#   input_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   is_stateful: true
# }
# op {
#   name: "ReaderRestoreState"
#   input_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   input_arg {
#     name: "state"
#     type: DT_STRING
#   }
# }
# op {
#   name: "ReaderRestoreStateV2"
#   input_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   input_arg {
#     name: "state"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "ReaderSerializeState"
#   input_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   output_arg {
#     name: "state"
#     type: DT_STRING
#   }
# }
# op {
#   name: "ReaderSerializeStateV2"
#   input_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   output_arg {
#     name: "state"
#     type: DT_STRING
#   }
#   is_stateful: true
# }
# op {
#   name: "Restore"
#   input_arg {
#     name: "file_pattern"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor_name"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "tensor"
#     type_attr: "dt"
#   }
#   attr {
#     name: "dt"
#     type: "type"
#   }
#   attr {
#     name: "preferred_shard"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "RestoreSlice"
#   input_arg {
#     name: "file_pattern"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor_name"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "shape_and_slice"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "tensor"
#     type_attr: "dt"
#   }
#   attr {
#     name: "dt"
#     type: "type"
#   }
#   attr {
#     name: "preferred_shard"
#     type: "int"
#     default_value {
#       i: -1
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "RestoreV2"
#   input_arg {
#     name: "prefix"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor_names"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "shape_and_slices"
#     type: DT_STRING
#   }
#   output_arg {
#     name: "tensors"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "Save"
#   input_arg {
#     name: "filename"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor_names"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "data"
#     type_list_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "SaveSlices"
#   input_arg {
#     name: "filename"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor_names"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "shapes_and_slices"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "data"
#     type_list_attr: "T"
#   }
#   attr {
#     name: "T"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "SaveV2"
#   input_arg {
#     name: "prefix"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensor_names"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "shape_and_slices"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "tensors"
#     type_list_attr: "dtypes"
#   }
#   attr {
#     name: "dtypes"
#     type: "list(type)"
#     has_minimum: true
#     minimum: 1
#   }
#   is_stateful: true
# }
# op {
#   name: "ShardedFilename"
#   input_arg {
#     name: "basename"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "shard"
#     type: DT_INT32
#   }
#   input_arg {
#     name: "num_shards"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "filename"
#     type: DT_STRING
#   }
# }
# op {
#   name: "ShardedFilespec"
#   input_arg {
#     name: "basename"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "num_shards"
#     type: DT_INT32
#   }
#   output_arg {
#     name: "filename"
#     type: DT_STRING
#   }
# }
# op {
#   name: "TFRecordReader"
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
#     name: "compression_type"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   deprecation {
#     version: 26
#     explanation: "Use TFRecordReaderV2"
#   }
#   is_stateful: true
# }
# op {
#   name: "TFRecordReaderV2"
#   output_arg {
#     name: "reader_handle"
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
#     name: "compression_type"
#     type: "string"
#     default_value {
#       s: ""
#     }
#   }
#   is_stateful: true
# }
# op {
#   name: "TextLineReader"
#   output_arg {
#     name: "reader_handle"
#     type: DT_STRING
#     is_ref: true
#   }
#   attr {
#     name: "skip_header_lines"
#     type: "int"
#     default_value {
#       i: 0
#     }
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
#   deprecation {
#     version: 26
#     explanation: "Use TextLineReaderV2"
#   }
#   is_stateful: true
# }
# op {
#   name: "TextLineReaderV2"
#   output_arg {
#     name: "reader_handle"
#     type: DT_RESOURCE
#   }
#   attr {
#     name: "skip_header_lines"
#     type: "int"
#     default_value {
#       i: 0
#     }
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
#   name: "WholeFileReader"
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
#   is_stateful: true
# }
# op {
#   name: "WholeFileReaderV2"
#   output_arg {
#     name: "reader_handle"
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
#   name: "WriteFile"
#   input_arg {
#     name: "filename"
#     type: DT_STRING
#   }
#   input_arg {
#     name: "contents"
#     type: DT_STRING
#   }
# }
_op_def_lib = _InitOpDefLibrary(b"\n\346\001\n\027FixedLengthRecordReader\032\024\n\rreader_handle\030\007\200\001\001\"\027\n\014header_bytes\022\003int\032\002\030\000\"\023\n\014record_bytes\022\003int\"\027\n\014footer_bytes\022\003int\032\002\030\000\"\024\n\thop_bytes\022\003int\032\002\030\000\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000B!\010\032\022\035Use FixedLengthRecordReaderV2\210\001\001\n\332\001\n\031FixedLengthRecordReaderV2\032\021\n\rreader_handle\030\024\"\027\n\014header_bytes\022\003int\032\002\030\000\"\023\n\014record_bytes\022\003int\"\027\n\014footer_bytes\022\003int\032\002\030\000\"\024\n\thop_bytes\022\003int\032\002\030\000\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"\026\n\010encoding\022\006string\032\002\022\000\210\001\001\nw\n\016IdentityReader\032\024\n\rreader_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000B\030\010\032\022\024Use IdentityReaderV2\210\001\001\n\\\n\020IdentityReaderV2\032\021\n\rreader_handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\nY\n\nLMDBReader\032\024\n\rreader_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n+\n\rMatchingFiles\022\013\n\007pattern\030\007\032\r\n\tfilenames\030\007\ne\n\022MergeV2Checkpoints\022\027\n\023checkpoint_prefixes\030\007\022\026\n\022destination_prefix\030\007\"\033\n\017delete_old_dirs\022\004bool\032\002(\001\210\001\001\n&\n\010ReadFile\022\014\n\010filename\030\007\032\014\n\010contents\030\007\nF\n\030ReaderNumRecordsProduced\022\024\n\rreader_handle\030\007\200\001\001\032\024\n\020records_produced\030\t\nH\n\032ReaderNumRecordsProducedV2\022\021\n\rreader_handle\030\024\032\024\n\020records_produced\030\t\210\001\001\nH\n\033ReaderNumWorkUnitsCompleted\022\024\n\rreader_handle\030\007\200\001\001\032\023\n\017units_completed\030\t\nJ\n\035ReaderNumWorkUnitsCompletedV2\022\021\n\rreader_handle\030\024\032\023\n\017units_completed\030\t\210\001\001\nK\n\nReaderRead\022\024\n\rreader_handle\030\007\200\001\001\022\023\n\014queue_handle\030\007\200\001\001\032\007\n\003key\030\007\032\t\n\005value\030\007\nb\n\016ReaderReadUpTo\022\024\n\rreader_handle\030\007\200\001\001\022\023\n\014queue_handle\030\007\200\001\001\022\017\n\013num_records\030\t\032\010\n\004keys\030\007\032\n\n\006values\030\007\na\n\020ReaderReadUpToV2\022\021\n\rreader_handle\030\024\022\020\n\014queue_handle\030\024\022\017\n\013num_records\030\t\032\010\n\004keys\030\007\032\n\n\006values\030\007\210\001\001\nJ\n\014ReaderReadV2\022\021\n\rreader_handle\030\024\022\020\n\014queue_handle\030\024\032\007\n\003key\030\007\032\t\n\005value\030\007\210\001\001\n#\n\013ReaderReset\022\024\n\rreader_handle\030\007\200\001\001\n%\n\rReaderResetV2\022\021\n\rreader_handle\030\024\210\001\001\n5\n\022ReaderRestoreState\022\024\n\rreader_handle\030\007\200\001\001\022\t\n\005state\030\007\n7\n\024ReaderRestoreStateV2\022\021\n\rreader_handle\030\024\022\t\n\005state\030\007\210\001\001\n7\n\024ReaderSerializeState\022\024\n\rreader_handle\030\007\200\001\001\032\t\n\005state\030\007\n9\n\026ReaderSerializeStateV2\022\021\n\rreader_handle\030\024\032\t\n\005state\030\007\210\001\001\nn\n\007Restore\022\020\n\014file_pattern\030\007\022\017\n\013tensor_name\030\007\032\014\n\006tensor\"\002dt\"\n\n\002dt\022\004type\"#\n\017preferred_shard\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\n\210\001\n\014RestoreSlice\022\020\n\014file_pattern\030\007\022\017\n\013tensor_name\030\007\022\023\n\017shape_and_slice\030\007\032\014\n\006tensor\"\002dt\"\n\n\002dt\022\004type\"#\n\017preferred_shard\022\003int\032\013\030\377\377\377\377\377\377\377\377\377\001\210\001\001\no\n\tRestoreV2\022\n\n\006prefix\030\007\022\020\n\014tensor_names\030\007\022\024\n\020shape_and_slices\030\007\032\021\n\007tensors2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\210\001\001\nI\n\004Save\022\014\n\010filename\030\007\022\020\n\014tensor_names\030\007\022\t\n\004data2\001T\"\023\n\001T\022\nlist(type)(\0010\001\210\001\001\nf\n\nSaveSlices\022\014\n\010filename\030\007\022\020\n\014tensor_names\030\007\022\025\n\021shapes_and_slices\030\007\022\t\n\004data2\001T\"\023\n\001T\022\nlist(type)(\0010\001\210\001\001\nl\n\006SaveV2\022\n\n\006prefix\030\007\022\020\n\014tensor_names\030\007\022\024\n\020shape_and_slices\030\007\022\021\n\007tensors2\006dtypes\"\030\n\006dtypes\022\nlist(type)(\0010\001\210\001\001\nH\n\017ShardedFilename\022\014\n\010basename\030\007\022\t\n\005shard\030\003\022\016\n\nnum_shards\030\003\032\014\n\010filename\030\007\n=\n\017ShardedFilespec\022\014\n\010basename\030\007\022\016\n\nnum_shards\030\003\032\014\n\010filename\030\007\n\227\001\n\016TFRecordReader\032\024\n\rreader_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"\036\n\020compression_type\022\006string\032\002\022\000B\030\010\032\022\024Use TFRecordReaderV2\210\001\001\n|\n\020TFRecordReaderV2\032\021\n\rreader_handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\"\036\n\020compression_type\022\006string\032\002\022\000\210\001\001\n\225\001\n\016TextLineReader\032\024\n\rreader_handle\030\007\200\001\001\"\034\n\021skip_header_lines\022\003int\032\002\030\000\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000B\030\010\032\022\024Use TextLineReaderV2\210\001\001\nz\n\020TextLineReaderV2\032\021\n\rreader_handle\030\024\"\034\n\021skip_header_lines\022\003int\032\002\030\000\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n^\n\017WholeFileReader\032\024\n\rreader_handle\030\007\200\001\001\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n]\n\021WholeFileReaderV2\032\021\n\rreader_handle\030\024\"\027\n\tcontainer\022\006string\032\002\022\000\"\031\n\013shared_name\022\006string\032\002\022\000\210\001\001\n\'\n\tWriteFile\022\014\n\010filename\030\007\022\014\n\010contents\030\007")
