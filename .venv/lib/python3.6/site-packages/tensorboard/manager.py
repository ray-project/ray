# Copyright 2019 The TensorFlow Authors. All Rights Reserved.
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
"""Private utilities for managing multiple TensorBoard processes."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import base64
import collections
import datetime
import errno
import json
import os
import subprocess
import tempfile
import time

import six

from tensorboard import version
from tensorboard.util import tb_logging


# Type descriptors for `TensorBoardInfo` fields.
_FieldType = collections.namedtuple(
    "_FieldType",
    (
        "serialized_type",
        "runtime_type",
        "serialize",
        "deserialize",
    ),
)
_type_timestamp = _FieldType(
    serialized_type=int,  # seconds since epoch
    runtime_type=datetime.datetime,  # microseconds component ignored
    serialize=lambda dt: int(
        (dt - datetime.datetime.fromtimestamp(0)).total_seconds()),
    deserialize=lambda n: datetime.datetime.fromtimestamp(n),
)
_type_int = _FieldType(
    serialized_type=int,
    runtime_type=int,
    serialize=lambda n: n,
    deserialize=lambda n: n,
)
_type_str = _FieldType(
    serialized_type=six.text_type,  # `json.loads` always gives Unicode
    runtime_type=str,
    serialize=six.text_type,
    deserialize=str,
)

# Information about a running TensorBoard instance.
_TENSORBOARD_INFO_FIELDS = collections.OrderedDict((
    ("version", _type_str),
    ("start_time", _type_timestamp),
    ("pid", _type_int),
    ("port", _type_int),
    ("path_prefix", _type_str),  # may be empty
    ("logdir", _type_str),  # may be empty
    ("db", _type_str),  # may be empty
    ("cache_key", _type_str),  # opaque, as given by `cache_key` below
))
TensorBoardInfo = collections.namedtuple(
    "TensorBoardInfo",
    _TENSORBOARD_INFO_FIELDS,
)


def data_source_from_info(info):
  """Format the data location for the given TensorBoardInfo.

  Args:
    info: A TensorBoardInfo value.

  Returns:
    A human-readable string describing the logdir or database connection
    used by the server: e.g., "logdir /tmp/logs".
  """
  if info.db:
    return "db %s" % info.db
  else:
    return "logdir %s" % info.logdir


def _info_to_string(info):
  """Convert a `TensorBoardInfo` to string form to be stored on disk.

  The format returned by this function is opaque and should only be
  interpreted by `_info_from_string`.

  Args:
    info: A valid `TensorBoardInfo` object.

  Raises:
    ValueError: If any field on `info` is not of the correct type.

  Returns:
    A string representation of the provided `TensorBoardInfo`.
  """
  for key in _TENSORBOARD_INFO_FIELDS:
    field_type = _TENSORBOARD_INFO_FIELDS[key]
    if not isinstance(getattr(info, key), field_type.runtime_type):
      raise ValueError(
          "expected %r of type %s, but found: %r" %
          (key, field_type.runtime_type, getattr(info, key))
      )
  if info.version != version.VERSION:
    raise ValueError(
        "expected 'version' to be %r, but found: %r" %
        (version.VERSION, info.version)
    )
  json_value = {
      k: _TENSORBOARD_INFO_FIELDS[k].serialize(getattr(info, k))
      for k in _TENSORBOARD_INFO_FIELDS
  }
  return json.dumps(json_value, sort_keys=True, indent=4)


def _info_from_string(info_string):
  """Parse a `TensorBoardInfo` object from its string representation.

  Args:
    info_string: A string representation of a `TensorBoardInfo`, as
      produced by a previous call to `_info_to_string`.

  Returns:
    A `TensorBoardInfo` value.

  Raises:
    ValueError: If the provided string is not valid JSON, or if it does
      not represent a JSON object with a "version" field whose value is
      `tensorboard.version.VERSION`, or if it has the wrong set of
      fields, or if at least one field is of invalid type.
  """

  try:
    json_value = json.loads(info_string)
  except ValueError:
    raise ValueError("invalid JSON: %r" % (info_string,))
  if not isinstance(json_value, dict):
    raise ValueError("not a JSON object: %r" % (json_value,))
  if json_value.get("version") != version.VERSION:
    raise ValueError("incompatible version: %r" % (json_value,))
  expected_keys = frozenset(_TENSORBOARD_INFO_FIELDS)
  actual_keys = frozenset(json_value)
  if expected_keys != actual_keys:
    raise ValueError(
        "bad keys on TensorBoardInfo (missing: %s; extraneous: %s)"
        % (expected_keys - actual_keys, actual_keys - expected_keys)
    )

  # Validate and deserialize fields.
  for key in _TENSORBOARD_INFO_FIELDS:
    field_type = _TENSORBOARD_INFO_FIELDS[key]
    if not isinstance(json_value[key], field_type.serialized_type):
      raise ValueError(
          "expected %r of type %s, but found: %r" %
          (key, field_type.serialized_type, json_value[key])
      )
    json_value[key] = field_type.deserialize(json_value[key])

  return TensorBoardInfo(**json_value)


def cache_key(working_directory, arguments, configure_kwargs):
  """Compute a `TensorBoardInfo.cache_key` field.

  The format returned by this function is opaque. Clients may only
  inspect it by comparing it for equality with other results from this
  function.

  Args:
    working_directory: The directory from which TensorBoard was launched
      and relative to which paths like `--logdir` and `--db` are
      resolved.
    arguments: The command-line args to TensorBoard, as `sys.argv[1:]`.
      Should be a list (or tuple), not an unparsed string. If you have a
      raw shell command, use `shlex.split` before passing it to this
      function.
    configure_kwargs: A dictionary of additional argument values to
      override the textual `arguments`, with the same semantics as in
      `tensorboard.program.TensorBoard.configure`. May be an empty
      dictionary.

  Returns:
    A string such that if two (prospective or actual) TensorBoard
    invocations have the same cache key then it is safe to use one in
    place of the other. The converse is not guaranteed: it is often safe
    to change the order of TensorBoard arguments, or to explicitly set
    them to their default values, or to move them between `arguments`
    and `configure_kwargs`, but such invocations may yield distinct
    cache keys.
  """
  if not isinstance(arguments, (list, tuple)):
    raise TypeError(
        "'arguments' should be a list of arguments, but found: %r "
        "(use `shlex.split` if given a string)"
        % (arguments,)
    )
  datum = {
      "working_directory": working_directory,
      "arguments": arguments,
      "configure_kwargs": configure_kwargs,
  }
  raw = base64.b64encode(
      json.dumps(datum, sort_keys=True, separators=(",", ":")).encode("utf-8")
  )
  # `raw` is of type `bytes`, even though it only contains ASCII
  # characters; we want it to be `str` in both Python 2 and 3.
  return str(raw.decode("ascii"))


def _get_info_dir():
  """Get path to directory in which to store info files.

  The directory returned by this function is "owned" by this module. If
  the contents of the directory are modified other than via the public
  functions of this module, subsequent behavior is undefined.

  The directory will be created if it does not exist.
  """
  path = os.path.join(tempfile.gettempdir(), ".tensorboard-info")
  try:
    os.makedirs(path)
  except OSError as e:
    if e.errno == errno.EEXIST and os.path.isdir(path):
      pass
    else:
      raise
  return path


def _get_info_file_path():
  """Get path to info file for the current process.

  As with `_get_info_dir`, the info directory will be created if it does
  not exist.
  """
  return os.path.join(_get_info_dir(), "pid-%d.info" % os.getpid())


def write_info_file(tensorboard_info):
  """Write TensorBoardInfo to the current process's info file.

  This should be called by `main` once the server is ready. When the
  server shuts down, `remove_info_file` should be called.

  Args:
    tensorboard_info: A valid `TensorBoardInfo` object.

  Raises:
    ValueError: If any field on `info` is not of the correct type.
  """
  payload = "%s\n" % _info_to_string(tensorboard_info)
  with open(_get_info_file_path(), "w") as outfile:
    outfile.write(payload)


def remove_info_file():
  """Remove the current process's TensorBoardInfo file, if it exists.

  If the file does not exist, no action is taken and no error is raised.
  """
  try:
    os.unlink(_get_info_file_path())
  except OSError as e:
    if e.errno == errno.ENOENT:
      # The user may have wiped their temporary directory or something.
      # Not a problem: we're already in the state that we want to be in.
      pass
    else:
      raise


def get_all():
  """Return TensorBoardInfo values for running TensorBoard processes.

  This function may not provide a perfect snapshot of the set of running
  processes. Its result set may be incomplete if the user has cleaned
  their /tmp/ directory while TensorBoard processes are running. It may
  contain extraneous entries if TensorBoard processes exited uncleanly
  (e.g., with SIGKILL or SIGQUIT).

  Returns:
    A fresh list of `TensorBoardInfo` objects.
  """
  info_dir = _get_info_dir()
  results = []
  for filename in os.listdir(info_dir):
    filepath = os.path.join(info_dir, filename)
    try:
      with open(filepath) as infile:
        contents = infile.read()
    except IOError as e:
      if e.errno == errno.EACCES:
        # May have been written by this module in a process whose
        # `umask` includes some bits of 0o444.
        continue
      else:
        raise
    try:
      info = _info_from_string(contents)
    except ValueError:
      tb_logging.get_logger().warning(
          "invalid info file: %r",
          filepath,
          exc_info=True,
      )
    else:
      results.append(info)
  return results


# The following four types enumerate the possible return values of the
# `start` function.

# Indicates that a call to `start` was compatible with an existing
# TensorBoard process, which can be reused according to the provided
# info.
StartReused = collections.namedtuple("StartReused", ("info",))

# Indicates that a call to `start` successfully launched a new
# TensorBoard process, which is available with the provided info.
StartLaunched = collections.namedtuple("StartLaunched", ("info",))

# Indicates that a call to `start` tried to launch a new TensorBoard
# instance, but the subprocess exited with the given exit code and
# output streams. (If the contents of the output streams are no longer
# available---e.g., because the user has emptied /tmp/---then the
# corresponding values will be `None`.)
StartFailed = collections.namedtuple(
    "StartFailed",
    (
        "exit_code",  # int, as `Popen.returncode` (negative for signal)
        "stdout",  # str, or `None` if the stream could not be read
        "stderr",  # str, or `None` if the stream could not be read
    ),
)

# Indicates that a call to `start` launched a TensorBoard process, but
# that process neither exited nor wrote its info file within the allowed
# timeout period. The process may still be running under the included
# PID.
StartTimedOut = collections.namedtuple("StartTimedOut", ("pid",))


def start(arguments, timeout=datetime.timedelta(seconds=60)):
  """Start a new TensorBoard instance, or reuse a compatible one.

  If the cache key determined by the provided arguments and the current
  working directory (see `cache_key`) matches the cache key of a running
  TensorBoard process (see `get_all`), that process will be reused.

  Otherwise, a new TensorBoard process will be spawned with the provided
  arguments, using the `tensorboard` binary from the system path.

  Args:
    arguments: List of strings to be passed as arguments to
      `tensorboard`. (If you have a raw command-line string, see
      `shlex.split`.)
    timeout: `datetime.timedelta` object describing how long to wait for
      the subprocess to initialize a TensorBoard server and write its
      `TensorBoardInfo` file. If the info file is not written within
      this time period, `start` will assume that the subprocess is stuck
      in a bad state, and will give up on waiting for it and return a
      `StartTimedOut` result. Note that in such a case the subprocess
      will not be killed. Default value is 60 seconds.

  Returns:
    A `StartReused`, `StartLaunched`, `StartFailed`, or `StartTimedOut`
    object.
  """
  match = _find_matching_instance(
      cache_key(
          working_directory=os.getcwd(),
          arguments=arguments,
          configure_kwargs={},
      ),
  )
  if match:
    return StartReused(info=match)

  (stdout_fd, stdout_path) = tempfile.mkstemp(prefix=".tensorboard-stdout-")
  (stderr_fd, stderr_path) = tempfile.mkstemp(prefix=".tensorboard-stderr-")
  start_time = datetime.datetime.now()
  try:
    p = subprocess.Popen(
        ["tensorboard"] + arguments,
        stdout=stdout_fd,
        stderr=stderr_fd,
    )
  finally:
    os.close(stdout_fd)
    os.close(stderr_fd)

  poll_interval_seconds = 0.5
  end_time = start_time + timeout
  while datetime.datetime.now() < end_time:
    time.sleep(poll_interval_seconds)
    subprocess_result = p.poll()
    if subprocess_result is not None:
      return StartFailed(
          exit_code=subprocess_result,
          stdout=_maybe_read_file(stdout_path),
          stderr=_maybe_read_file(stderr_path),
      )
    for info in get_all():
      if info.pid == p.pid and info.start_time >= start_time:
        return StartLaunched(info=info)
  else:
    return StartTimedOut(pid=p.pid)


def _find_matching_instance(cache_key):
  """Find a running TensorBoard instance compatible with the cache key.

  Returns:
    A `TensorBoardInfo` object, or `None` if none matches the cache key.
  """
  infos = get_all()
  candidates = [info for info in infos if info.cache_key == cache_key]
  for candidate in sorted(candidates, key=lambda x: x.port):
    # TODO(@wchargin): Check here that the provided port is still live.
    return candidate
  return None


def _maybe_read_file(filename):
  """Read the given file, if it exists.

  Args:
    filename: A path to a file.

  Returns:
    A string containing the file contents, or `None` if the file does
    not exist.
  """
  try:
    with open(filename) as infile:
      return infile.read()
  except IOError as e:
    if e.errno == errno.ENOENT:
      return None
