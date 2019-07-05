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
# ===========================================================================
"""Writer for storing imported summary event data to a SQLite DB."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import os
import sys
import time

import six

from tensorboard.compat import tf
from tensorboard.util import tb_logging
from tensorboard.util import tensor_util


logger = tb_logging.get_logger()

# Struct bundling a tag with its SummaryMetadata and a list of values, each of
# which are a tuple of step, wall time (as a float), and a TensorProto.
TagData = collections.namedtuple('TagData', ['tag', 'metadata', 'values'])


class SqliteWriter(object):
  """Sends summary data to SQLite using python's sqlite3 module."""

  def __init__(self, db_connection_provider):
    """Constructs a SqliteWriterEventSink.

    Args:
      db_connection_provider: Provider function for creating a DB connection.
    """
    self._db = db_connection_provider()

  def _make_blob(self, bytestring):
    """Helper to ensure SQLite treats the given data as a BLOB."""
    # Special-case python 2 pysqlite which uses buffers for BLOB.
    if sys.version_info[0] == 2:
      return buffer(bytestring)  # noqa: F821 (undefined name)
    return bytestring

  def _create_id(self):
    """Returns a freshly created DB-wide unique ID."""
    cursor = self._db.cursor()
    cursor.execute('INSERT INTO Ids DEFAULT VALUES')
    return cursor.lastrowid

  def _maybe_init_user(self):
    """Returns the ID for the current user, creating the row if needed."""
    user_name = os.environ.get('USER', '') or os.environ.get('USERNAME', '')
    cursor = self._db.cursor()
    cursor.execute('SELECT user_id FROM Users WHERE user_name = ?',
                   (user_name,))
    row = cursor.fetchone()
    if row:
      return row[0]
    user_id = self._create_id()
    cursor.execute(
        """
        INSERT INTO USERS (user_id, user_name, inserted_time)
        VALUES (?, ?, ?)
        """,
        (user_id, user_name, time.time()))
    return user_id

  def _maybe_init_experiment(self, experiment_name):
    """Returns the ID for the given experiment, creating the row if needed.

    Args:
      experiment_name: name of experiment.
    """
    user_id = self._maybe_init_user()
    cursor = self._db.cursor()
    cursor.execute(
        """
        SELECT experiment_id FROM Experiments
        WHERE user_id = ? AND experiment_name = ?
        """,
        (user_id, experiment_name))
    row = cursor.fetchone()
    if row:
      return row[0]
    experiment_id = self._create_id()
    # TODO: track computed time from run start times
    computed_time = 0
    cursor.execute(
        """
        INSERT INTO Experiments (
          user_id, experiment_id, experiment_name,
          inserted_time, started_time, is_watching
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, experiment_id, experiment_name, time.time(), computed_time,
         False))
    return experiment_id

  def _maybe_init_run(self, experiment_name, run_name):
    """Returns the ID for the given run, creating the row if needed.

    Args:
      experiment_name: name of experiment containing this run.
      run_name: name of run.
    """
    experiment_id = self._maybe_init_experiment(experiment_name)
    cursor = self._db.cursor()
    cursor.execute(
        """
        SELECT run_id FROM Runs
        WHERE experiment_id = ? AND run_name = ?
        """,
        (experiment_id, run_name))
    row = cursor.fetchone()
    if row:
      return row[0]
    run_id = self._create_id()
    # TODO: track actual run start times
    started_time = 0
    cursor.execute(
        """
        INSERT INTO Runs (
          experiment_id, run_id, run_name, inserted_time, started_time
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (experiment_id, run_id, run_name, time.time(), started_time))
    return run_id

  def _maybe_init_tags(self, run_id, tag_to_metadata):
    """Returns a tag-to-ID map for the given tags, creating rows if needed.

    Args:
      run_id: the ID of the run to which these tags belong.
      tag_to_metadata: map of tag name to SummaryMetadata for the tag.
    """
    cursor = self._db.cursor()
    # TODO: for huge numbers of tags (e.g. 1000+), this is slower than just
    # querying for the known tag names explicitly; find a better tradeoff.
    cursor.execute('SELECT tag_name, tag_id FROM Tags WHERE run_id = ?',
                   (run_id,))
    tag_to_id = {row[0]: row[1] for row in cursor.fetchall()
                 if row[0] in tag_to_metadata}
    new_tag_data = []
    for tag, metadata in six.iteritems(tag_to_metadata):
      if tag not in tag_to_id:
        tag_id = self._create_id()
        tag_to_id[tag] = tag_id
        new_tag_data.append((run_id, tag_id, tag, time.time(),
                             metadata.display_name,
                             metadata.plugin_data.plugin_name,
                             self._make_blob(metadata.plugin_data.content)))
    cursor.executemany(
        """
        INSERT INTO Tags (
          run_id, tag_id, tag_name, inserted_time, display_name, plugin_name,
          plugin_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        new_tag_data)
    return tag_to_id

  def write_summaries(self, tagged_data, experiment_name, run_name):
    """Transactionally writes the given tagged summary data to the DB.

    Args:
      tagged_data: map from tag to TagData instances.
      experiment_name: name of experiment.
      run_name: name of run.
    """
    logger.debug('Writing summaries for %s tags', len(tagged_data))
    # Connection used as context manager for auto commit/rollback on exit.
    # We still need an explicit BEGIN, because it doesn't do one on enter,
    # it waits until the first DML command - which is totally broken.
    # See: https://stackoverflow.com/a/44448465/1179226
    with self._db:
      self._db.execute('BEGIN TRANSACTION')
      run_id = self._maybe_init_run(experiment_name, run_name)
      tag_to_metadata = {
          tag: tagdata.metadata for tag, tagdata in six.iteritems(tagged_data)
      }
      tag_to_id = self._maybe_init_tags(run_id, tag_to_metadata)
      tensor_values = []
      for tag, tagdata in six.iteritems(tagged_data):
        tag_id = tag_to_id[tag]
        for step, wall_time, tensor_proto in tagdata.values:
          dtype = tensor_proto.dtype
          shape = ','.join(str(d.size) for d in tensor_proto.tensor_shape.dim)
          # Use tensor_proto.tensor_content if it's set, to skip relatively
          # expensive extraction into intermediate ndarray.
          data = self._make_blob(
              tensor_proto.tensor_content or
              tensor_util.make_ndarray(tensor_proto).tobytes())
          tensor_values.append((tag_id, step, wall_time, dtype, shape, data))
      self._db.executemany(
          """
          INSERT OR REPLACE INTO Tensors (
            series, step, computed_time, dtype, shape, data
          ) VALUES (?, ?, ?, ?, ?, ?)
          """,
          tensor_values)


# See tensorflow/contrib/tensorboard/db/schema.cc for documentation.
_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS Ids (
      id INTEGER PRIMARY KEY
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS Descriptions (
      id INTEGER PRIMARY KEY,
      description TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS Tensors (
      rowid INTEGER PRIMARY KEY,
      series INTEGER,
      step INTEGER,
      dtype INTEGER,
      computed_time REAL,
      shape TEXT,
      data BLOB
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS
      TensorSeriesStepIndex
    ON
      Tensors (series, step)
    WHERE
      series IS NOT NULL
      AND step IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS TensorStrings (
      rowid INTEGER PRIMARY KEY,
      tensor_rowid INTEGER NOT NULL,
      idx INTEGER NOT NULL,
      data BLOB
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS TensorStringIndex
    ON TensorStrings (tensor_rowid, idx)
    """,
    """
    CREATE TABLE IF NOT EXISTS Tags (
      rowid INTEGER PRIMARY KEY,
      run_id INTEGER,
      tag_id INTEGER NOT NULL,
      inserted_time DOUBLE,
      tag_name TEXT,
      display_name TEXT,
      plugin_name TEXT,
      plugin_data BLOB
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS TagIdIndex
    ON Tags (tag_id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS
      TagRunNameIndex
    ON
      Tags (run_id, tag_name)
    WHERE
      run_id IS NOT NULL
      AND tag_name IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS Runs (
      rowid INTEGER PRIMARY KEY,
      experiment_id INTEGER,
      run_id INTEGER NOT NULL,
      inserted_time REAL,
      started_time REAL,
      finished_time REAL,
      run_name TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS RunIdIndex
    ON Runs (run_id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS RunNameIndex
    ON Runs (experiment_id, run_name)
    WHERE run_name IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS Experiments (
      rowid INTEGER PRIMARY KEY,
      user_id INTEGER,
      experiment_id INTEGER NOT NULL,
      inserted_time REAL,
      started_time REAL,
      is_watching INTEGER,
      experiment_name TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ExperimentIdIndex
    ON Experiments (experiment_id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ExperimentNameIndex
    ON Experiments (user_id, experiment_name)
    WHERE experiment_name IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS Users (
      rowid INTEGER PRIMARY KEY,
      user_id INTEGER NOT NULL,
      inserted_time REAL,
      user_name TEXT,
      email TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS UserIdIndex
    ON Users (user_id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS UserNameIndex
    ON Users (user_name)
    WHERE user_name IS NOT NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS UserEmailIndex
    ON Users (email)
    WHERE email IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS Graphs (
      rowid INTEGER PRIMARY KEY,
      run_id INTEGER,
      graph_id INTEGER NOT NULL,
      inserted_time REAL,
      graph_def BLOB
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS GraphIdIndex
    ON Graphs (graph_id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS GraphRunIndex
    ON Graphs (run_id)
    WHERE run_id IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS Nodes (
      rowid INTEGER PRIMARY KEY,
      graph_id INTEGER NOT NULL,
      node_id INTEGER NOT NULL,
      node_name TEXT,
      op TEXT,
      device TEXT,
      node_def BLOB
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS NodeIdIndex
    ON Nodes (graph_id, node_id)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS NodeNameIndex
    ON Nodes (graph_id, node_name)
    WHERE node_name IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS NodeInputs (
      rowid INTEGER PRIMARY KEY,
      graph_id INTEGER NOT NULL,
      node_id INTEGER NOT NULL,
      idx INTEGER NOT NULL,
      input_node_id INTEGER NOT NULL,
      input_node_idx INTEGER,
      is_control INTEGER
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS NodeInputsIndex
    ON NodeInputs (graph_id, node_id, idx)
    """,
]


# Defines application ID as hexspeak for "TBOARD[0]", with an expansion digit.
# This differs from 0xFEEDABEE used in schema.h, which unfortunately exceeds
# the range of a signed 32-bit int and thus gets interpreted as 0.
_TENSORBOARD_APPLICATION_ID = 0x7B0A12D0


# Arbitrary user-controlled version number.
_TENSORBOARD_USER_VERSION = 0


def initialize_schema(connection):
  """Initializes the TensorBoard sqlite schema using the given connection.

  Args:
    connection: A sqlite DB connection.
  """
  cursor = connection.cursor()
  cursor.execute("PRAGMA application_id={}".format(_TENSORBOARD_APPLICATION_ID))
  cursor.execute("PRAGMA user_version={}".format(_TENSORBOARD_USER_VERSION))
  with connection:
    for statement in _SCHEMA_STATEMENTS:
      lines = statement.strip('\n').split('\n')
      message = lines[0] + ('...' if len(lines) > 1 else '')
      logger.debug('Running DB init statement: %s', message)
      cursor.execute(statement)
