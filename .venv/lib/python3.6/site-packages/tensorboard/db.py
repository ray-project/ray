# Copyright 2017 The TensorFlow Authors. All Rights Reserved.
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

"""TensorBoard core database schema.

Currently the only database supported is SQLite.

WARNING: This module is EXPERIMENTAL. It will not be considered stable
until data migration tools are put into place. Until that time, any
database created with this schema will need to be deleted as new updates
occur.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib
import functools
import random
import re
import sqlite3
import threading
import types  # pylint: disable=unused-import

from tensorboard.util import util

TESTING_MODE = False


class TensorBase(object):
  """API for TensorBase.

  This class is thread safe.
  """

  def __init__(self, db_connection_provider, retrier_factory=util.Retrier):
    """Creates new instance.

    :type db_connection_provider: () -> Connection
    :type retrier_factory: ((Exception) -> bool) -> util.Retrier
    """
    self._db_connection_provider = db_connection_provider
    self._plugin_ids_by_name = {}  # type: dict[str, int]
    self._plugin_ids_by_name_lock = threading.Lock()
    self._retrier = retrier_factory(_is_transient_sqlite_error)

  def run_transaction(self, callback):
    """Runs an optimistic database transaction.

    If the callback returns without throwing an exception, it is
    committed, otherwise it's rolled back. If the exception is a
    transient database error, such as write contention or a random ID
    INSERT failure, then we retry with exponential back-off.

    :type callback: (Connection) -> T
    :rtype: T
    """
    with contextlib.closing(self._db_connection_provider()) as connection:
      return self._retrier.run(_TransactionRunner(connection, callback))

  def get_plugin_ids(self, names):
    """Gets IDs of plugins, creating rows if they don't exist.

    This function maintains a cache of the plugins table in local
    memory, to avoid performing queries when possible. When writing to
    the table, this function is optimistic and can cause the outer
    transaction to abort, in which case it will need to be retried.

    Args:
      db_conn: A PEP 249 Connection object.
      names: An iterable of strings of plugin names.

    Returns:
      Map of plugin names to their permanent arbitrary IDs.

    :type names: list[str]
    :rtype: dict[str, int]
    """
    with self._plugin_ids_by_name_lock:
      if all(name in self._plugin_ids_by_name for name in names):
        return self._get_plugin_ids(names)
      self._plugin_ids_by_name.update(
          self.run_transaction(functools.partial(_sync_plugins, names)))
      return self._get_plugin_ids(names)

  def _get_plugin_ids(self, names):
    return {name: self._plugin_ids_by_name[name] for name in names}


def _sync_plugins(names, connection):
  """Fetches Plugins table and assigns IDs for new names if necessary.

  :type names: list[str]
  :type connection: Connection
  :rtype: dict[str, int]
  """
  the_whole_table = {}  # type: dict[str, int]
  names = set(names)
  max_id = 0
  for id_, name in connection.execute('SELECT plugin_id, name FROM Plugins'):
    if id_ > max_id:
      max_id = id_
    the_whole_table[name] = id_
    names.discard(name)
  new_rows = []
  for name in names:
    max_id += 1
    the_whole_table[name] = max_id
    new_rows.append((max_id, name))
  if new_rows:
    connection.executemany(
        'INSERT INTO Plugins (plugin_id, name) VALUES (?, ?)',
        new_rows)
  return the_whole_table


class Schema(object):
  """SQL schema creation tool for TensorBase."""

  def __init__(self, db_conn):
    """Creates new instance.

    :type db_conn: sqlite3.Connection
    """
    self._db_conn = db_conn

  def create_tables(self):
    """Creates SQL tables needed by TensorBoard.

    If the tables are already created, this function has no effect.

    Please note that the create_indexes() method should be called after
    this method. It might be advantageous in certain circumstances to
    hold off on calling that method, for example when loading a really
    large backup into a fresh database.
    """
    self.create_experiments_table()
    self.create_runs_table()
    self.create_tags_table()
    self.create_tensors_table()
    self.create_big_tensors_table()
    self.create_event_logs_table()
    self.create_plugins_table()

  def create_indexes(self):
    """Creates SQL tables and indexes needed by TensorBoard.

    If the indexes are already created, this function has no effect.
    """
    self.create_experiments_table_name_index()
    self.create_runs_table_id_index()
    self.create_runs_table_name_index()
    self.create_tags_table_id_index()
    self.create_tags_table_name_index()
    self.create_event_logs_table_path_index()
    self.create_plugins_table_name_index()

  def create_experiments_table(self):
    """Creates the Experiments table.

    This table stores information about experiments, which are sets of
    runs.

    Fields:
      experiment_id: Random integer primary key in range [0,2^28).
      name: (Uniquely indexed) Arbitrary string which is displayed to
          the user in the TensorBoard UI, which can be no greater than
          500 characters.
      description: Arbitrary markdown text describing the experiment.
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE TABLE IF NOT EXISTS Experiments (
          experiment_id INTEGER PRIMARY KEY,
          experiment_name VARCHAR(500) NOT NULL,
          description TEXT NOT NULL
        )
      ''')

  def create_experiments_table_name_index(self):
    """Uniquely indexes the name field on the Experiments table."""
    with self._cursor() as c:
      c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS ExperimentsNameIndex
        ON Experiments (experiment_name)
      ''')

  def create_runs_table(self):
    """Creates the Runs table.

    This table stores information about runs. Each row usually
    represents a single attempt at training or testing a TensorFlow
    model, with a given set of hyper-parameters, whose summaries are
    written out to a single event logs directory with a monotonic step
    counter.

    When a run is deleted from this table, TensorBoard SHOULD treat all
    information associated with it as deleted, even if those rows in
    different tables still exist.

    Fields:
      rowid: Row ID which has run_id in the low 29 bits and
          experiment_id in the higher 28 bits. This is used to control
          locality.
      experiment_id: The 28-bit experiment ID.
      run_id: Unique randomly generated 29-bit ID for this run.
      name: Arbitrary string which is displayed to the user in the
          TensorBoard UI, which is unique within a given experiment,
          which can be no greater than 1900 characters.
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE TABLE IF NOT EXISTS Runs (
          rowid INTEGER PRIMARY KEY,
          run_id INTEGER NOT NULL,
          experiment_id INTEGER NOT NULL,
          run_name VARCHAR(1900) NOT NULL
        )
      ''')

  def create_runs_table_id_index(self):
    """Uniquely indexes the run_id field on the Runs table."""
    with self._cursor() as c:
      c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS RunsIdIndex ON Runs (run_id)
      ''')

  def create_runs_table_name_index(self):
    """Uniquely indexes the name field on the Runs table.

    More accurately, this indexes (experiment_id, name).
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS RunsNameIndex
        ON Runs (experiment_id, run_name)
      ''')

  def create_tags_table(self):
    """Creates the Tags table.

    Fields:
      rowid: The rowid which has tag_id field in the low 31 bits and the
          experiment ID in the higher 28 bits.
      tag_id: Unique randomly distributed 31-bit ID for this tag.
      run_id: The id of the row in the runs table, with which this tag
          is associated.
      plugin_id: The ID of the related row in the Plugins table.
      name: The tag. See the tag field in summary.proto for more
          information, which can be no greater than 500 characters.
      display_name: Same as SummaryMetadata.display_name, if set, which
          can be no greater than 500 characters.
      summary_description: Same as SummaryMetadata.summary_description,
          if set. This is Markdown describing the summary.
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE TABLE IF NOT EXISTS Tags (
          rowid INTEGER PRIMARY KEY,
          tag_id INTEGER NOT NULL,
          run_id INTEGER NOT NULL,
          plugin_id INTEGER NOT NULL,
          tag_name VARCHAR(500) NOT NULL,
          display_name VARCHAR(500),
          summary_description TEXT
        )
      ''')

  def create_tags_table_id_index(self):
    """Indexes the tag_id field on the Tags table."""
    with self._cursor() as c:
      c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS TagsIdIndex ON Tags (tag_id)
      ''')

  def create_tags_table_name_index(self):
    """Indexes the name field on the Tags table."""
    with self._cursor() as c:
      c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS TagsNameIndex
        ON Tags (run_id, tag_name)
      ''')

  def create_tensors_table(self):
    """Creates the Tensors table.

    This table is designed to offer contiguous in-page data storage.

    Fields:
      rowid: A 63-bit number containing the step count in the low 32
          bits, and the randomly generated tag ID in the higher 31 bits.
      encoding: A number indicating how the tensor was encoded to the
          tensor blob field. 0 indicates an uncompressed binary Tensor
          proto. 1..9 indicates a binary Tensor proto gzipped at the
          corresponding level. 10..255 are reserved for future encoding
          methods.
      is_big: A boolean indicating that the tensor field is empty and a
          separate asynchronous lookup should be performed on the
          BigTensors table.
      tensor: A binary representation of this tensor. This will be empty
          if the is_big field is true.
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE TABLE IF NOT EXISTS Tensors (
          rowid INTEGER PRIMARY KEY,
          encoding TINYINT NOT NULL,
          is_big BOOLEAN NOT NULL,
          tensor BLOB NOT NULL  -- TODO(@jart): VARBINARY on MySQL, MS-SQL, etc.
        )
      ''')

  def create_big_tensors_table(self):
    """Creates the BigTensors table.

    This table is meant for tensors larger than half a b-tree page.
    Please note that some databases, e.g. MySQL, will store these
    tensors off-page.

    Fields:
      rowid: Must be same as corresponding Tensors table row.
      tensor: A binary representation of this tensor, using the encoding
          specified in the corresponding Tensors table row.
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE TABLE IF NOT EXISTS BigTensors (
          rowid INTEGER PRIMARY KEY,
          tensor BLOB NOT NULL
        )
      ''')

  def create_plugins_table(self):
    """Creates the Plugins table.

    This table exists to assign arbitrary IDs to TBPlugin names. These
    IDs are handed out in monotonically increasing order, but that's OK,
    because the size of this table will be extremely small. These IDs
    will not be the same across TensorBoard installs.

    It is assumed that once an ID is mapped to a name, that the mapping
    will never change.

    Fields:
      plugin_id: Arbitrary integer arbitrarily limited to 16-bits.
      name: Arbitrary string which is the same as the
          TBPlugin.plugin_name field, which can be no greater than 255
          characters.
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE TABLE IF NOT EXISTS Plugins (
          plugin_id INTEGER PRIMARY KEY,
          name VARCHAR(255) NOT NULL
        )
      ''')

  def create_plugins_table_name_index(self):
    """Uniquely indexes the name field on the plugins table."""
    with self._cursor() as c:
      c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS PluginsNameIndex
        ON Plugins (name)
      ''')

  def create_event_logs_table(self):
    """Creates the EventLogs table.

    Event logs are files written to disk by TensorFlow via FileWriter,
    which outputs records containing binary-encoded tf.Event protocol buffers.

    This table is used by FileLoader to track the progress of files
    being loaded off disk into the database.

    Each time FileLoader runs a transaction committing events to the
    database, it updates the offset field.

    Fields:
      rowid: An arbitrary event_log_id in the first 29 bits, and the
          run_id in the higher 29 bits.
      run_id: A reference to the id field of the associated row in the
          runs table. Must be the same as what's in those rowid bits.
      path: The basename of the path of the event log file. It SHOULD be
          formatted: events.out.tfevents.UNIX_TIMESTAMP.HOSTNAME[SUFFIX]
      offset: The byte offset in the event log file *after* the last
          successfully committed event record.
    """
    with self._cursor() as c:
      c.execute('''\
        CREATE TABLE IF NOT EXISTS EventLogs (
          rowid INTEGER PRIMARY KEY,
          run_id INTEGER NOT NULL,
          path VARCHAR(1023) NOT NULL,
          offset INTEGER NOT NULL
        )
      ''')

  def create_event_logs_table_path_index(self):
    """Uniquely indexes the (name, path) fields on the event_logs table."""
    with self._cursor() as c:
      c.execute('''\
        CREATE UNIQUE INDEX IF NOT EXISTS EventLogsPathIndex
        ON EventLogs (run_id, path)
      ''')

  def _cursor(self):
    return contextlib.closing(self._db_conn.cursor())  # type: Cursor


def _is_transient_sqlite_error(exception):
  """Returns True if transaction should be retried on SQLite.

  :type exception: Exception
  """
  return (isinstance(exception, (FakeTransientDatabaseError,
                                 sqlite3.DatabaseError)) and
          not isinstance(exception, sqlite3.ProgrammingError))


class _TransactionRunner(object):
  """Utility class for running a transaction attempt.

  If TESTING_MODE is True, then the first invocation will always result
  in a roll back.
  """

  def __init__(self, db_conn, callback):
    """Runs a write-deferred database transaction.

    :type db_conn: Connection
    :type callback: (TransactionConnection) -> T
    """
    self._db_conn = db_conn
    self._callback = callback
    self._should_rollback = TESTING_MODE

  def __call__(self):
    """Runs a write-deferred database transaction.

    :rtype: T
    """
    with self._db_conn:
      tx_db_conn = _TransactionConnection(self._db_conn)
      result = self._callback(tx_db_conn)
      if self._should_rollback:
        self._should_rollback = False
        raise FakeTransientDatabaseError()
      with contextlib.closing(self._db_conn.cursor()) as c:
        for method, sql, parameters in tx_db_conn.write_queries:
          getattr(c, method)(sql, parameters)
      return result


class FakeTransientDatabaseError(Exception):
  """Exception thrown to roll back transactions in TESTING_MODE."""

  def __str__(self):
    return 'FakeTransientDatabaseError'


class Connection(object):
  """Delegate for PEP 249 Connection object."""

  def __init__(self, delegate):
    """Creates new instance.

    :type delegate: Connection
    """
    self._delegate = delegate
    self._is_closed = False

  def cursor(self):
    """Returns a new database cursor.

    :rtype: Cursor
    """
    return Cursor(self)

  def execute(self, sql, parameters=()):
    """Executes a query and returns its cursor.

    If this is a write query, it won't be executed until the end of the
    transaction.

    This method is not part of PEP 249 but is part of the sqlite3 API.

    :type sql: str
    :type parameters: tuple[object]
    :rtype: Cursor
    """
    cursor = self.cursor()
    cursor.execute(sql, parameters)
    return cursor

  def executemany(self, sql, seq_of_parameters=()):
    """Executes a query many times and returns its cursor.

    If this is a write query, it won't be executed until the end of the
    transaction.

    This method is not part of PEP 249 but is part of the sqlite3 API.

    :type sql: str
    :type seq_of_parameters: list[tuple[object]]
    """
    cursor = self.cursor()
    cursor.executemany(sql, seq_of_parameters)
    return cursor

  def commit(self):
    """Commits transaction."""
    self._check_closed()
    self._delegate.commit()

  def rollback(self):
    """Rolls back transaction."""
    self._check_closed()
    self._delegate.rollback()

  def close(self):
    """Closes resources associated with connection."""
    if self._delegate is not None:
      self._delegate.close()
      self._delegate = None
    self._is_closed = True

  def __enter__(self):
    self._check_closed()
    return self._delegate.__enter__()

  def __exit__(self, exc_type, exc_value, traceback):
    self._check_closed()
    self._delegate.__exit__(exc_type, exc_value, traceback)

  def _check_closed(self):
    if self._is_closed:
      raise ValueError('connection was closed')


class Cursor(object):
  """Delegate for PEP 249 Cursor object."""

  def __init__(self, connection):
    """Creates new instance.

    :type connection: Connection
    """
    self.connection = connection  # Cycle required by PEP 249
    self._delegate = None  # type: Cursor
    self._is_closed = False

  def execute(self, sql, parameters=()):
    """Executes a single query.

    :type sql: str
    :type parameters: tuple[object]
    """
    self._init_delegate()
    self._delegate.execute(sql, parameters)

  def executemany(self, sql, seq_of_parameters=()):
    """Executes a single query many times.

    :type sql: str
    :type seq_of_parameters: list[tuple[object]]
    """
    self._init_delegate()
    self._delegate.executemany(sql, seq_of_parameters)

  def executescript(self, sql):
    """Executes a script of many queries.

    :type sql: str
    """
    self._init_delegate()
    self._delegate.executescript(sql)

  def fetchone(self):
    """Returns next row in result set.

    :rtype: tuple[object]
    """
    self._check_that_read_query_was_issued()
    return self._delegate.fetchone()

  def fetchmany(self, size=None):
    """Returns next chunk of rows in result set.

    :type size: int
    """
    self._check_that_read_query_was_issued()
    if size is not None:
      return self._delegate.fetchmany(size)
    else:
      return self._delegate.fetchmany()

  def fetchall(self):
    """Returns all remaining rows in the result set.

    :rtype: list[tuple[object]]
    """
    self._check_that_read_query_was_issued()
    return self._delegate.fetchall()

  @property
  def description(self):
    """Returns information about each column in result set.

    See: https://www.python.org/dev/peps/pep-0249/

    :rtype: list[tuple[str, int, int, int, int, int, bool]]
    """
    self._check_that_read_query_was_issued()
    return self._delegate.description

  @property
  def rowcount(self):
    """Returns number of rows retrieved by last read query.

    :rtype: int
    """
    self._check_that_read_query_was_issued()
    return self._delegate.rowcount

  @property
  def lastrowid(self):
    """Returns last row ID.

    :rtype: int
    """
    self._check_that_read_query_was_issued()
    return self._delegate.lastrowid

  def _get_arraysize(self):
    self._init_delegate()
    return self._delegate.arraysize

  def _set_arraysize(self, arraysize):
    self._init_delegate()
    self._delegate.arraysize = arraysize

  arraysize = property(_get_arraysize, _set_arraysize)

  def close(self):
    """Closes resources associated with cursor."""
    if self._delegate is not None:
      self._delegate.close()
      self._delegate = None
    self._is_closed = True

  def __iter__(self):
    """Returns iterator over results of last read query.

    :rtype: types.GeneratorType[tuple[object]]
    """
    self._check_that_read_query_was_issued()
    for row in self._delegate:
      yield row

  def nextset(self):
    """Raises NotImplementedError."""
    raise NotImplementedError('Cursor.nextset not supported')

  def callproc(self, procname, parameters=()):
    """Raises NotImplementedError."""
    raise NotImplementedError('Cursor.callproc not supported')

  def setinputsizes(self, sizes):
    """Raises NotImplementedError."""
    raise NotImplementedError('Cursor.setinputsizes not supported')

  def setoutputsize(self, size, column):
    """Raises NotImplementedError."""
    raise NotImplementedError('Cursor.setoutputsize not supported')

  def _init_delegate(self):
    self._check_closed()
    if self._delegate is None:
      self._delegate = self.connection._delegate.cursor()

  def _check_that_read_query_was_issued(self):
    self._check_closed()
    if self._delegate is None:
      raise ValueError('no read query was issued')

  def _check_closed(self):
    if self._is_closed:
      raise ValueError('cursor was closed')


class _TransactionConnection(Connection):
  """PEP 249 Connection object when inside TensorBoard transactions."""

  def __init__(self, connection):
    super(_TransactionConnection, self).__init__(connection)
    self.write_queries = []

  def cursor(self):
    return _TransactionCursor(self)

  def commit(self):
    raise NotImplementedError('Please return from callback to commit')

  def rollback(self):
    raise NotImplementedError('Please throw an exception to rollback')

  def close(self):
    raise NotImplementedError('Connection.close not available in transactions')

  def __enter__(self):
    raise NotImplementedError('Already in a transaction')

  def __exit__(self, exc_type, exc_value, traceback):
    pass


class _TransactionCursor(Cursor):
  """PEP 249 Cursor object when inside TensorBoard transactions."""

  FORBIDDEN_PATTERN = re.compile(
      r'^\s*(?:ALTER|CREATE|DROP|VACUUM)\b', re.I)
  WRITE_QUERY_PATTERN = re.compile(
      r'^\s*(?:DELETE|INSERT|REPLACE|UPDATE)\b', re.I)

  def __init__(self, connection):
    super(_TransactionCursor, self).__init__(connection)
    self.connection = connection  # assign again for IDE type inference

  def execute(self, sql, parameters=()):
    _check_sql_allowed_in_transaction(sql)
    if _TransactionCursor.WRITE_QUERY_PATTERN.search(sql) is not None:
      self._check_closed()
      self.connection.write_queries.append(('execute', sql, parameters))
      return
    super(_TransactionCursor, self).execute(sql, parameters)

  def executemany(self, sql, seq_of_parameters=()):
    _check_sql_allowed_in_transaction(sql)
    if _TransactionCursor.WRITE_QUERY_PATTERN.search(sql) is not None:
      self._check_closed()
      self.connection.write_queries.append(
          ('executemany', sql, seq_of_parameters))
      return
    super(_TransactionCursor, self).executemany(sql, seq_of_parameters)

  def executescript(self, sql):
    # This method has surprising behavior with Python's SQLite driver.
    # It also prevents us from deferring write queries.
    raise NotImplementedError(
        'Cursor.executescript not supported in transactions')

  @property
  def lastrowid(self):
    raise NotImplementedError('Cursor.lastrowid not supported in transactions')


def _check_sql_allowed_in_transaction(sql):
  if _TransactionCursor.FORBIDDEN_PATTERN.search(sql) is not None:
    raise ValueError('Query forbidden in transaction: ' + sql)


class Id(object):
  """Utility class for unsigned fixed bit IDs."""

  def __init__(self, name, bits):
    """Creates new instance.

    :type name: str
    :type bits: int
    """
    if bits < 2:
      raise ValueError('bits must be >1')
    self.name = name
    self.bits = bits
    self.max = _mask(bits)

  def check(self, x):
    """Throws ValueError if x isn't in bit range.

    :type x: int
    :rtype: int
    """
    return _check_id(x, self.bits, self.name)

  def generate(self):
    """Generates a random ID in the bit range.

    :rtype: int
    """
    return random.randint(1, self.max)


class RowId(object):
  """Utility class for bit-packed SQLite primary keys."""

  def __init__(self, name, global_id, local_id):
    """Creates new instance.

    :type name: str
    :type global_id: Id
    :type local_id: Id
    """
    self.name = name
    self.bits = global_id.bits + local_id.bits
    if self.bits > 63:
      raise ValueError('%s can not exceed 63 bits' % name)
    self._global = global_id
    self._local = local_id

  def check(self, rowid):
    """Throws ValueError if rowid isn't in proper ranges.

    :type rowid: int
    :rtype: int
    """
    self.parse(rowid)
    return rowid

  def create(self, high, low):
    """Creates a rowid from its global and local portions.

    :type high: int
    :type low: int
    :rtype: int
    """
    self._global.check(high)
    self._local.check(low)
    return (high << self._local.bits) + low

  def parse(self, rowid):
    """Parses a rowid into its global and local portions.

    :type rowid: int
    :rtype: tuple[int, int]
    """
    _check_id(rowid, self.bits, self.name)
    return (self._global.check(rowid >> self._local.bits),
            self._local.check(rowid & self._local.max))

  def get_range(self, high):
    """Returns an inclusive range of all possible y values.

    This is used for the SQL BETWEEN operator.

    :type high: int
    :rtype: tuple[int, int]
    """
    return self.create(high, 1), self.create(high, self._local.max)


def _check_id(id_, bits, name):
  if id_ == 0:
    raise ValueError('%s can not be zero' % name)
  if id_ < 0:
    raise ValueError('%s can not be a negative number: %d' % (name, id_))
  if id_ > _mask(bits):
    raise ValueError('%s must be a %d-bit number: %d' % (name, bits, id_))
  return id_


def _mask(bits):
  """Returns highest integer that can be stored in `bits` unsigned bits."""
  return (1 << bits) - 1


EXPERIMENT_ID = Id('experiment_id', 28)
RUN_ID = Id('run_id', 29)
TAG_ID = Id('tag_id', 31)
TAG_PLUGIN_ID = Id('tag_plugin_id', 35)
STEP_ID = Id('step', 32)
EVENT_LOG_ID = Id('event_log_id', 29)

RUN_ROWID = RowId('Runs.rowid', EXPERIMENT_ID, RUN_ID)
TAG_ROWID = RowId('Tags.rowid', EXPERIMENT_ID, TAG_ID)
TENSOR_ROWID = RowId('Tensors.rowid', TAG_ID, STEP_ID)
EVENT_LOG_ROWID = RowId('EventLogs.rowid', RUN_ID, EVENT_LOG_ID)
