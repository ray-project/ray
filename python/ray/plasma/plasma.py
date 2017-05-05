from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import subprocess
import sys
import time

import ray.core.src.plasma.libplasma as libplasma
from ray.core.src.plasma.libplasma import plasma_object_exists_error
from ray.core.src.plasma.libplasma import plasma_out_of_memory_error

__all__ = ["PlasmaBuffer", "buffers_equal", "PlasmaClient",
           "start_plasma_store", "start_plasma_manager",
           "plasma_object_exists_error", "plasma_out_of_memory_error",
           "DEFAULT_PLASMA_STORE_MEMORY"]

PLASMA_WAIT_TIMEOUT = 2 ** 30


class PlasmaBuffer(object):
  """This is the type of objects returned by calls to get with a PlasmaClient.

  We define our own class instead of directly returning a buffer object so that
  we can add a custom destructor which notifies Plasma that the object is no
  longer being used, so the memory in the Plasma store backing the object can
  potentially be freed.

  Attributes:
    buffer (buffer): A buffer containing an object in the Plasma store.
    plasma_id (PlasmaID): The ID of the object in the buffer.
    plasma_client (PlasmaClient): The PlasmaClient that we use to communicate
      with the store and manager.
  """
  def __init__(self, buff, plasma_id, plasma_client):
    """Initialize a PlasmaBuffer."""
    self.buffer = buff
    self.plasma_id = plasma_id
    self.plasma_client = plasma_client

  def __del__(self):
    """Notify Plasma that the object is no longer needed.

    If the plasma client has been shut down, then don't do anything.
    """
    if self.plasma_client.alive:
      libplasma.release(self.plasma_client.conn, self.plasma_id)

  def __getitem__(self, index):
    """Read from the PlasmaBuffer as if it were just a regular buffer."""
    # We currently don't allow slicing plasma buffers. We should handle this
    # better, but it requires some care because the slice may be backed by the
    # same memory in the object store, but the original plasma buffer may go
    # out of scope causing the memory to no longer be accessible.
    assert not isinstance(index, slice)
    value = self.buffer[index]
    if sys.version_info >= (3, 0) and not isinstance(index, slice):
      value = chr(value)
    return value

  def __setitem__(self, index, value):
    """Write to the PlasmaBuffer as if it were just a regular buffer.

    This should fail because the buffer should be read only.
    """
    # We currently don't allow slicing plasma buffers. We should handle this
    # better, but it requires some care because the slice may be backed by the
    # same memory in the object store, but the original plasma buffer may go
    # out of scope causing the memory to no longer be accessible.
    assert not isinstance(index, slice)
    if sys.version_info >= (3, 0) and not isinstance(index, slice):
      value = ord(value)
    self.buffer[index] = value

  def __len__(self):
    """Return the length of the buffer."""
    return len(self.buffer)


def buffers_equal(buff1, buff2):
  """Compare two buffers. These buffers may be PlasmaBuffer objects.

  This method should only be used in the tests. We implement a special helper
  method for doing this because doing comparisons by slicing is much faster,
  but we don't want to expose slicing of PlasmaBuffer objects because it
  currently is not safe.
  """
  buff1_to_compare = buff1.buffer if isinstance(buff1, PlasmaBuffer) else buff1
  buff2_to_compare = buff2.buffer if isinstance(buff2, PlasmaBuffer) else buff2
  return buff1_to_compare[:] == buff2_to_compare[:]


class PlasmaClient(object):
  """The PlasmaClient is used to interface with a plasma store and manager.

  The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a
  buffer, and get a buffer. Buffers are referred to by object IDs, which are
  strings.
  """

  def __init__(self, store_socket_name, manager_socket_name=None,
               release_delay=64):
    """Initialize the PlasmaClient.

    Args:
      store_socket_name (str): Name of the socket the plasma store is listening
        at.
      manager_socket_name (str): Name of the socket the plasma manager is
        listening at.
      release_delay (int): The maximum number of objects that the client will
        keep and delay releasing (for caching reasons).
    """
    self.store_socket_name = store_socket_name
    self.manager_socket_name = manager_socket_name
    self.alive = True

    if manager_socket_name is not None:
      self.conn = libplasma.connect(store_socket_name, manager_socket_name,
                                    release_delay)
    else:
      self.conn = libplasma.connect(store_socket_name, "", release_delay)

  def shutdown(self):
    """Shutdown the client so that it does not send messages.

    If we kill the Plasma store and Plasma manager that this client is
    connected to, then we can use this method to prevent the client from trying
    to send messages to the killed processes.
    """
    if self.alive:
      libplasma.disconnect(self.conn)
    self.alive = False

  def create(self, object_id, size, metadata=None):
    """Create a new buffer in the PlasmaStore for a particular object ID.

    The returned buffer is mutable until seal is called.

    Args:
      object_id (str): A string used to identify an object.
      size (int): The size in bytes of the created buffer.
      metadata (buffer): An optional buffer encoding whatever metadata the user
        wishes to encode.

    Raises:
      plasma_object_exists_error: This exception is raised if the object could
        not be created because there already is an object with the same ID in
        the plasma store.
      plasma_out_of_memory_error: This exception is raised if the object could
        not be created because the plasma store is unable to evict enough
        objects to create room for it.
    """
    # Turn the metadata into the right type.
    metadata = bytearray(b"") if metadata is None else metadata
    buff = libplasma.create(self.conn, object_id, size, metadata)
    return PlasmaBuffer(buff, object_id, self)

  def get(self, object_ids, timeout_ms=-1):
    """Create a buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block. The retrieved
    buffer is immutable.

    Args:
      object_ids (List[str]): A list of strings used to identify some objects.
      timeout_ms (int): The number of milliseconds that the get call should
        block before timing out and returning. Pass -1 if the call should block
        and 0 if the call should return immediately.
    """
    results = libplasma.get(self.conn, object_ids, timeout_ms)
    assert len(object_ids) == len(results)
    returns = []
    for i in range(len(object_ids)):
      if results[i] is None:
        returns.append(None)
      else:
        returns.append(PlasmaBuffer(results[i][0], object_ids[i], self))
    return returns

  def get_metadata(self, object_ids, timeout_ms=-1):
    """Create a buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block until the
    object has been sealed. The retrieved buffer is immutable.

    Args:
      object_ids (List[str]): A list of strings used to identify some objects.
      timeout_ms (int): The number of milliseconds that the get call should
        block before timing out and returning. Pass -1 if the call should block
        and 0 if the call should return immediately.
    """
    results = libplasma.get(self.conn, object_ids, timeout_ms)
    assert len(object_ids) == len(results)
    returns = []
    for i in range(len(object_ids)):
      if results[i] is None:
        returns.append(None)
      else:
        returns.append(PlasmaBuffer(results[i][1], object_ids[i], self))
    return returns

  def contains(self, object_id):
    """Check if the object is present and has been sealed in the PlasmaStore.

    Args:
      object_id (str): A string used to identify an object.
    """
    return libplasma.contains(self.conn, object_id)

  def hash(self, object_id):
    """Compute the hash of an object in the object store.

    Args:
      object_id (str): A string used to identify an object.

    Returns:
      A digest string object's SHA256 hash. If the object isn't in the object
      store, the string will have length zero.
    """
    return libplasma.hash(self.conn, object_id)

  def seal(self, object_id):
    """Seal the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been sealed, the buffer is immutable and can only be
    accessed through get.

    Args:
      object_id (str): A string used to identify an object.
    """
    libplasma.seal(self.conn, object_id)

  def delete(self, object_id):
    """Delete the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been deleted, the buffer is no longer accessible.

    Args:
      object_id (str): A string used to identify an object.
    """
    libplasma.delete(self.conn, object_id)

  def evict(self, num_bytes):
    """Evict some objects until to recover some bytes.

    Recover at least num_bytes bytes if possible.

    Args:
      num_bytes (int): The number of bytes to attempt to recover.
    """
    return libplasma.evict(self.conn, num_bytes)

  def transfer(self, addr, port, object_id):
    """Transfer local object with id object_id to another plasma instance

    Args:
      addr (str): IPv4 address of the plasma instance the object is sent to.
      port (int): Port number of the plasma instance the object is sent to.
      object_id (str): A string used to identify an object.
    """
    return libplasma.transfer(self.conn, object_id, addr, port)

  def fetch(self, object_ids):
    """Fetch the objects with the given IDs from other plasma manager instances.

    Args:
      object_ids (List[str]): A list of strings used to identify the objects.
    """
    return libplasma.fetch(self.conn, object_ids)

  def wait(self, object_ids, timeout=PLASMA_WAIT_TIMEOUT, num_returns=1):
    """Wait until num_returns objects in object_ids are ready.

    Currently, the object ID arguments to wait must be unique.

    Args:
      object_ids (List[str]): List of object IDs to wait for.
      timeout (int): Return to the caller after timeout milliseconds.
      num_returns (int): We are waiting for this number of objects to be ready.

    Returns:
      ready_ids, waiting_ids (List[str], List[str]): List of object IDs that
        are ready and list of object IDs we might still wait on respectively.
    """
    # Check that the object ID arguments are unique. The plasma manager
    # currently crashes if given duplicate object IDs.
    if len(object_ids) != len(set(object_ids)):
      raise Exception("Wait requires a list of unique object IDs.")
    ready_ids, waiting_ids = libplasma.wait(self.conn, object_ids, timeout,
                                            num_returns)
    return ready_ids, list(waiting_ids)

  def subscribe(self):
    """Subscribe to notifications about sealed objects."""
    self.notification_fd = libplasma.subscribe(self.conn)

  def get_next_notification(self):
    """Get the next notification from the notification socket."""
    return libplasma.receive_notification(self.notification_fd)


DEFAULT_PLASMA_STORE_MEMORY = 10 ** 9


def random_name():
  return str(random.randint(0, 99999999))


def start_plasma_store(plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY,
                       use_valgrind=False, use_profiler=False,
                       stdout_file=None, stderr_file=None):
  """Start a plasma store process.

  Args:
    use_valgrind (bool): True if the plasma store should be started inside of
      valgrind. If this is True, use_profiler must be False.
    use_profiler (bool): True if the plasma store should be started inside a
      profiler. If this is True, use_valgrind must be False.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.

  Return:
    A tuple of the name of the plasma store socket and the process ID of the
      plasma store process.
  """
  if use_valgrind and use_profiler:
    raise Exception("Cannot use valgrind and profiler at the same time.")
  plasma_store_executable = os.path.join(os.path.abspath(
      os.path.dirname(__file__)),
      "../core/src/plasma/plasma_store")
  plasma_store_name = "/tmp/plasma_store{}".format(random_name())
  command = [plasma_store_executable,
             "-s", plasma_store_name,
             "-m", str(plasma_store_memory)]
  if use_valgrind:
    pid = subprocess.Popen(["valgrind",
                            "--track-origins=yes",
                            "--leak-check=full",
                            "--show-leak-kinds=all",
                            "--leak-check-heuristics=stdstring",
                            "--error-exitcode=1"] + command,
                           stdout=stdout_file, stderr=stderr_file)
    time.sleep(1.0)
  elif use_profiler:
    pid = subprocess.Popen(["valgrind", "--tool=callgrind"] + command,
                           stdout=stdout_file, stderr=stderr_file)
    time.sleep(1.0)
  else:
    pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
    time.sleep(0.1)
  return plasma_store_name, pid


def new_port():
  return random.randint(10000, 65535)


def start_plasma_manager(store_name, redis_address,
                         node_ip_address="127.0.0.1", plasma_manager_port=None,
                         num_retries=20, use_valgrind=False,
                         run_profiler=False, stdout_file=None,
                         stderr_file=None):
  """Start a plasma manager and return the ports it listens on.

  Args:
    store_name (str): The name of the plasma store socket.
    redis_address (str): The address of the Redis server.
    node_ip_address (str): The IP address of the node.
    plasma_manager_port (int): The port to use for the plasma manager. If this
      is not provided, a port will be generated at random.
    use_valgrind (bool): True if the Plasma manager should be started inside of
      valgrind and False otherwise.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.

  Returns:
    A tuple of the Plasma manager socket name, the process ID of the Plasma
      manager process, and the port that the manager is listening on.

  Raises:
    Exception: An exception is raised if the manager could not be started.
  """
  plasma_manager_executable = os.path.join(
      os.path.abspath(os.path.dirname(__file__)),
      "../core/src/plasma/plasma_manager")
  plasma_manager_name = "/tmp/plasma_manager{}".format(random_name())
  if plasma_manager_port is not None:
    if num_retries != 1:
      raise Exception("num_retries must be 1 if port is specified.")
  else:
    plasma_manager_port = new_port()
  process = None
  counter = 0
  while counter < num_retries:
    if counter > 0:
      print("Plasma manager failed to start, retrying now.")
    command = [plasma_manager_executable,
               "-s", store_name,
               "-m", plasma_manager_name,
               "-h", node_ip_address,
               "-p", str(plasma_manager_port),
               "-r", redis_address,
               ]
    if use_valgrind:
      process = subprocess.Popen(["valgrind",
                                  "--track-origins=yes",
                                  "--leak-check=full",
                                  "--show-leak-kinds=all",
                                  "--error-exitcode=1"] + command,
                                 stdout=stdout_file, stderr=stderr_file)
    elif run_profiler:
      process = subprocess.Popen(["valgrind", "--tool=callgrind"] + command,
                                 stdout=stdout_file, stderr=stderr_file)
    else:
      process = subprocess.Popen(command, stdout=stdout_file,
                                 stderr=stderr_file)
    # This sleep is critical. If the plasma_manager fails to start because the
    # port is already in use, then we need it to fail within 0.1 seconds.
    time.sleep(0.1)
    # See if the process has terminated
    if process.poll() is None:
      return plasma_manager_name, process, plasma_manager_port
    # Generate a new port and try again.
    plasma_manager_port = new_port()
    counter += 1
  raise Exception("Couldn't start plasma manager.")
