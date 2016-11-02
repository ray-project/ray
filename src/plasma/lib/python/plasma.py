import ctypes
import os
import random
import socket
import subprocess
import time

Addr = ctypes.c_ubyte * 4

PLASMA_ID_SIZE = 20
ID = ctypes.c_ubyte * PLASMA_ID_SIZE

class PlasmaID(ctypes.Structure):
  _fields_ = [("plasma_id", ID)]

def make_plasma_id(string):
  if len(string) != PLASMA_ID_SIZE:
    raise Exception("PlasmaIDs must be {} characters long".format(PLASMA_ID_SIZE))
  return PlasmaID(plasma_id=ID.from_buffer_copy(string))

def plasma_id_to_str(plasma_id):
  return str(bytearray(plasma_id.plasma_id))

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
      self.plasma_client.client.plasma_release(self.plasma_client.plasma_conn, self.plasma_id)

  def __getitem__(self, index):
    """Read from the PlasmaBuffer as if it were just a regular buffer."""
    return self.buffer[index]

  def __setitem__(self, index, value):
    """Write to the PlasmaBuffer as if it were just a regular buffer.

    This should fail because the buffer should be read only.
    """
    self.buffer[index] = value

  def __len__(self):
    """Return the length of the buffer."""
    return len(self.buffer)

class PlasmaClient(object):
  """The PlasmaClient is used to interface with a plasma store and a plasma manager.

  The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a
  buffer, and get a buffer. Buffers are referred to by object IDs, which are
  strings.
  """

  def __init__(self, store_socket_name, manager_socket_name=None):
    """Initialize the PlasmaClient.

    Args:
      store_socket_name (str): Name of the socket the plasma store is listening at.
      manager_socket_name (str): Name of the socket the plasma manager is listening at.
    """
    self.alive = True
    plasma_client_library = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/plasma_client.so")
    self.client = ctypes.cdll.LoadLibrary(plasma_client_library)

    self.client.plasma_connect.restype = ctypes.c_void_p
    self.client.plasma_create.restype = None
    self.client.plasma_get.restype = None
    self.client.plasma_release.restype = None
    self.client.plasma_contains.restype = None
    self.client.plasma_seal.restype = None
    self.client.plasma_delete.restype = None
    self.client.plasma_evict.restype = ctypes.c_int64
    self.client.plasma_subscribe.restype = ctypes.c_int
    self.client.plasma_wait.restype = ctypes.c_int

    self.buffer_from_memory = ctypes.pythonapi.PyBuffer_FromMemory
    self.buffer_from_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_memory.restype = ctypes.py_object

    self.buffer_from_read_write_memory = ctypes.pythonapi.PyBuffer_FromReadWriteMemory
    self.buffer_from_read_write_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_read_write_memory.restype = ctypes.py_object

    if manager_socket_name is not None:
      self.has_manager_conn = True
      self.plasma_conn = ctypes.c_void_p(self.client.plasma_connect(store_socket_name, manager_socket_name))
    else:
      self.has_manager_conn = False
      self.plasma_conn = ctypes.c_void_p(self.client.plasma_connect(store_socket_name, None))

  def shutdown(self):
    """Shutdown the client so that it does not send messages.

    If we kill the Plasma store and Plasma manager that this client is connected
    to, then we can use this method to prevent the client from trying to send
    messages to the killed processes.
    """
    self.alive = False

  def create(self, object_id, size, metadata=None):
    """Create a new buffer in the PlasmaStore for a particular object ID.

    The returned buffer is mutable until seal is called.

    Args:
      object_id (str): A string used to identify an object.
      size (int): The size in bytes of the created buffer.
      metadata (buffer): An optional buffer encoding whatever metadata the user
        wishes to encode.
    """
    # This is used to hold the address of the buffer.
    data = ctypes.c_void_p()
    # Turn the metadata into the right type.
    metadata = buffer("") if metadata is None else metadata
    metadata = (ctypes.c_ubyte * len(metadata)).from_buffer_copy(metadata)
    self.client.plasma_create(self.plasma_conn, make_plasma_id(object_id), size, ctypes.cast(metadata, ctypes.POINTER(ctypes.c_ubyte * len(metadata))), len(metadata), ctypes.byref(data))
    return PlasmaBuffer(self.buffer_from_read_write_memory(data, size), make_plasma_id(object_id), self)

  def get(self, object_id):
    """Create a buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block. The retrieved
    buffer is immutable.

    Args:
      object_id (str): A string used to identify an object.
    """
    size = ctypes.c_int64()
    data = ctypes.c_void_p()
    metadata_size = ctypes.c_int64()
    metadata = ctypes.c_void_p()
    self.client.plasma_get(self.plasma_conn, make_plasma_id(object_id), ctypes.byref(size), ctypes.byref(data), ctypes.byref(metadata_size), ctypes.byref(metadata))
    return PlasmaBuffer(self.buffer_from_memory(data, size), make_plasma_id(object_id), self)

  def get_metadata(self, object_id):
    """Create a buffer from the PlasmaStore based on object ID.

    If the object has not been sealed yet, this call will block until the object
    has been sealed. The retrieved buffer is immutable.

    Args:
      object_id (str): A string used to identify an object.
    """
    size = ctypes.c_int64()
    data = ctypes.c_void_p()
    metadata_size = ctypes.c_int64()
    metadata = ctypes.c_void_p()
    self.client.plasma_get(self.plasma_conn, make_plasma_id(object_id), ctypes.byref(size), ctypes.byref(data), ctypes.byref(metadata_size), ctypes.byref(metadata))
    return PlasmaBuffer(self.buffer_from_memory(metadata, metadata_size), make_plasma_id(object_id), self)

  def contains(self, object_id):
    """Check if the object is present and has been sealed in the PlasmaStore.

    Args:
      object_id (str): A string used to identify an object.
    """
    has_object = ctypes.c_int()
    self.client.plasma_contains(self.plasma_conn, make_plasma_id(object_id), ctypes.byref(has_object))
    has_object = has_object.value
    if has_object == 1:
      return True
    elif has_object == 0:
      return False
    else:
      raise Exception("This code should be unreachable.")

  def seal(self, object_id):
    """Seal the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been sealed, the buffer is immutable and can only be
    accessed through get.

    Args:
      object_id (str): A string used to identify an object.
    """
    self.client.plasma_seal(self.plasma_conn, make_plasma_id(object_id))

  def delete(self, object_id):
    """Delete the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been deleted, the buffer is no longer accessible.

    Args:
      object_id (str): A string used to identify an object.
    """
    self.client.plasma_delete(self.plasma_conn, make_plasma_id(object_id))

  def evict(self, num_bytes):
    """Evict some objects until to recover some bytes.

    Recover at least num_bytes bytes if possible.

    Args:
      num_bytes (int): The number of bytes to attempt to recover.
    """
    num_bytes_evicted = self.client.plasma_evict(self.plasma_conn, num_bytes)
    return num_bytes_evicted

  def transfer(self, addr, port, object_id):
    """Transfer local object with id object_id to another plasma instance

    Args:
      addr (str): IPv4 address of the plasma instance the object is sent to.
      port (int): Port number of the plasma instance the object is sent to.
      object_id (str): A string used to identify an object.
    """
    if not self.has_manager_conn:
      raise Exception("Not connected to the plasma manager socket")
    self.client.plasma_transfer(self.plasma_conn, addr, port, make_plasma_id(object_id))

  def fetch(self, object_ids):
    """Fetch the object with id object_id from another plasma manager instance.

    Args:
      object_id (str): A string used to identify an object.
    """
    object_id_array = (len(object_ids) * PlasmaID)()
    for i, object_id in enumerate(object_ids):
      object_id_array[i] = make_plasma_id(object_id)
    success_array = (len(object_ids) * ctypes.c_int)()
    if not self.has_manager_conn:
      raise Exception("Not connected to the plasma manager socket")
    self.client.plasma_fetch(self.plasma_conn,
                             object_id_array._length_,
                             object_id_array,
                             success_array);
    return [bool(success) for success in success_array]

  def wait(self, object_ids, timeout, num_returns):
    """Wait until num_returns objects in object_ids are ready.

    Args:
      object_ids (List[str]): List of object IDs to wait for.
      timeout (int): Return to the caller after timeout milliseconds.
      num_returns (int): We are waiting for this number of objects to be ready.

    Returns:
      ready_ids, waiting_ids (List[str], List[str]): List of object IDs that
        are ready and list of object IDs we might still wait on respectively.
    """
    if not self.has_manager_conn:
      raise Exception("Not connected to the plasma manager socket")
    if num_returns < 0:
      raise Exception("The argument num_returns cannot be less than one.")
    if num_returns > len(object_ids):
      raise Exception("The argument num_returns cannot be greater than len(object_ids): num_returns is {}, len(object_ids) is {}.".format(num_returns, len(object_ids)))
    if timeout > 2 ** 36:
      raise Exception("The method wait currently cannot be used with a timeout greater than 2 ** 36.")
    object_id_array = (len(object_ids) * PlasmaID)()
    for i, object_id in enumerate(object_ids):
      object_id_array[i] = make_plasma_id(object_id)
    return_id_array = (num_returns * PlasmaID)()
    num_return_objects = self.client.plasma_wait(self.plasma_conn,
                                                 object_id_array._length_,
                                                 object_id_array,
                                                 ctypes.c_int64(timeout),
                                                 num_returns,
                                                 return_id_array)
    ready_ids = map(plasma_id_to_str, return_id_array[num_returns-num_return_objects:])
    return ready_ids, list(set(object_ids) - set(ready_ids))

  def subscribe(self):
    """Subscribe to notifications about sealed objects."""
    fd = self.client.plasma_subscribe(self.plasma_conn)
    self.notification_sock = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_STREAM)
    # Make the socket non-blocking.
    self.notification_sock.setblocking(0)

  def get_next_notification(self):
    """Get the next notification from the notification socket."""
    if not self.notification_sock:
      raise Exception("To get notifications, first call subscribe.")
    # Loop until we've read PLASMA_ID_SIZE bytes from the socket.
    while True:
      try:
        message_data = self.notification_sock.recv(PLASMA_ID_SIZE)
      except socket.error:
        time.sleep(0.001)
      else:
        assert len(message_data) == PLASMA_ID_SIZE
        break
    return message_data

def start_plasma_manager(store_name, manager_name, redis_address, num_retries=5, use_valgrind=False, run_profiler=False):
  """Start a plasma manager and return the ports it listens on.

  Args:
    store_name (str): The name of the plasma store socket.
    manager_name (str): The name of the plasma manager socket.
    redis_address (str): The address of the Redis server.
    use_valgrind (bool): True if the Plasma manager should be started inside of
      valgrind and False otherwise.

  Returns:
    The process ID of the Plasma manager and the port that the manager is
      listening on.

  Raises:
    Exception: An exception is raised if the manager could not be properly
      started.
  """
  plasma_manager_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/plasma_manager")
  port = None
  process = None
  counter = 0
  while counter < num_retries:
    if counter > 0:
      print("Plasma manager failed to start, retrying now.")
    port = random.randint(10000, 65535)
    command = [plasma_manager_executable,
               "-s", store_name,
               "-m", manager_name,
               "-h", "127.0.0.1",
               "-p", str(port),
               "-r", redis_address]
    if use_valgrind:
      process = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + command)
    elif run_profiler:
      process = subprocess.Popen(["valgrind", "--tool=callgrind"] + command)
    else:
      process = subprocess.Popen(command)
    # This sleep is critical. If the plasma_manager fails to start because the
    # port is already in use, then we need it to fail within 0.1 seconds.
    time.sleep(0.1)
    # See if the process has terminated
    if process.poll() == None:
      return process, port
    counter += 1
  raise Exception("Couldn't start plasma manager.")
