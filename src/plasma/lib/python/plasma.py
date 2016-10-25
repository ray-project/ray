import os
import socket
import ctypes
import time

Addr = ctypes.c_ubyte * 4

PLASMA_ID_SIZE = 20
ID = ctypes.c_ubyte * PLASMA_ID_SIZE

class PlasmaID(ctypes.Structure):
  _fields_ = [("plasma_id", ID)]

def make_plasma_id(string):
  if len(string) != PLASMA_ID_SIZE:
    raise Exception("PlasmaIDs must be {} characters long".format(PLASMA_ID_SIZE))
  object_id = map(ord, string)
  return PlasmaID(plasma_id=ID(*object_id))

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
    """Notify Plasma that the object is no longer needed."""
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

  def __init__(self, socket_name, addr=None, port=None):
    """Initialize the PlasmaClient.

    Args:
      socket_name (str): Name of the socket the plasma store is listening at.
      addr (str): IPv4 address of plasma manager attached to the plasma store.
      port (int): Port number of the plasma manager attached to the plasma store.
    """
    if port is not None:
      if not isinstance(port, int):
        raise Exception("The 'port' argument must be an integer. The given argument has type {}.".format(type(port)))
      if not 0 < port < 65536:
        raise Exception("The 'port' argument must be greater than 0 and less than 65536. The given value is {}.".format(port))

    plasma_client_library = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/plasma_client.so")
    self.client = ctypes.cdll.LoadLibrary(plasma_client_library)

    self.client.plasma_connect.restype = ctypes.c_void_p
    self.client.plasma_create.restype = None
    self.client.plasma_get.restype = None
    self.client.plasma_release.restype = None
    self.client.plasma_contains.restype = None
    self.client.plasma_seal.restype = None
    self.client.plasma_delete.restype = None
    self.client.plasma_subscribe.restype = ctypes.c_int

    self.buffer_from_memory = ctypes.pythonapi.PyBuffer_FromMemory
    self.buffer_from_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_memory.restype = ctypes.py_object

    self.buffer_from_read_write_memory = ctypes.pythonapi.PyBuffer_FromReadWriteMemory
    self.buffer_from_read_write_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_read_write_memory.restype = ctypes.py_object

    if addr is not None and port is not None:
      self.has_manager_conn = True
      self.plasma_conn = ctypes.c_void_p(self.client.plasma_connect(socket_name, addr, port))
    else:
      self.has_manager_conn = False
      self.plasma_conn = ctypes.c_void_p(self.client.plasma_connect(socket_name, None, 0))

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
