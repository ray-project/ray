import os
import socket
import ctypes

Addr = ctypes.c_ubyte * 4

ID = ctypes.c_ubyte * 20

class PlasmaID(ctypes.Structure):
  _fields_ = [("plasma_id", ID)]

def make_plasma_id(string):
  if len(string) != 20:
    raise Exception("PlasmaIDs must be 20 characters long")
  object_id = map(ord, string)
  return PlasmaID(plasma_id=ID(*object_id))

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

    self.client.plasma_store_connect.restype = ctypes.c_void_p
    self.client.plasma_create.restype = None
    self.client.plasma_get.restype = None
    self.client.plasma_seal.restype = None

    self.buffer_from_memory = ctypes.pythonapi.PyBuffer_FromMemory
    self.buffer_from_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_memory.restype = ctypes.py_object

    self.buffer_from_read_write_memory = ctypes.pythonapi.PyBuffer_FromReadWriteMemory
    self.buffer_from_read_write_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_read_write_memory.restype = ctypes.py_object

    self.store_conn = ctypes.c_void_p(self.client.plasma_store_connect(socket_name))

    if addr is not None and port is not None:
      self.manager_conn = self.client.plasma_manager_connect(addr, port)
    else:
      self.manager_conn = -1 # not connected

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
    self.client.plasma_create(self.store_conn, make_plasma_id(object_id), size, ctypes.cast(metadata, ctypes.POINTER(ctypes.c_ubyte * len(metadata))), len(metadata), ctypes.byref(data))
    return self.buffer_from_read_write_memory(data, size)

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
    buf = self.client.plasma_get(self.store_conn, make_plasma_id(object_id), ctypes.byref(size), ctypes.byref(data), ctypes.byref(metadata_size), ctypes.byref(metadata))
    return self.buffer_from_memory(data, size)

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
    buf = self.client.plasma_get(self.store_conn, make_plasma_id(object_id), ctypes.byref(size), ctypes.byref(data), ctypes.byref(metadata_size), ctypes.byref(metadata))
    return self.buffer_from_memory(metadata, metadata_size)

  def seal(self, object_id):
    """Seal the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been sealed, the buffer is immutable and can only be
    accessed through get.

    Args:
      object_id (str): A string used to identify an object.
    """
    self.client.plasma_seal(self.store_conn, make_plasma_id(object_id))

  def transfer(self, addr, port, object_id):
    """Transfer local object with id object_id to another plasma instance

    Args:
      addr (str): IPv4 address of the plasma instance the object is sent to.
      port (int): Port number of the plasma instance the object is sent to.
      object_id (str): A string used to identify an object.
    """
    if self.manager_conn == -1:
      raise Exception("Not connected to the plasma manager socket")
    self.client.plasma_transfer(self.manager_conn, addr, port, make_plasma_id(object_id))
