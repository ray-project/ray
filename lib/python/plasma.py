import os
import socket
import ctypes

Addr = ctypes.c_ubyte * 4

ID = ctypes.c_ubyte * 20

class PlasmaID(ctypes.Structure):
  _fields_ = [("plasma_id", ID)]

# these must be in sync with plasma_request_type in plasma.h (can we have a test for that?)
PLASMA_CREATE = 0
PLASMA_GET = 1
PLASMA_SEAL = 2
PLASMA_TRANSFER = 3
PLASMA_DATA = 4
PLASMA_REGISTER = 5
PLASMA_GET_MANAGER_PORT = 6
PLASMA_RETURN_MANAGER_PORT = 7

class PlasmaRequest(ctypes.Structure):
  _fields_ = [("type", ctypes.c_int),
              ("manager_id", ctypes.c_int),
              ("object_id", PlasmaID),
              ("size", ctypes.c_int64),
              ("addr", Addr),
              ("port", ctypes.c_int)]

class PlasmaBuffer(ctypes.Structure):
  _fields_ = [("plasma_id", PlasmaID),
              ("data", ctypes.c_void_p),
              ("size", ctypes.c_int64),
              ("writable", ctypes.c_int)]

def make_plasma_id(string):
  if len(string) != 20:
    raise Exception("PlasmaIDs must be 20 characters long")
  object_id = map(ord, string)
  return PlasmaID(plasma_id=ID(*object_id))

class PlasmaManager(object):
  """The PlasmaManager is used to manage a PlasmaStore.

  There should be one PlasmaManager per PlasmaStore. The PlasmaManager is
  responsible for interfacing with other PlasmaManagers in order to transfer
  objects between PlasmaStores. This class sends commands to the C
  implementation of the PlasmaManager using sockets.

  Attributes:
    sock: The socket used to communicate with the C implementation of the
      PlasmaManager.
  """

  def __init__(self, addr, port):
    """Initialize the PlasmaManager."""
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.connect((addr, port))

  def register(self, manager_id, addr, port):
    """Register another object manager."""
    req = PlasmaRequest(type=PLASMA_REGISTER, manager_id=manager_id,
                        addr=Addr(*map(int, addr.split("."))), port=port)
    self.sock.send(buffer(req)[:])

  def transfer(self, manager_id, object_id):
    """Transfer local object with id object_id to manager with id manager_id."""
    req = PlasmaRequest(type=PLASMA_TRANSFER, manager_id=manager_id,
                        object_id=make_plasma_id(object_id))
    self.sock.send(buffer(req)[:])

class PlasmaClient(object):
  """The PlasmaClient is used to interface with a PlasmaStore.

  The PlasmaClient can ask the PlasmaStore to allocate a new buffer, seal a
  buffer, and get a buffer. Buffers are referred to by object IDs, which are
  strings.
  """

  def __init__(self, socket_name):
    plasma_client_library = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/plasma_client.so")
    self.client = ctypes.cdll.LoadLibrary(plasma_client_library)

    self.client.plasma_store_connect.restype = ctypes.c_int

    self.client.plasma_create.argtypes = [ctypes.c_int, PlasmaID, ctypes.c_int64]
    self.client.plasma_create.restype = PlasmaBuffer

    self.client.plasma_get.argtypes = [ctypes.c_int, PlasmaID]
    self.client.plasma_get.restype = PlasmaBuffer

    self.client.plasma_seal.argtypes = [ctypes.c_int, PlasmaID]
    self.client.plasma_seal.restype = None

    self.buffer_from_memory = ctypes.pythonapi.PyBuffer_FromMemory
    self.buffer_from_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_memory.restype = ctypes.py_object

    self.buffer_from_read_write_memory = ctypes.pythonapi.PyBuffer_FromReadWriteMemory
    self.buffer_from_read_write_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    self.buffer_from_read_write_memory.restype = ctypes.py_object

    self.sock = self.client.plasma_store_connect(socket_name)

  def create(self, object_id, size):
    """Create a new buffer in the PlasmaStore for a particular object ID.

    The returned buffer is mutable until seal is called.

    Args:
      object_id (str): A string used to identify an object.
      size (int): The size in bytes of the created buffer.
    """
    buf = self.client.plasma_create(self.sock, make_plasma_id(object_id), size)
    return self.buffer_from_read_write_memory(buf.data, buf.size)

  def get(self, object_id):
    """Create a buffer from the PlasmaStore based on object ID.

    This method can only be called after the buffer has been sealed. The
    retrieved buffer is immutable.

    Args:
      object_id (str): A string used to identify an object.
    """
    buf = self.client.plasma_get(self.sock, make_plasma_id(object_id))
    return self.buffer_from_memory(buf.data, buf.size)

  def seal(self, object_id):
    """Seal the buffer in the PlasmaStore for a particular object ID.

    Once a buffer has been sealed, the buffer is immutable and can only be
    accessed through get.

    Args:
      object_id (str): A string used to identify an object.
    """
    self.client.plasma_seal(self.sock, make_plasma_id(object_id))
