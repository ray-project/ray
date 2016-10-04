import ctypes
import os

photon_client_library_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/photon_client.so")
photon_client_library = ctypes.cdll.LoadLibrary(photon_client_library_path)
photon_client_library.alloc_task_spec.restype = ctypes.c_void_p
photon_client_library.photon_connect.restype = ctypes.c_void_p
photon_client_library.photon_submit.restype = None
photon_client_library.photon_get_task.restype = ctypes.c_void_p

ID = ctypes.c_ubyte * 20

buffer_from_read_write_memory = ctypes.pythonapi.PyBuffer_FromReadWriteMemory
buffer_from_read_write_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
buffer_from_read_write_memory.restype = ctypes.py_object

buffer_from_memory = ctypes.pythonapi.PyBuffer_FromMemory
buffer_from_memory.argtypes = [ctypes.c_void_p, ctypes.c_int64]
buffer_from_memory.restype = ctypes.py_object

photon_client_library.task_function.restype = ctypes.c_void_p
photon_client_library.task_num_args.restype = ctypes.c_int64
photon_client_library.task_num_returns.restype = ctypes.c_int64
photon_client_library.task_arg_type.restype = ctypes.c_int8
photon_client_library.task_arg_id.restype = ctypes.c_void_p
photon_client_library.task_arg_val.restype = ctypes.c_void_p
photon_client_library.task_arg_length.restype = ctypes.c_void_p
photon_client_library.task_return.restype = ctypes.c_void_p


class TaskInfo(object):
  def __init__(self, function_id, args, return_ids):
    self.function_id = function_id
    self.args = args
    self.return_ids = return_ids

def extract_task(c_task):
  function_id = buffer_from_memory(photon_client_library.task_function(c_task), 20)[:]
  num_args = photon_client_library.task_num_args(c_task)
  num_returns = photon_client_library.task_num_returns(c_task)
  arg_vals_and_ids = []
  for i in range(num_args):
    arg_type = photon_client_library.task_arg_type(c_task, i)
    if arg_type == 0:
      arg_id = buffer_from_memory(photon_client_library.task_arg_id(c_task, i), 20)
      arg_vals_and_ids.append((arg_type, arg_id))
    elif arg_type == 1:
      arg_val = photon_client_library.task_arg_val(c_task, i)[:]
      arg_length = photon_client_library.task_arg_length(c_task, i)
      arg_value = buffer_from_memory(arg_val, arg_length)[:]
      arg_vals_and_ids.append((arg_type, arg_value))
    else:
      raise Exception("arg_type must be 0 or 1")
  return_ids = []
  for i in range(num_returns):
    ret_id = buffer_from_memory(photon_client_library.task_return(c_task, i), 20)
    return_ids.append(ret_id[:])
  return TaskInfo(function_id, arg_vals_and_ids, return_ids)

class UniqueID(ctypes.Structure):
  _fields_ = [("unique_id", ID)]

def make_id(string):
  if len(string) != 20:
    raise Exception("PlasmaIDs must be 20 characters long")
  unique_id = map(ord, string)
  return UniqueID(unique_id=ID(*unique_id))

class Task(object):
  def __init__(self, function_id, args, return_ids):
    function_id = make_id(function_id)
    self.task_spec = ctypes.c_void_p(photon_client_library.alloc_task_spec(function_id, len(args), 1, 0))
    for arg in args:
      photon_client_library.task_args_add_ref(self.task_spec, make_id(arg))

    # Add return IDs. This may not be the appropriate place for this.
    num_returns = photon_client_library.task_num_returns(self.task_spec)
    for i in range(num_returns):
      ret_id = buffer_from_read_write_memory(photon_client_library.task_return(self.task_spec, i), 20)
      for j in range(20):
        ret_id[j] = return_ids[i][j]

  def __del__(self):
    photon_client_library.free_task_spec(self.task_spec)

class PhotonClient(object):

  def __init__(self, socket_name):
    self.photon_conn = ctypes.c_void_p(photon_client_library.photon_connect(socket_name))

  def submit(self, function_id, args, return_ids):
    task = Task(function_id, args, return_ids)
    photon_client_library.photon_submit(self.photon_conn, task.task_spec)

  def get_task(self):
    c_task = ctypes.c_void_p(photon_client_library.photon_get_task(self.photon_conn))
    task = c_task # TODO Extract the actual task. EXTRACT...(c_task)
    # photon_client_library.free_task_spec(c_task)
    return extract_task(task)
