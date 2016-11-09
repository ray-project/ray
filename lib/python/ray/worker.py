from __future__ import print_function

import hashlib
import os
import sys
import time
import traceback
import copy
import funcsigs
import numpy as np
import colorama
import atexit
import random
import redis
import threading
import string

# Ray modules
import config
import pickling
import serialization
import services
import numbuf
import photon
import plasma

SCRIPT_MODE = 0
WORKER_MODE = 1
PYTHON_MODE = 2
SILENT_MODE = 3

def random_string():
  return np.random.bytes(20)

def random_object_id():
  return photon.ObjectID(random_string())

class FunctionID(object):
  def __init__(self, function_id):
    self.function_id = function_id

  def id(self):
    return self.function_id

contained_objectids = []
def numbuf_serialize(value):
  """This serializes a value and tracks the object IDs inside the value.

  We also define a custom ObjectID serializer which also closes over the global
  variable contained_objectids, and whenever the custom serializer is called, it
  adds the releevant ObjectID to the list contained_objectids. The list
  contained_objectids should be reset between calls to numbuf_serialize.

  Args:
    value: A Python object that will be serialized.

  Returns:
    The serialized object.
  """
  assert len(contained_objectids) == 0, "This should be unreachable."
  return numbuf.serialize_list([value])

class RayTaskError(Exception):
  """An object used internally to represent a task that threw an exception.

  If a task throws an exception during execution, a RayTaskError is stored in
  the object store for each of the task's outputs. When an object is retrieved
  from the object store, the Python method that retrieved it checks to see if
  the object is a RayTaskError and if it is then an exception is thrown
  propagating the error message.

  Currently, we either use the exception attribute or the traceback attribute
  but not both.

  Attributes:
    function_name (str): The name of the function that failed and produced the
      RayTaskError.
    exception (Exception): The exception object thrown by the failed task.
    traceback_str (str): The traceback from the exception.
  """

  def __init__(self, function_name, exception, traceback_str):
    """Initialize a RayTaskError."""
    self.function_name = function_name
    if isinstance(exception, RayGetError) or isinstance(exception, RayGetArgumentError):
      self.exception = exception
    else:
      self.exception = None
    self.traceback_str = traceback_str

  def __str__(self):
    """Format a RayTaskError as a string."""
    if self.traceback_str is None:
      # This path is taken if getting the task arguments failed.
      return "Remote function {}{}{} failed with:\n\n{}".format(colorama.Fore.RED, self.function_name, colorama.Fore.RESET, self.exception)
    else:
      # This path is taken if the task execution failed.
      return "Remote function {}{}{} failed with:\n\n{}".format(colorama.Fore.RED, self.function_name, colorama.Fore.RESET, self.traceback_str)

class RayGetError(Exception):
  """An exception used when get is called on an output of a failed task.

  Attributes:
    objectid (lib.ObjectID): The ObjectID that get was called on.
    task_error (RayTaskError): The RayTaskError object created by the failed
      task.
  """

  def __init__(self, objectid, task_error):
    """Initialize a RayGetError object."""
    self.objectid = objectid
    self.task_error = task_error

  def __str__(self):
    """Format a RayGetError as a string."""
    return "Could not get objectid {}. It was created by remote function {}{}{} which failed with:\n\n{}".format(self.objectid, colorama.Fore.RED, self.task_error.function_name, colorama.Fore.RESET, self.task_error)

class RayGetArgumentError(Exception):
  """An exception used when a task's argument was produced by a failed task.

  Attributes:
    argument_index (int): The index (zero indexed) of the failed argument in
      present task's remote function call.
    function_name (str): The name of the function for the current task.
    objectid (lib.ObjectID): The ObjectID that was passed in as the argument.
    task_error (RayTaskError): The RayTaskError object created by the failed
      task.
  """

  def __init__(self, function_name, argument_index, objectid, task_error):
    """Initialize a RayGetArgumentError object."""
    self.argument_index = argument_index
    self.function_name = function_name
    self.objectid = objectid
    self.task_error = task_error

  def __str__(self):
    """Format a RayGetArgumentError as a string."""
    return "Failed to get objectid {} as argument {} for remote function {}{}{}. It was created by remote function {}{}{} which failed with:\n{}".format(self.objectid, self.argument_index, colorama.Fore.RED, self.function_name, colorama.Fore.RESET, colorama.Fore.RED, self.task_error.function_name, colorama.Fore.RESET, self.task_error)


class Reusable(object):
  """An Python object that can be shared between tasks.

  Attributes:
    initializer (Callable[[], object]): A function used to create and initialize
      the reusable variable.
    reinitializer (Optional[Callable[[object], object]]): An optional function
      used to reinitialize the reusable variable after it has been used. This
      argument can be used as an optimization if there is a fast way to
      reinitialize the state of the variable other than rerunning the
      initializer.
  """

  def __init__(self, initializer, reinitializer=None):
    """Initialize a Reusable object."""
    if not callable(initializer):
      raise Exception("When creating a RayReusable, initializer must be a function.")
    self.initializer = initializer
    if reinitializer is None:
      # If no reinitializer is passed in, use a wrapped version of the initializer.
      reinitializer = lambda value: initializer()
    if not callable(reinitializer):
      raise Exception("When creating a RayReusable, reinitializer must be a function.")
    self.reinitializer = reinitializer

class RayReusables(object):
  """An object used to store Python variables that are shared between tasks.

  Each worker process will have a single RayReusables object. This class serves
  two purposes. First, some objects are not serializable, and so the code that
  creates those objects must be run on the worker that uses them. This class is
  responsible for running the code that creates those objects. Second, some of
  these objects are expensive to create, and so they should be shared between
  tasks. However, if a task mutates a variable that is shared between tasks,
  then the behavior of the overall program may be nondeterministic (it could
  depend on scheduling decisions). To fix this, if a task uses a one of these
  shared objects, then that shared object will be reinitialized after the task
  finishes. Since the initialization may be expensive, the user can pass in
  custom reinitialization code that resets the state of the shared variable to
  the way it was after initialization. If the reinitialization code does not do
  this, then the behavior of the overall program is undefined.

  Attributes:
    _names (List[str]): A list of the names of all the reusable variables.
    _reinitializers (Dict[str, Callable]): A dictionary mapping the name of the
      reusable variables to the corresponding reinitializer.
    _running_remote_function_locally (bool): A flag used to indicate if a remote
      function is running locally on the driver so that we can simulate the same
      behavior as running a remote function remotely.
    _reusables: A dictionary mapping the name of a reusable variable to the
      value of the reusable variable.
    _local_mode_reusables: A copy of _reusables used on the driver when running
      remote functions locally on the driver. This is needed because there are
      two ways in which reusable variables can be used on the driver. The first
      is that the driver's copy can be manipulated. This copy is never reset
      (think of the driver as a single long-running task). The second way is
      that a remote function can be run locally on the driver, and this remote
      function needs access to a copy of the reusable variable, and that copy
      must be reinitialized after use.
    _cached_reusables (List[Tuple[str, Reusable]]): A list of pairs. The first
      element of each pair is the name of a reusable variable, and the second
      element is the Reusable object. This list is used to store reusable
      variables that are defined before the driver is connected. Once the driver
      is connected, these variables will be exported.
    _used (List[str]): A list of the names of all the reusable variables that
      have been accessed within the scope of the current task. This is reset to
      the empty list after each task.
  """

  def __init__(self):
    """Initialize a RayReusables object."""
    self._names = set()
    self._reinitializers = {}
    self._running_remote_function_locally = False
    self._reusables = {}
    self._local_mode_reusables = {}
    self._cached_reusables = []
    self._used = set()
    self._slots = ("_names", "_reinitializers", "_running_remote_function_locally", "_reusables", "_local_mode_reusables", "_cached_reusables", "_used", "_slots", "_create_reusable_variable", "_reinitialize", "__getattribute__", "__setattr__", "__delattr__")
    # CHECKPOINT: Attributes must not be added after _slots. The above attributes are protected from deletion.

  def _create_reusable_variable(self, name, reusable):
    """Create a reusable variable locally.

    Args:
      name (str): The name of the reusable variable.
      reusable (Reusable): The reusable object to use to create the reusable
        variable.
    """
    self._names.add(name)
    self._reinitializers[name] = reusable.reinitializer
    self._reusables[name] = reusable.initializer()
    # We create a second copy of the reusable variable on the driver to use
    # inside of remote functions that run locally. This occurs when we start Ray
    # in PYTHON_MODE and when we call a remote function locally.
    if _mode() in [SCRIPT_MODE, SILENT_MODE, PYTHON_MODE]:
      self._local_mode_reusables[name] = reusable.initializer()

  def _reinitialize(self):
    """Reinitialize the reusable variables that the current task used."""
    for name in self._used:
      current_value = self._reusables[name]
      new_value = self._reinitializers[name](current_value)
      # If we are on the driver, reset the copy of the reusable variable in the
      # _local_mode_reusables dictionary.
      if _mode() in [SCRIPT_MODE, SILENT_MODE, PYTHON_MODE]:
        assert self._running_remote_function_locally
        self._local_mode_reusables[name] = new_value
      else:
        self._reusables[name] = new_value
    self._used.clear() # Reset the _used list.

  def __getattribute__(self, name):
    """Get an attribute. This handles reusable variables as a special case.

    When __getattribute__ is called with the name of a reusable variable, that
    name is added to the list of variables that were used in the current task.

    Args:
      name (str): The name of the attribute to get.
    """
    if name == "_slots":
      return object.__getattribute__(self, name)
    if name in self._slots:
      return object.__getattribute__(self, name)
    # Handle various fields that are not reusable variables.
    if name not in self._names:
      return object.__getattribute__(self, name)
    # Make a note of the fact that the reusable variable has been used.
    if name in self._names and name not in self._used:
      self._used.add(name)
    if self._running_remote_function_locally:
      return self._local_mode_reusables[name]
    else:
      return self._reusables[name]

  def __setattr__(self, name, value):
    """Set an attribute. This handles reusable variables as a special case.

    This is used to create reusable variables. When it is called, it runs the
    function for initializing the variable to create the variable. If this is
    called on the driver, then the functions for initializing and reinitializing
    the variable are shipped to the workers.

    If this is called before ray.init has been run, then the reusable variable
    will be cached and it will be created and exported when connect is called.

    Args:
      name (str): The name of the attribute to set. This is either a whitelisted
        name or it is treated as the name of a reusable variable.
      value: If name is a whitelisted name, then value can be any value. If name
        is the name of a reusable variable, then this is a Reusable object.
    """
    try:
      slots = self._slots
    except AttributeError:
      slots = ()
    if slots == ():
      return object.__setattr__(self, name, value)
    if name in slots:
      return object.__setattr__(self, name, value)
    reusable = value
    if not issubclass(type(reusable), Reusable):
      raise Exception("To set a reusable variable, you must pass in a Reusable object")
    # If ray.init has not been called, cache the reusable variable to export
    # later. Otherwise, export the reusable variable to the workers and define
    # it locally.
    if _mode() is None:
      self._cached_reusables.append((name, reusable))
    else:
      # If we are on the driver, export the reusable variable to all the
      # workers.
      if _mode() in [SCRIPT_MODE, SILENT_MODE]:
        _export_reusable_variable(name, reusable)
      # Define the reusable variable locally.
      self._create_reusable_variable(name, reusable)
      # Create an empty attribute with the name of the reusable variable. This
      # allows the Python interpreter to do tab complete properly.
      object.__setattr__(self, name, None)

  def __delattr__(self, name):
    """We do not allow attributes of RayReusables to be deleted.

    Args:
      name (str): The name of the attribute to delete.
    """
    raise Exception("Attempted deletion of attribute {}. Attributes of a RayReusable object may not be deleted.".format(name))

class ObjectFixture(object):
  """This is used to handle releasing objects backed by the object store.

  This keeps a PlasmaBuffer in scope as long as an object that is backed by that
  PlasmaBuffer is in scope. This prevents memory in the object store from getting
  released while it is still being used to back a Python object.
  """

  def __init__(self, plasma_buffer):
    """Initialize an ObjectFixture object."""
    self.plasma_buffer = plasma_buffer

class Worker(object):
  """A class used to define the control flow of a worker process.

  Note:
    The methods in this class are considered unexposed to the user. The
    functions outside of this class are considered exposed.

  Attributes:
    functions (Dict[str, Callable]): A dictionary mapping the name of a remote
      function to the remote function itself. This is the set of remote
      functions that can be executed by this worker.
    connected (bool): True if Ray has been started and False otherwise.
    mode: The mode of the worker. One of SCRIPT_MODE, PYTHON_MODE, SILENT_MODE,
      and WORKER_MODE.
    cached_remote_functions (List[Tuple[str, str]]): A list of pairs
      representing the remote functions that were defined before he worker
      called connect. The first element is the name of the remote function, and
      the second element is the serialized remote function. When the worker
      eventually does call connect, if it is a driver, it will export these
      functions to the scheduler. If cached_remote_functions is None, that means
      that connect has been called already.
    cached_functions_to_run (List): A list of functions to run on all of the
      workers that should be exported as soon as connect is called.
    driver_export_counter (int): The number of exports that the driver has
      exported. This is only used on the driver.
    worker_import_counter (int): The number of exports that the worker has
      imported so far. This is only used on the workers.
  """

  def __init__(self):
    """Initialize a Worker object."""
    self.functions = {}
    self.num_return_vals = {}
    self.function_names = {}
    self.function_export_counters = {}
    self.connected = False
    self.mode = None
    self.cached_remote_functions = []
    self.cached_functions_to_run = []
    self.driver_export_counter = 0
    self.worker_import_counter = 0

  def set_mode(self, mode):
    """Set the mode of the worker.

    The mode SCRIPT_MODE should be used if this Worker is a driver that is being
    run as a Python script or interactively in a shell. It will print
    information about task failures.

    The mode WORKER_MODE should be used if this Worker is not a driver. It will
    not print information about tasks.

    The mode PYTHON_MODE should be used if this Worker is a driver and if you
    want to run the driver in a manner equivalent to serial Python for debugging
    purposes. It will not send remote function calls to the scheduler and will
    insead execute them in a blocking fashion.

    The mode SILENT_MODE should be used only during testing. It does not print
    any information about errors because some of the tests intentionally fail.

    args:
      mode: One of SCRIPT_MODE, WORKER_MODE, PYTHON_MODE, and SILENT_MODE.
    """
    self.mode = mode
    colorama.init()

  def put_object(self, objectid, value):
    """Put value in the local object store with object id objectid.

    This assumes that the value for objectid has not yet been placed in the
    local object store.

    Args:
      objectid (object_id.ObjectID): The object ID of the value to be put.
      value (serializable object): The value to put in the object store.
    """
    # Serialize and put the object in the object store.
    schema, size, serialized = numbuf_serialize(value)
    size = size + 4096 * 4 + 8 # The last 8 bytes are for the metadata offset. This is temporary.
    buff = self.plasma_client.create(objectid.id(), size, bytearray(schema))
    data = np.frombuffer(buff.buffer, dtype="byte")[8:]
    metadata_offset = numbuf.write_to_buffer(serialized, memoryview(data))
    np.frombuffer(buff.buffer, dtype="int64", count=1)[0] = metadata_offset
    self.plasma_client.seal(objectid.id())

    global contained_objectids
    # Optionally do something with the contained_objectids here.
    contained_objectids = []

  def get_object(self, objectid):
    """Get the value in the local object store associated with objectid.

    Return the value from the local object store for objectid. This will block
    until the value for objectid has been written to the local object store.

    Args:
      objectid (object_id.ObjectID): The object ID of the value to retrieve.
    """
    buff = self.plasma_client.get(objectid.id())
    metadata = self.plasma_client.get_metadata(objectid.id())
    metadata_size = len(metadata)
    data = np.frombuffer(buff.buffer, dtype="byte")[8:]
    metadata_offset = int(np.frombuffer(buff.buffer, dtype="int64", count=1)[0])
    serialized = numbuf.read_from_buffer(memoryview(data), bytearray(metadata), metadata_offset)
    # Create an ObjectFixture. If the object we are getting is backed by the
    # PlasmaBuffer, this ObjectFixture will keep the PlasmaBuffer in scope as
    # long as the object is in scope.
    object_fixture = ObjectFixture(buff)
    deserialized = numbuf.deserialize_list(serialized, object_fixture)
    # Unwrap the object from the list (it was wrapped put_object).
    assert len(deserialized) == 1
    return deserialized[0]

  def submit_task(self, function_id, func_name, args):
    """Submit a remote task to the scheduler.

    Tell the scheduler to schedule the execution of the function with name
    func_name with arguments args. Retrieve object IDs for the outputs of
    the function from the scheduler and immediately return them.

    Args:
      func_name (str): The name of the function to be executed.
      args (List[Any]): The arguments to pass into the function. Arguments can
        be object IDs or they can be values. If they are values, they
        must be serializable objecs.
    """
    # Put large or complex arguments that are passed by value in the object
    # store first.
    args_for_photon = []
    for arg in args:
      if isinstance(arg, photon.ObjectID):
        args_for_photon.append(arg)
      elif photon.check_simple_value(arg):
        args_for_photon.append(arg)
      else:
        args_for_photon.append(put(arg))

    # Submit the task to Photon.
    task = photon.Task(photon.ObjectID(function_id.id()),
                       args_for_photon,
                       self.num_return_vals[function_id.id()],
                       self.current_task_id,
                       self.task_index)
    # Increment the worker's task index to track how many tasks have been
    # submitted by the current task so far.
    self.task_index += 1
    self.photon_client.submit(task)

    return task.returns()

  def run_function_on_all_workers(self, function):
    """Run arbitrary code on all of the workers.

    This function will first be run on the driver, and then it will be exported
    to all of the workers to be run. It will also be run on any new workers that
    register later. If ray.init has not been called yet, then cache the function
    and export it later.

    Args:
      function (Callable): The function to run on all of the workers. It should
        not take any arguments. If it returns anything, its return values will
        not be used.
    """
    if self.mode not in [None, SCRIPT_MODE, SILENT_MODE, PYTHON_MODE]:
      raise Exception("run_function_on_all_workers can only be called on a driver.")
    # If ray.init has not been called yet, then cache the function and export it
    # when connect is called. Otherwise, run the function on all workers.
    if self.mode is None:
      self.cached_functions_to_run.append(function)
    else:
      # First run the function on the driver.
      function(self)
      # Run the function on all workers.
      function_to_run_id = random_string()
      key = "FunctionsToRun:{}".format(function_to_run_id)
      self.redis_client.hmset(key, {"function_id": function_to_run_id,
                                    "function": pickling.dumps(function)})
      self.redis_client.rpush("Exports", key)
      self.driver_export_counter += 1

global_worker = Worker()
"""Worker: The global Worker object for this worker process.

We use a global Worker object to ensure that there is a single worker object
per worker process.
"""

reusables = RayReusables()
"""RayReusables: The reusable variables that are shared between tasks.

Each worker process has its own RayReusables object, and these objects should be
the same in all workers. This is used for storing variables that are not
serializable but must be used by remote tasks. In addition, it is used to
reinitialize these variables after they are used so that changes to their state
made by one task do not affect other tasks.
"""

class RayConnectionError(Exception):
  pass

def check_connected(worker=global_worker):
  """Check if the worker is connected.

  Raises:
    Exception: An exception is raised if the worker is not connected.
  """
  if not worker.connected:
    raise RayConnectionError("This command cannot be called before Ray has been started. You can start Ray with 'ray.init(start_ray_local=True, num_workers=1)'.")

def print_failed_task(task_status):
  """Print information about failed tasks.

  Args:
    task_status (Dict): A dictionary containing the name, operationid, and
      error message for a failed task.
  """
  print("""
    Error: Task failed
      Function Name: {}
      Task ID: {}
      Error Message: \n{}
  """.format(task_status["function_name"], task_status["operationid"], task_status["error_message"]))

def error_info(worker=global_worker):
  """Return information about failed tasks."""
  check_connected(worker)
  result = {"TaskError": [],
            "RemoteFunctionImportError": [],
            "ReusableVariableImportError": [],
            "ReusableVariableReinitializeError": [],
            "FunctionToRunError": []
            }
  error_keys = worker.redis_client.lrange("ErrorKeys", 0, -1)
  for error_key in error_keys:
    error_type = error_key.split(":", 1)[0]
    error_contents = worker.redis_client.hgetall(error_key)
    result[error_type].append(error_contents)

  return result

def initialize_numbuf(worker=global_worker):
  """Initialize the serialization library.

  This defines a custom serializer for object IDs and also tells numbuf to
  serialize several exception classes that we define for error handling.
  """
  # Define a custom serializer and deserializer for handling Object IDs.
  def objectid_custom_serializer(obj):
    class_identifier = serialization.class_identifier(type(obj))
    contained_objectids.append(obj)
    return obj.id()
  def objectid_custom_deserializer(serialized_obj):
    return photon.ObjectID(serialized_obj)
  serialization.add_class_to_whitelist(photon.ObjectID, pickle=False, custom_serializer=objectid_custom_serializer, custom_deserializer=objectid_custom_deserializer)

  if worker.mode in [SCRIPT_MODE, SILENT_MODE]:
    # These should only be called on the driver because register_class will
    # export the class to all of the workers.
    register_class(RayTaskError)
    register_class(RayGetError)
    register_class(RayGetArgumentError)

def init(start_ray_local=False, num_workers=None, driver_mode=SCRIPT_MODE):
  """Either connect to an existing Ray cluster or start one and connect to it.

  This method handles two cases. Either a Ray cluster already exists and we
  just attach this driver to it, or we start all of the processes associated
  with a Ray cluster and attach to the newly started cluster.

  Args:
    start_ray_local (Optional[bool]): If True then this will start a scheduler
      an object store, and some workers. If False, this will attach to an
      existing Ray cluster.
    num_workers (Optional[int]): The number of workers to start if
      start_ray_local is True.
    driver_mode (Optional[bool]): The mode in which to start the driver. This
      should be one of SCRIPT_MODE, PYTHON_MODE, and SILENT_MODE.

  Returns:
    The address of the Redis server.

  Raises:
    Exception: An exception is raised if an inappropriate combination of
      arguments is passed in.
  """
  if driver_mode == PYTHON_MODE:
    # If starting Ray in PYTHON_MODE, don't start any other processes.
    address_info = {}
  elif start_ray_local:
    # In this case, we launch a scheduler, a new object store, and some workers,
    # and we connect to them.
    if driver_mode not in [SCRIPT_MODE, PYTHON_MODE, SILENT_MODE]:
      raise Exception("If start_ray_local=True, then driver_mode must be in [ray.SCRIPT_MODE, ray.PYTHON_MODE, ray.SILENT_MODE].")
    # Use the address 127.0.0.1 in local mode.
    num_workers = 1 if num_workers is None else num_workers
    # Start the scheduler, object store, and some workers. These will be killed
    # by the call to cleanup(), which happens when the Python script exits.
    address_info = services.start_ray_local(num_workers=num_workers)
  else:
    raise Exception("This mode is currently not enabled.")
  # Connect this driver to Redis, the object store, and the local scheduler. The
  # corresponing call to disconnect will happen in the call to cleanup() when
  # the Python script exits.
  connect(address_info, driver_mode, worker=global_worker)
  return address_info

def cleanup(worker=global_worker):
  """Disconnect the driver, and terminate any processes started in init.

  This will automatically run at the end when a Python process that uses Ray
  exits. It is ok to run this twice in a row. Note that we manually call
  services.cleanup() in the tests because we need to start and stop many
  clusters in the tests, but the import and exit only happen once.
  """
  disconnect(worker)
  worker.set_mode(None)
  worker.driver_export_counter = 0
  worker.worker_import_counter = 0
  if hasattr(worker, "plasma_client"):
    worker.plasma_client.shutdown()
  services.cleanup()

atexit.register(cleanup)

def print_error_messages(worker):
  """Print error messages in the background on the driver.

  This runs in a separate thread on the driver and prints error messages in the
  background.
  """
  worker.error_message_pubsub_client = worker.redis_client.pubsub()
  # Exports that are published after the call to
  # error_message_pubsub_client.psubscribe and before the call to
  # error_message_pubsub_client.listen will still be processed in the loop.
  worker.error_message_pubsub_client.psubscribe("__keyspace@0__:ErrorKeys")
  num_errors_printed = 0

  # Get the exports that occurred before the call to psubscribe.
  with worker.lock:
    error_keys = worker.redis_client.lrange("ErrorKeys", 0, -1)
    for error_key in error_keys:
      error_message = worker.redis_client.hget(error_key, "message")
      print(error_message)
      num_errors_printed += 1

  try:
    for msg in worker.error_message_pubsub_client.listen():
      with worker.lock:
        for error_key in worker.redis_client.lrange("ErrorKeys", num_errors_printed, -1):
          error_message = worker.redis_client.hget(error_key, "message")
          print(error_message)
          num_errors_printed += 1
  except redis.ConnectionError:
    # When Redis terminates the listen call will throw a ConnectionError, which
    # we catch here.
    pass

def fetch_and_register_remote_function(key, worker=global_worker):
  """Import a remote function."""
  function_id_str, function_name, serialized_function, num_return_vals, module, function_export_counter = worker.redis_client.hmget(key, ["function_id", "name", "function", "num_return_vals", "module", "function_export_counter"])
  function_id = photon.ObjectID(function_id_str)
  num_return_vals = int(num_return_vals)
  function_export_counter = int(function_export_counter)
  try:
    function = pickling.loads(serialized_function)
  except:
    # If an exception was thrown when the remote function was imported, we
    # record the traceback and notify the scheduler of the failure.
    traceback_str = format_error_message(traceback.format_exc())
    # Log the error message.
    error_key = "RemoteFunctionImportError:{}".format(function_id.id())
    worker.redis_client.hmset(error_key, {"function_id": function_id.id(),
                                          "function_name": function_name,
                                          "message": traceback_str})
    worker.redis_client.rpush("ErrorKeys", error_key)
  else:
    # TODO(rkn): Why is the below line necessary?
    function.__module__ = module
    function_name = "{}.{}".format(function.__module__, function.__name__)
    worker.functions[function_id.id()] = remote(num_return_vals=num_return_vals, function_id=function_id)(function)
    worker.function_names[function_id.id()] = function_name
    worker.num_return_vals[function_id.id()] = num_return_vals
    worker.function_export_counters[function_id.id()] = function_export_counter
    # Add the function to the function table.
    worker.redis_client.rpush("FunctionTable:{}".format(function_id.id()), worker.worker_id)

def fetch_and_register_reusable_variable(key, worker=global_worker):
  """Import a reusable variable."""
  reusable_variable_name, serialized_initializer, serialized_reinitializer = worker.redis_client.hmget(key, ["name", "initializer", "reinitializer"])
  try:
    initializer = pickling.loads(serialized_initializer)
    reinitializer = pickling.loads(serialized_reinitializer)
    reusables.__setattr__(reusable_variable_name, Reusable(initializer, reinitializer))
  except:
    # If an exception was thrown when the reusable variable was imported, we
    # record the traceback and notify the scheduler of the failure.
    traceback_str = format_error_message(traceback.format_exc())
    # Log the error message.
    error_key = "ReusableVariableImportError:{}".format(random_string())
    worker.redis_client.hmset(error_key, {"name": reusable_variable_name,
                                          "message": traceback_str})
    worker.redis_client.rpush("ErrorKeys", error_key)

def fetch_and_execute_function_to_run(key, worker=global_worker):
  """Run on arbitrary function on the worker."""
  serialized_function, = worker.redis_client.hmget(key, ["function"])
  try:
    # Deserialize the function.
    function = pickling.loads(serialized_function)
    # Run the function.
    function(worker)
  except:
    # If an exception was thrown when the function was run, we record the
    # traceback and notify the scheduler of the failure.
    traceback_str = traceback.format_exc()
    # Log the error message.
    name = function.__name__  if "function" in locals() and hasattr(function, "__name__") else ""
    error_key = "FunctionToRunError:{}".format(random_string())
    worker.redis_client.hmset(error_key, {"name": name,
                                          "message": traceback_str})
    worker.redis_client.rpush("ErrorKeys", error_key)

def import_thread(worker):
  worker.import_pubsub_client = worker.redis_client.pubsub()
  # Exports that are published after the call to import_pubsub_client.psubscribe
  # and before the call to import_pubsub_client.listen will still be processed
  # in the loop.
  worker.import_pubsub_client.psubscribe("__keyspace@0__:Exports")
  worker_info_key = "WorkerInfo:{}".format(worker.worker_id)
  worker.redis_client.hset(worker_info_key, "export_counter", 0)
  worker.worker_import_counter = 0

  # Get the exports that occurred before the call to psubscribe.
  with worker.lock:
    export_keys = worker.redis_client.lrange("Exports", 0, -1)
    for key in export_keys:
      if key.startswith("RemoteFunction"):
        fetch_and_register_remote_function(key, worker=worker)
      elif key.startswith("ReusableVariables"):
        fetch_and_register_reusable_variable(key, worker=worker)
      elif key.startswith("FunctionsToRun"):
        fetch_and_execute_function_to_run(key, worker=worker)
      else:
        raise Exception("This code should be unreachable.")
      worker.redis_client.hincrby(worker_info_key, "export_counter", 1)
      worker.worker_import_counter += 1

  for msg in worker.import_pubsub_client.listen():
    with worker.lock:
      if msg["type"] == "psubscribe":
        continue
      assert msg["data"] == "rpush"
      num_imports = worker.redis_client.llen("Exports")
      assert num_imports >= worker.worker_import_counter
      for i in range(worker.worker_import_counter, num_imports):
        key = worker.redis_client.lindex("Exports", i)
        if key.startswith("RemoteFunction"):
          fetch_and_register_remote_function(key, worker=worker)
        elif key.startswith("ReusableVariables"):
          fetch_and_register_reusable_variable(key, worker=worker)
        elif key.startswith("FunctionsToRun"):
          fetch_and_execute_function_to_run(key, worker=worker)
        else:
          raise Exception("This code should be unreachable.")
        worker.redis_client.hincrby(worker_info_key, "export_counter", 1)
        worker.worker_import_counter += 1

def connect(address_info, mode=WORKER_MODE, worker=global_worker):
  """Connect this worker to the scheduler and an object store.

  Args:
    address_info (dict): This contains the entries node_ip_address,
      redis_address, object_store_name, object_store_manager_name, and
      local_scheduler_name.
    mode: The mode of the worker. One of SCRIPT_MODE, WORKER_MODE, PYTHON_MODE,
      and SILENT_MODE.
  """
  worker.worker_id = random_string()
  worker.connected = True
  worker.set_mode(mode)
  # If running Ray in PYTHON_MODE, there is no need to create call create_worker
  # or to start the worker service.
  if mode == PYTHON_MODE:
    return
  # Create a Redis client.
  worker.redis_client = redis.StrictRedis(host=address_info["node_ip_address"], port=address_info["redis_port"])
  worker.redis_client.config_set("notify-keyspace-events", "AKE")
  worker.lock = threading.Lock()
  # Create an object store client.
  worker.plasma_client = plasma.PlasmaClient(address_info["object_store_name"], address_info["object_store_manager_name"])
  # Create the local scheduler client.
  worker.photon_client = photon.PhotonClient(address_info["local_scheduler_name"])
  # Register the worker with Redis.
  if mode in [SCRIPT_MODE, SILENT_MODE]:
    worker.redis_client.rpush("Drivers", worker.worker_id)
  elif mode == WORKER_MODE:
    worker.redis_client.rpush("Workers", worker.worker_id)
  else:
    raise Exception("This code should be unreachable.")
  # If this is a driver, set the current task ID to a specific fixed value and
  # set the task index to 0.
  if mode in [SCRIPT_MODE, SILENT_MODE]:
    worker.current_task_id = photon.ObjectID("".join(chr(i) for i in range(20)))
    worker.task_index = 0
  # If this is a worker, then start a thread to import exports from the driver.
  if mode == WORKER_MODE:
    t = threading.Thread(target=import_thread, args=(worker,))
    # Making the thread a daemon causes it to exit when the main thread exits.
    t.daemon = True
    t.start()
  # If this is a driver running in SCRIPT_MODE, start a thread to print error
  # messages asynchronously in the background. Ideally the scheduler would push
  # messages to the driver's worker service, but we ran into bugs when trying to
  # properly shutdown the driver's worker service, so we are temporarily using
  # this implementation which constantly queries the scheduler for new error
  # messages.
  if mode == SCRIPT_MODE:
    t = threading.Thread(target=print_error_messages, args=(worker,))
    # Making the thread a daemon causes it to exit when the main thread exits.
    t.daemon = True
    t.start()
  # Initialize the serialization library. This registers some classes, and so
  # it must be run before we export all of the cached remote functions.
  initialize_numbuf()
  if mode in [SCRIPT_MODE, SILENT_MODE]:
    # Add the directory containing the script that is running to the Python
    # paths of the workers. Also add the current directory. Note that this
    # assumes that the directory structures on the machines in the clusters are
    # the same.
    script_directory = os.path.abspath(os.path.dirname(sys.argv[0]))
    current_directory = os.path.abspath(os.path.curdir)
    worker.run_function_on_all_workers(lambda worker: sys.path.insert(1, script_directory))
    worker.run_function_on_all_workers(lambda worker: sys.path.insert(1, current_directory))
    # TODO(rkn): Here we first export functions to run, then reusable variables,
    # then remote functions. The order matters. For example, one of the
    # functions to run may set the Python path, which is needed to import a
    # module used to define a reusable variable, which in turn is used inside a
    # remote function. We may want to change the order to simply be the order in
    # which the exports were defined on the driver. In addition, we will need to
    # retain the ability to decide what the first few exports are (mostly to set
    # the Python path). Additionally, note that the first exports to be defined
    # on the driver will be the ones defined in separate modules that are
    # imported by the driver.
    # Export cached functions_to_run.
    for function in worker.cached_functions_to_run:
      worker.run_function_on_all_workers(function)
    # Export cached reusable variables to the workers.
    for name, reusable_variable in reusables._cached_reusables:
      reusables.__setattr__(name, reusable_variable)
    # Export cached remote functions to the workers.
    for function_id, func_name, func, num_return_vals in worker.cached_remote_functions:
      export_remote_function(function_id, func_name, func, num_return_vals, worker)
  worker.cached_functions_to_run = None
  worker.cached_remote_functions = None
  reusables._cached_reusables = None

def disconnect(worker=global_worker):
  """Disconnect this worker from the scheduler and object store."""
  # Reset the list of cached remote functions so that if more remote functions
  # are defined and then connect is called again, the remote functions will be
  # exported. This is mostly relevant for the tests.
  worker.connected = False
  worker.cached_functions_to_run = []
  worker.cached_remote_functions = []
  reusables._cached_reusables = []

def register_class(cls, pickle=False, worker=global_worker):
  """Enable workers to serialize or deserialize objects of a particular class.

  This method runs the register_class function defined below on every worker,
  which will enable numbuf to properly serialize and deserialize objects of this
  class.

  Args:
    cls (type): The class that numbuf should serialize.
    pickle (bool): If False then objects of this class will be serialized by
      turning their __dict__ fields into a dictionary. If True, then objects
      of this class will be serialized using pickle.

  Raises:
    Exception: An exception is raised if pickle=False and the class cannot be
      efficiently serialized by Ray.
  """
  # If the worker is not a driver, then return. We do this so that Python
  # modules can register classes and these modules can be imported on workers
  # without any trouble.
  if worker.mode == WORKER_MODE:
    return
  # Raise an exception if cls cannot be serialized efficiently by Ray.
  if not pickle:
    serialization.check_serializable(cls)
  def register_class_for_serialization(worker):
    serialization.add_class_to_whitelist(cls, pickle=pickle)
  worker.run_function_on_all_workers(register_class_for_serialization)

def get(objectid, worker=global_worker):
  """Get a remote object or a list of remote objects from the object store.

  This method blocks until the object corresponding to objectid is available in
  the local object store. If this object is not in the local object store, it
  will be shipped from an object store that has it (once the object has been
  created). If objectid is a list, then the objects corresponding to each object
  in the list will be returned.

  Args:
    objectid: Object ID of the object to get or a list of object IDs to get.

  Returns:
    A Python object or a list of Python objects.
  """
  check_connected(worker)
  if worker.mode == PYTHON_MODE:
    return objectid # In PYTHON_MODE, ray.get is the identity operation (the input will actually be a value not an objectid)
  if isinstance(objectid, list):
    values = [worker.get_object(x) for x in objectid]
    for i, value in enumerate(values):
      if isinstance(value, RayTaskError):
        raise RayGetError(objectid[i], value)
    return values
  value = worker.get_object(objectid)
  if isinstance(value, RayTaskError):
    # If the result is a RayTaskError, then the task that created this object
    # failed, and we should propagate the error message here.
    raise RayGetError(objectid, value)
  return value

def put(value, worker=global_worker):
  """Store an object in the object store.

  Args:
    value (serializable object): The Python object to be stored.

  Returns:
    The object ID assigned to this value.
  """
  check_connected(worker)
  if worker.mode == PYTHON_MODE:
    return value # In PYTHON_MODE, ray.put is the identity operation
  objectid = random_object_id()
  worker.put_object(objectid, value)
  return objectid

def wait(object_ids, num_returns=1, timeout=None, worker=global_worker):
  """Return a list of IDs that are ready and a list of IDs that are not ready.

  If timeout is set, the function returns either when the requested number of
  IDs are ready or when the timeout is reached, whichever occurs first. If it is
  not set, the function simply waits until that number of objects is ready and
  returns that exact number of objectids.

  This method returns two lists. The first list consists of object IDs that
  correspond to objects that are stored in the object store. The second list
  corresponds to the rest of the object IDs (which may or may not be ready).

  Args:
    object_ids (List[ObjectID]): List of object IDs for objects that may
      or may not be ready.
    num_returns (int): The number of object IDs that should be returned.
    timeout (int): The maximum amount of time in milliseconds to wait before
      returning.

  Returns:
    A list of object IDs that are ready and a list of the remaining object IDs.
  """
  check_connected(worker)
  object_id_strs = [object_id.id() for object_id in object_ids]
  timeout = timeout if timeout is not None else 2 ** 36
  ready_ids, remaining_ids = worker.plasma_client.wait(object_id_strs, timeout, num_returns)
  ready_ids = [photon.ObjectID(object_id) for object_id in ready_ids]
  remaining_ids = [photon.ObjectID(object_id) for object_id in remaining_ids]
  return ready_ids, remaining_ids

def format_error_message(exception_message):
  """Improve the formatting of an exception thrown by a remote function.

  This method takes a traceback from an exception and makes it nicer by
  removing a few uninformative lines and adding some space to indent the
  remaining lines nicely.

  Args:
    exception_message (str): A message generated by traceback.format_exc().

  Returns:
    A string of the formatted exception message.
  """
  lines = exception_message.split("\n")
  # Remove lines 1, 2, 3, and 4, which are always the same, they just contain
  # information about the main loop.
  lines = lines[0:1] + lines[5:]
  return "\n".join(lines)

def main_loop(worker=global_worker):
  """The main loop a worker runs to receive and execute tasks.

  This method is an infinite loop. It waits to receive commands from the
  scheduler. A command may consist of a task to execute, a remote function to
  import, a reusable variable to import, or an order to terminate the worker
  process. The worker executes the command, notifies the scheduler of any errors
  that occurred while executing the command, and waits for the next command.
  """

  def process_task(task): # wrapping these lines in a function should cause the local variables to go out of scope more quickly, which is useful for inspecting reference counts
    """Execute a task assigned to this worker.

    This method deserializes a task from the scheduler, and attempts to execute
    the task. If the task succeeds, the outputs are stored in the local object
    store. If the task throws an exception, RayTaskError objects are stored in
    the object store to represent the failed task (these will be retrieved by
    calls to get or by subsequent tasks that use the outputs of this task).
    After the task executes, the worker resets any reusable variables that were
    accessed by the task.
    """
    worker.current_task_id = task.task_id()
    worker.task_index = 0
    function_id = task.function_id()
    args = task.arguments()
    return_object_ids = task.returns()
    function_name = worker.function_names[function_id.id()]
    try:
      arguments = get_arguments_for_execution(worker.functions[function_id.id()], args, worker) # get args from objstore
      outputs = worker.functions[function_id.id()].executor(arguments) # execute the function
      if len(return_object_ids) == 1:
        outputs = (outputs,)
      store_outputs_in_objstore(return_object_ids, outputs, worker) # store output in local object store
    except Exception as e:
      # If the task threw an exception, then record the traceback. We determine
      # whether the exception was thrown in the task execution by whether the
      # variable "arguments" is defined.
      traceback_str = format_error_message(traceback.format_exc()) if "arguments" in locals() else None
      failure_object = RayTaskError(function_name, e, traceback_str)
      failure_objects = [failure_object for _ in range(len(return_object_ids))]
      store_outputs_in_objstore(return_object_ids, failure_objects, worker)
      # Log the error message.
      error_key = "TaskError:{}".format(random_string())
      worker.redis_client.hmset(error_key, {"function_id": function_id.id(),
                                            "function_name": function_name,
                                            "message": traceback_str})
      worker.redis_client.rpush("ErrorKeys", error_key)
    try:
      # Reinitialize the values of reusable variables that were used in the task
      # above so that changes made to their state do not affect other tasks.
      reusables._reinitialize()
    except Exception as e:
      # The attempt to reinitialize the reusable variables threw an exception.
      # We record the traceback and notify the scheduler.
      traceback_str = format_error_message(traceback.format_exc())
      error_key = "ReusableVariableReinitializeError:{}".format(random_string())
      worker.redis_client.hmset(error_key, {"task_id": "NOTIMPLEMENTED",
                                            "function_id": function_id.id(),
                                            "function_name": function_name,
                                            "message": traceback_str})
      worker.redis_client.rpush("ErrorKeys", error_key)

  while True:
    task = worker.photon_client.get_task()
    function_id = task.function_id()
    # Check that the number of imports we have is at least as great as the
    # export counter for the task. If not, wait until we have imported enough.
    while True:
      with worker.lock:
        if worker.functions.has_key(function_id.id()) and (worker.function_export_counters[function_id.id()] <= worker.worker_import_counter):
          break
      time.sleep(0.001)
    # Execute the task.
    with worker.lock:
      process_task(task)

def _submit_task(function_id, func_name, args, worker=global_worker):
  """This is a wrapper around worker.submit_task.

  We use this wrapper so that in the remote decorator, we can call _submit_task
  instead of worker.submit_task. The difference is that when we attempt to
  serialize remote functions, we don't attempt to serialize the worker object,
  which cannot be serialized.
  """
  return worker.submit_task(function_id, func_name, args)

def _mode(worker=global_worker):
  """This is a wrapper around worker.mode.

  We use this wrapper so that in the remote decorator, we can call _mode()
  instead of worker.mode. The difference is that when we attempt to serialize
  remote functions, we don't attempt to serialize the worker object, which
  cannot be serialized.
  """
  return worker.mode

def _reusables():
  """Return the reusables object.

  We use this wrapper because so that functions which use the reusables variable
  can be pickled.
  """
  return reusables

def _export_reusable_variable(name, reusable, worker=global_worker):
  """Export a reusable variable to the workers. This is only called by a driver.

  Args:
    name (str): The name of the variable to export.
    reusable (Reusable): The reusable object containing code for initializing
      and reinitializing the variable.
  """
  if _mode(worker) not in [SCRIPT_MODE, SILENT_MODE]:
    raise Exception("_export_reusable_variable can only be called on a driver.")
  reusable_variable_id = name
  key = "ReusableVariables:{}".format(reusable_variable_id)
  worker.redis_client.hmset(key, {"name": name,
                                  "initializer": pickling.dumps(reusable.initializer),
                                  "reinitializer": pickling.dumps(reusable.reinitializer)})
  worker.redis_client.rpush("Exports", key)
  worker.driver_export_counter += 1

def export_remote_function(function_id, func_name, func, num_return_vals, worker=global_worker):
  if _mode(worker) not in [SCRIPT_MODE, SILENT_MODE]:
    raise Exception("export_remote_function can only be called on a driver.")
  key = "RemoteFunction:{}".format(function_id.id())
  worker.num_return_vals[function_id.id()] = num_return_vals
  pickled_func = pickling.dumps(func)
  worker.redis_client.hmset(key, {"function_id": function_id.id(),
                                  "name": func_name,
                                  "module": func.__module__,
                                  "function": pickled_func,
                                  "num_return_vals": num_return_vals,
                                  "function_export_counter": worker.driver_export_counter})
  worker.redis_client.rpush("Exports", key)
  worker.driver_export_counter += 1

def remote(*args, **kwargs):
  """This decorator is used to create remote functions.

  Args:
    num_return_vals (int): The number of object IDs that a call to this function
      should return.
  """
  worker = global_worker
  def make_remote_decorator(num_return_vals, func_id=None):
    def remote_decorator(func):
      func_name = "{}.{}".format(func.__module__, func.__name__)
      if func_id is None:
        function_id = FunctionID((hashlib.sha256(func_name).digest())[:20])
      else:
        function_id = func_id

      def func_call(*args, **kwargs):
        """This gets run immediately when a worker calls a remote function."""
        check_connected()
        args = list(args)
        args.extend([kwargs[keyword] if kwargs.has_key(keyword) else default for keyword, default in keyword_defaults[len(args):]]) # fill in the remaining arguments
        if any([arg is funcsigs._empty for arg in args]):
          raise Exception("Not enough arguments were provided to {}.".format(func_name))
        if _mode() == PYTHON_MODE:
          # In PYTHON_MODE, remote calls simply execute the function. We copy the
          # arguments to prevent the function call from mutating them and to match
          # the usual behavior of immutable remote objects.
          try:
            _reusables()._running_remote_function_locally = True
            result = func(*copy.deepcopy(args))
          finally:
            _reusables()._reinitialize()
            _reusables()._running_remote_function_locally = False
          return result
        objectids = _submit_task(function_id, func_name, args)
        if len(objectids) == 1:
          return objectids[0]
        elif len(objectids) > 1:
          return objectids
      def func_executor(arguments):
        """This gets run when the remote function is executed."""
        start_time = time.time()
        result = func(*arguments)
        end_time = time.time()
        return result
      def func_invoker(*args, **kwargs):
        """This is returned by the decorator and used to invoke the function."""
        raise Exception("Remote functions cannot be called directly. Instead of running '{}()', try '{}.remote()'.".format(func_name, func_name))
      func_invoker.remote = func_call
      func_invoker.executor = func_executor
      func_invoker.is_remote = True
      func_name = "{}.{}".format(func.__module__, func.__name__)
      func_invoker.func_name = func_name
      func_invoker.func_doc = func.func_doc

      sig_params = [(k, v) for k, v in funcsigs.signature(func).parameters.iteritems()]
      keyword_defaults = [(k, v.default) for k, v in sig_params]
      has_vararg_param = any([v.kind == v.VAR_POSITIONAL for k, v in sig_params])
      func_invoker.has_vararg_param = has_vararg_param
      has_kwargs_param = any([v.kind == v.VAR_KEYWORD for k, v in sig_params])
      check_signature_supported(has_kwargs_param, has_vararg_param, keyword_defaults, func_name)

      # Everything ready - export the function
      if worker.mode in [None, SCRIPT_MODE, SILENT_MODE]:
        func_name_global_valid = func.__name__ in func.__globals__
        func_name_global_value = func.__globals__.get(func.__name__)
        # Set the function globally to make it refer to itself
        func.__globals__[func.__name__] = func_invoker  # Allow the function to reference itself as a global variable
        try:
          to_export = pickling.dumps((func, num_return_vals, func.__module__))
        finally:
          # Undo our changes
          if func_name_global_valid: func.__globals__[func.__name__] = func_name_global_value
          else: del func.__globals__[func.__name__]
      if worker.mode in [SCRIPT_MODE, SILENT_MODE]:
        export_remote_function(function_id, func_name, func, num_return_vals)
      elif worker.mode is None:
        worker.cached_remote_functions.append((function_id, func_name, func, num_return_vals))
      return func_invoker

    return remote_decorator

  if _mode() == WORKER_MODE:
    if kwargs.has_key("function_id"):
      num_return_vals = kwargs["num_return_vals"]
      function_id = kwargs["function_id"]
      return make_remote_decorator(num_return_vals, function_id)

  if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
    # This is the case where the decorator is just @ray.remote.
    num_return_vals = 1
    func = args[0]
    return make_remote_decorator(num_return_vals)(func)
  else:
    # This is the case where the decorator is something like
    # @ray.remote(num_return_vals=2).
    assert len(args) == 0 and kwargs.has_key("num_return_vals"), "The @ray.remote decorator must be applied either with no arguments and no parentheses, for example '@ray.remote', or it must be applied with only the argument num_return_vals, like '@ray.remote(num_return_vals=2)'."
    num_return_vals = kwargs["num_return_vals"]
    assert not kwargs.has_key("function_id")
    return make_remote_decorator(num_return_vals)

def check_signature_supported(has_kwargs_param, has_vararg_param, keyword_defaults, name):
  """Check if we support the signature of this function.

  We currently do not allow remote functions to have **kwargs. We also do not
  support keyword argumens in conjunction with a *args argument.

  Args:
    has_kwards_param (bool): True if the function being checked has a **kwargs
      argument.
    has_vararg_param (bool): True if the function being checked has a *args
      argument.
    keyword_defaults (List): A list of the default values for the arguments to
      the function being checked.
    name (str): The name of the function to check.

  Raises:
    Exception: An exception is raised if the signature is not supported.
  """
  # check if the user specified kwargs
  if has_kwargs_param:
    raise "Function {} has a **kwargs argument, which is currently not supported.".format(name)
  # check if the user specified a variable number of arguments and any keyword arguments
  if has_vararg_param and any([d != funcsigs._empty for _, d in keyword_defaults]):
    raise "Function {} has a *args argument as well as a keyword argument, which is currently not supported.".format(name)

def get_arguments_for_execution(function, serialized_args, worker=global_worker):
  """Retrieve the arguments for the remote function.

  This retrieves the values for the arguments to the remote function that were
  passed in as object IDs. Argumens that were passed by value are not changed.
  This is called by the worker that is executing the remote function.

  Args:
    function (Callable): The remote function whose arguments are being
      retrieved.
    serialized_args (List): The arguments to the function. These are either
      strings representing serialized objects passed by value or they are
      ObjectIDs.

  Returns:
    The retrieved arguments in addition to the arguments that were passed by
    value.

  Raises:
    RayGetArgumentError: This exception is raised if a task that created one of
      the arguments failed.
  """
  arguments = []
  for (i, arg) in enumerate(serialized_args):
    if isinstance(arg, photon.ObjectID):
      # get the object from the local object store
      argument = worker.get_object(arg)
      if isinstance(argument, RayTaskError):
        # If the result is a RayTaskError, then the task that created this
        # object failed, and we should propagate the error message here.
        raise RayGetArgumentError(function.__name__, i, arg, argument)
    else:
      # pass the argument by value
      argument = arg

    arguments.append(argument)
  return arguments

def store_outputs_in_objstore(objectids, outputs, worker=global_worker):
  """Store the outputs of a remote function in the local object store.

  This stores the values that were returned by a remote function in the local
  object store. If any of the return values are object IDs, then these object
  IDs are aliased with the object IDs that the scheduler assigned for the return
  values. This is called by the worker that executes the remote function.

  Note:
    The arguments objectids and outputs should have the same length.

  Args:
    objectids (List[ObjectID]): The object IDs that were assigned to the
      outputs of the remote function call.
    outputs (Tuple): The value returned by the remote function. If the remote
      function was supposed to only return one value, then its output was
      wrapped in a tuple with one element prior to being passed into this
      function.
  """
  for i in range(len(objectids)):
    if isinstance(outputs[i], photon.ObjectID):
      raise Exception("This remote function returned an ObjectID as its {}th return value. This is not allowed.".format(i))
  for i in range(len(objectids)):
    worker.put_object(objectids[i], outputs[i])
