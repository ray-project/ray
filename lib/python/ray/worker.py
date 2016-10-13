import os
import sys
import time
import traceback
import copy
import logging
import funcsigs
import numpy as np
import colorama
import atexit
import threading
import string
import weakref

# Ray modules
import config
import pickling
import serialization
import internal.graph_pb2
import graph
import services
import libnumbuf
import libraylib as raylib

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
  return libnumbuf.serialize_list([value])

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
    self._slots = ("_names", "_reinitializers", "_running_remote_function_locally", "_reusables", "_local_mode_reusables", "_cached_reusables", "_used", "_slots", "_create_and_export", "_reinitialize", "__getattribute__", "__setattr__", "__delattr__")
    # CHECKPOINT: Attributes must not be added after _slots. The above attributes are protected from deletion.

  def _create_and_export(self, name, reusable):
    """Create a reusable variable and add export it to the workers.

    If ray.init has not been called yet, then store the reusable variable and
    export it later then connect is called.

    Args:
      name (str): The name of the reusable variable.
      reusable (Reusable): The reusable object to use to create the reusable
        variable.
    """
    self._names.add(name)
    self._reinitializers[name] = reusable.reinitializer
    # Export the reusable variable to the workers if we are on the driver. If
    # ray.init has not been called yet, then cache the reusable variable to
    # export later.
    if _mode() in [raylib.SCRIPT_MODE, raylib.SILENT_MODE]:
      _export_reusable_variable(name, reusable)
    elif _mode() is None:
      self._cached_reusables.append((name, reusable))
    self._reusables[name] = reusable.initializer()
    # We create a second copy of the reusable variable on the driver to use
    # inside of remote functions that run locally. This occurs when we start Ray
    # in PYTHON_MODE and when  we call a remote function locally.
    if _mode() in [raylib.SCRIPT_MODE, raylib.SILENT_MODE, raylib.PYTHON_MODE]:
      self._local_mode_reusables[name] = reusable.initializer()

  def _reinitialize(self):
    """Reinitialize the reusable variables that the current task used."""
    for name in self._used:
      current_value = self._reusables[name]
      new_value = self._reinitializers[name](current_value)
      # If we are on the driver, reset the copy of the reusable variable in the
      # _local_mode_reusables dictionary.
      if _mode() in [raylib.SCRIPT_MODE, raylib.SILENT_MODE, raylib.PYTHON_MODE]:
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
    # Create the reusable variable locally, and export it if possible.
    self._create_and_export(name, reusable)
    # Create an empty attribute with the name of the reusable variable. This
    # allows the Python interpreter to do tab complete properly.
    return object.__setattr__(self, name, None)

  def __delattr__(self, name):
    """We do not allow attributes of RayReusables to be deleted.

    Args:
      name (str): The name of the attribute to delete.
    """
    raise Exception("Attempted deletion of attribute {}. Attributes of a RayReusable object may not be deleted.".format(name))

class ObjectFixture(object):
  """This is used to handle unmapping objects backed by the object store.

  The object referred to by objectid will get unmaped when the fixture is
  deallocated. In addition, the ObjectFixture holds the objectid as a field,
  which ensures that the corresponding object will not be deallocated from the
  object store while the ObjectFixture is alive. ObjectFixture is used as the
  base object for numpy arrays that are contained in the object referred to by
  objectid and prevents memory that is used by them from getting unmapped by the
  worker or deallocated by the object store.
  """

  def __init__(self, objectid, segmentid, handle):
    """Initialize an ObjectFixture object."""
    self.objectid = objectid
    self.segmentid = segmentid
    self.handle = handle

  def __del__(self):
    """Unmap the segment when the object goes out of scope."""
    # We probably shouldn't have this if statement, but if raylib gets set to
    # None before this __del__ call happens, then an exception will be thrown
    # at exit.
    if raylib is not None:
      raylib.unmap_object(self.handle, self.segmentid)

class Worker(object):
  """A class used to define the control flow of a worker process.

  Note:
    The methods in this class are considered unexposed to the user. The
    functions outside of this class are considered exposed.

  Attributes:
    functions (Dict[str, Callable]): A dictionary mapping the name of a remote
      function to the remote function itself. This is the set of remote
      functions that can be executed by this worker.
    handle (worker capsule): A Python object wrapping a C++ Worker object.
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
  """

  def __init__(self):
    """Initialize a Worker object."""
    self.functions = {}
    self.handle = None
    self.mode = None
    self.cached_remote_functions = []
    self.cached_functions_to_run = []

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
      objectid (raylib.ObjectID): The object ID of the value to be put.
      value (serializable object): The value to put in the object store.
    """
    # We put the value into a list here because in arrow the concept of
    # "serializing a single object" does not exits.
    schema, size, serialized = numbuf_serialize(value)
    global contained_objectids
    raylib.add_contained_objectids(self.handle, objectid, contained_objectids)
    contained_objectids = []
    # TODO(pcm): Right now, metadata is serialized twice, change that in the future
    # in the following line, the "8" is for storing the metadata size,
    # the len(schema) is for storing the metadata and the 8192 is for storing
    # the metadata in the batch (see INITIAL_METADATA_SIZE in arrow)
    size = size + 8 + len(schema) + 4096 * 4
    buff, segmentid = raylib.allocate_buffer(self.handle, objectid, size)
    # write the metadata length
    np.frombuffer(buff, dtype="int64", count=1)[0] = len(schema)
    # metadata buffer
    metadata = np.frombuffer(buff, dtype="byte", offset=8, count=len(schema))
    # write the metadata
    metadata[:] = schema
    data = np.frombuffer(buff, dtype="byte")[8 + len(schema):]
    metadata_offset = libnumbuf.write_to_buffer(serialized, memoryview(data))
    raylib.finish_buffer(self.handle, objectid, segmentid, metadata_offset)

  def get_object(self, objectid):
    """Get the value in the local object store associated with objectid.

    Return the value from the local object store for objectid. This will block
    until the value for objectid has been written to the local object store.

    Args:
      objectid (raylib.ObjectID): The object ID of the value to retrieve.
    """
    assert raylib.is_arrow(self.handle, objectid), "All objects should be serialized using Arrow."
    buff, segmentid, metadata_offset = raylib.get_buffer(self.handle, objectid)
    metadata_size = int(np.frombuffer(buff, dtype="int64", count=1)[0])
    metadata = np.frombuffer(buff, dtype="byte", offset=8, count=metadata_size)
    data = np.frombuffer(buff, dtype="byte")[8 + metadata_size:]
    serialized = libnumbuf.read_from_buffer(memoryview(data), bytearray(metadata), metadata_offset)
    # If there is currently no ObjectFixture for this ObjectID, then create a
    # new one. The object_fixtures object is a WeakValueDictionary, so entries
    # will be discarded when there are no strong references to their values.
    # We create object_fixture outside of the assignment because if we created
    # it inside the assignement it would immediately go out of scope.
    object_fixture = None
    if objectid.id not in object_fixtures:
      object_fixture = ObjectFixture(objectid, segmentid, self.handle)
      object_fixtures[objectid.id] = object_fixture
    deserialized = libnumbuf.deserialize_list(serialized, object_fixtures[objectid.id])
    # Unwrap the object from the list (it was wrapped put_object)
    assert len(deserialized) == 1
    result = deserialized[0]
    return result

  def alias_objectids(self, alias_objectid, target_objectid):
    """Make two object IDs refer to the same object."""
    raylib.alias_objectids(self.handle, alias_objectid, target_objectid)

  def submit_task(self, func_name, args):
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
    # Convert all of the argumens to object IDs. It is a little strange that we
    # are calling put, which is external to this class.
    serialized_args = []
    for arg in args:
      if isinstance(arg, raylib.ObjectID):
        next_arg = arg
      else:
        serialized_arg = serialization.serialize_argument_if_possible(arg)
        if serialized_arg is not None:
          # Serialize the argument and pass it by value.
          next_arg = serialized_arg
        else:
          # Put the objet in the object store under the hood.
          next_arg = put(arg)
      serialized_args.append(next_arg)
    task_capsule = raylib.serialize_task(self.handle, func_name, serialized_args)
    objectids = raylib.submit_task(self.handle, task_capsule)
    return objectids

  def export_function_to_run_on_all_workers(self, function):
    """Export this function and run it on all workers.

    Args:
      function (Callable): The function to run on all of the workers. It should
        not take any arguments. If it returns anything, its return values will
        not be used.
    """
    if self.mode not in [raylib.SCRIPT_MODE, raylib.SILENT_MODE, raylib.PYTHON_MODE]:
      raise Exception("run_function_on_all_workers can only be called on a driver.")
    # Run the function on all of the workers.
    if self.mode in [raylib.SCRIPT_MODE, raylib.SILENT_MODE]:
      raylib.run_function_on_all_workers(self.handle, pickling.dumps(function))


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
    if self.mode not in [None, raylib.SCRIPT_MODE, raylib.SILENT_MODE, raylib.PYTHON_MODE]:
      raise Exception("run_function_on_all_workers can only be called on a driver.")
    # First run the function on the driver.
    function(self)
    # If ray.init has not been called yet, then cache the function and export it
    # when connect is called. Otherwise, run the function on all workers.
    if self.mode is None:
      self.cached_functions_to_run.append(function)
    else:
      self.export_function_to_run_on_all_workers(function)

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

logger = logging.getLogger("ray")
"""Logger: The logging object for the Python worker code."""

object_fixtures = weakref.WeakValueDictionary()
"""WeakValueDictionary: The mapping from ObjectID to ObjectFixture object.

This is to ensure that we have only one ObjectFixture per ObjectID. That way, if
we call get on an object twice, we do not unmap the segment before both of the
results go out of scope. It is a WeakValueDictionary instead of a regular
dictionary so that it does not keep the ObjectFixtures in scope forever.
"""

class RayConnectionError(Exception):
  pass

def check_connected(worker=global_worker):
  """Check if the worker is connected.

  Raises:
    Exception: An exception is raised if the worker is not connected.
  """
  if worker.handle is None and worker.mode != raylib.PYTHON_MODE:
    raise RayConnectionError("This command cannot be called before a Ray cluster has been started. You can start one with 'ray.init(start_ray_local=True, num_workers=1)'.")

def print_failed_task(task_status):
  """Print information about failed tasks.

  Args:
    task_status (Dict): A dictionary containing the name, operationid, and
      error message for a failed task.
  """
  print """
    Error: Task failed
      Function Name: {}
      Task ID: {}
      Error Message: \n{}
  """.format(task_status["function_name"], task_status["operationid"], task_status["error_message"])

def scheduler_info(worker=global_worker):
  """Return information about the state of the scheduler."""
  check_connected(worker)
  return raylib.scheduler_info(worker.handle)

def visualize_computation_graph(file_path=None, view=False, worker=global_worker):
  """Write the computation graph to a pdf file.

  Args:
    file_path (str): The name of a pdf file that the rendered computation graph
      will be written to. If this argument is None, a temporary path will be
      used.
    view (bool): If true, the result the python graphviz package will try to
      open the result in a viewer.

  Examples:
    Try the following code.

    >>> import ray.array.distributed as da
    >>> x = da.zeros([20, 20])
    >>> y = da.zeros([20, 20])
    >>> z = da.dot(x, y)
    >>> ray.visualize_computation_graph(view=True)
  """
  check_connected(worker)
  if file_path is None:
    file_path = config.get_log_file_path("computation-graph.pdf")

  base_path, extension = os.path.splitext(file_path)
  if extension != ".pdf":
    raise Exception("File path must be a .pdf file")
  proto_path = base_path + ".binaryproto"

  raylib.dump_computation_graph(worker.handle, proto_path)
  g = internal.graph_pb2.CompGraph()
  g.ParseFromString(open(proto_path).read())
  graph.graph_to_graphviz(g).render(base_path, view=view)

  print "Wrote graph dot description to file {}".format(base_path)
  print "Wrote graph protocol buffer description to file {}".format(proto_path)
  print "Wrote computation graph to file {}.pdf".format(base_path)

def task_info(worker=global_worker):
  """Return information about failed tasks."""
  check_connected(worker)
  return raylib.task_info(worker.handle)

def initialize_numbuf(worker=global_worker):
  """Initialize the serialization library.

  This defines a custom serializer for object IDs and also tells numbuf to
  serialize several exception classes that we define for error handling.
  """
  # Define a custom serializer and deserializer for handling Object IDs.
  def objectid_custom_serializer(obj):
    class_identifier = serialization.class_identifier(type(obj))
    contained_objectids.append(obj)
    return raylib.serialize_objectid(worker.handle, obj)
  def objectid_custom_deserializer(serialized_obj):
    return raylib.deserialize_objectid(worker.handle, serialized_obj)
  serialization.add_class_to_whitelist(raylib.ObjectID, pickle=False, custom_serializer=objectid_custom_serializer, custom_deserializer=objectid_custom_deserializer)

  if worker.mode in [raylib.SCRIPT_MODE, raylib.SILENT_MODE]:
    # These should only be called on the driver because register_class will
    # export the class to all of the workers.
    register_class(RayTaskError)
    register_class(RayGetError)
    register_class(RayGetArgumentError)

def init(start_ray_local=False, num_workers=None, num_objstores=None, scheduler_address=None, node_ip_address=None, driver_mode=raylib.SCRIPT_MODE):
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
    num_objstores (Optional[int]): The number of object stores to start if
      start_ray_local is True.
    scheduler_address (Optional[str]): The address of the scheduler to connect
      to if start_ray_local is False.
    node_ip_address (Optional[str]): The address of the node the worker is
      running on. It is required if start_ray_local is False and it cannot be
      provided otherwise.
    driver_mode (Optional[bool]): The mode in which to start the driver. This
      should be one of SCRIPT_MODE, PYTHON_MODE, and SILENT_MODE.

  Returns:
    A string containing the address of the scheduler.

  Raises:
    Exception: An exception is raised if an inappropriate combination of
      arguments is passed in.
  """
  # Make GRPC only print error messages.
  os.environ["GRPC_VERBOSITY"] = "ERROR"
  if driver_mode == raylib.PYTHON_MODE:
    # If starting Ray in PYTHON_MODE, don't start any other processes.
    pass
  elif start_ray_local:
    # In this case, we launch a scheduler, a new object store, and some workers,
    # and we connect to them.
    if (scheduler_address is not None) or (node_ip_address is not None):
      raise Exception("If start_ray_local=True, then you cannot pass in a scheduler_address or a node_ip_address.")
    if driver_mode not in [raylib.SCRIPT_MODE, raylib.PYTHON_MODE, raylib.SILENT_MODE]:
      raise Exception("If start_ray_local=True, then driver_mode must be in [ray.SCRIPT_MODE, ray.PYTHON_MODE, ray.SILENT_MODE].")
    # Use the address 127.0.0.1 in local mode.
    node_ip_address = "127.0.0.1"
    num_workers = 1 if num_workers is None else num_workers
    num_objstores = 1 if num_objstores is None else num_objstores
    # Start the scheduler, object store, and some workers. These will be killed
    # by the call to cleanup(), which happens when the Python script exits.
    scheduler_address = services.start_ray_local(num_objstores=num_objstores, num_workers=num_workers, worker_path=None)
  else:
    # In this case, there is an existing scheduler and object store, and we do
    # not need to start any processes.
    if (num_workers is not None) or (num_objstores is not None):
      raise Exception("The arguments num_workers and num_objstores must not be provided unless start_ray_local=True.")
    if (node_ip_address is None) or (scheduler_address is None):
      raise Exception("When start_ray_local=False, node_ip_address and scheduler_address must be provided.")
  # Connect this driver to the scheduler and object store. The corresponing call
  # to disconnect will happen in the call to cleanup() when the Python script
  # exits.
  connect(node_ip_address, scheduler_address, worker=global_worker, mode=driver_mode)
  return scheduler_address

def cleanup(worker=global_worker):
  """Disconnect the driver, and terminate any processes started in init.

  This will automatically run at the end when a Python process that uses Ray
  exits. It is ok to run this twice in a row. Note that we manually call
  services.cleanup() in the tests because we need to start and stop many
  clusters in the tests, but the import and exit only happen once.
  """
  disconnect(worker)
  worker.set_mode(None)
  services.cleanup()

atexit.register(cleanup)

def print_error_messages(worker=global_worker):
  num_failed_tasks = 0
  num_failed_remote_function_imports = 0
  num_failed_reusable_variable_imports = 0
  num_failed_reusable_variable_reinitializations = 0
  num_failed_function_to_runs = 0
  while True:
    try:
      info = task_info(worker=worker)
      # Print failed task errors.
      for error in info["failed_tasks"][num_failed_tasks:]:
        print error["error_message"]
      num_failed_tasks = len(info["failed_tasks"])
      # Print remote function import errors.
      for error in info["failed_remote_function_imports"][num_failed_remote_function_imports:]:
        print error["error_message"]
      num_failed_remote_function_imports = len(info["failed_remote_function_imports"])
      # Print reusable variable import errors.
      for error in info["failed_reusable_variable_imports"][num_failed_reusable_variable_imports:]:
        print error["error_message"]
      num_failed_reusable_variable_imports = len(info["failed_reusable_variable_imports"])
      # Print reusable variable reinitialization errors.
      for error in info["failed_reinitialize_reusable_variables"][num_failed_reusable_variable_reinitializations:]:
        print error["error_message"]
      num_failed_reusable_variable_reinitializations = len(info["failed_reinitialize_reusable_variables"])
      for error in info["failed_function_to_runs"][num_failed_function_to_runs:]:
        print error["error_message"]
      num_failed_function_to_runs = len(info["failed_function_to_runs"])
      time.sleep(0.2)
    except:
      # When the driver is exiting, we set worker.handle to None, which will
      # cause the check_connected call inside of task_info to raise an
      # exception. We use the try block here to suppress that exception. In
      # addition, when the script exits, the different names get set to None,
      # for example, the time module and the task_info method get set to None,
      # and so a TypeError will be thrown when we attempt to call time.sleep or
      # task_info.
      pass

def connect(node_ip_address, scheduler_address, objstore_address=None, worker=global_worker, mode=raylib.WORKER_MODE):
  """Connect this worker to the scheduler and an object store.

  Args:
    node_ip_address (str): The ip address of the node the worker runs on.
    scheduler_address (str): The ip address and port of the scheduler.
    objstore_address (Optional[str]): The ip address and port of the local
      object store. Normally, this argument should be omitted and the scheduler
      will tell the worker what object store to connect to.
    mode: The mode of the worker. One of SCRIPT_MODE, WORKER_MODE, PYTHON_MODE,
      and SILENT_MODE.
  """
  assert worker.handle is None, "When connect is called, worker.handle should be None."
  # If running Ray in PYTHON_MODE, there is no need to create call create_worker
  # or to start the worker service.
  if mode == raylib.PYTHON_MODE:
    worker.mode = raylib.PYTHON_MODE
    return

  worker.scheduler_address = scheduler_address
  random_string = "".join(np.random.choice(list(string.ascii_uppercase + string.digits)) for _ in range(10))
  cpp_log_file_name = config.get_log_file_path("-".join(["worker", random_string, "c++"]) + ".log")
  python_log_file_name = config.get_log_file_path("-".join(["worker", random_string]) + ".log")
  # Create a worker object. This also creates the worker service, which can
  # receive commands from the scheduler. This call also sets up a queue between
  # the worker and the worker service.
  worker.handle, worker.worker_address = raylib.create_worker(node_ip_address, scheduler_address, objstore_address if objstore_address is not None else "", mode, cpp_log_file_name)
  # If this is a driver running in SCRIPT_MODE, start a thread to print error
  # messages asynchronously in the background. Ideally the scheduler would push
  # messages to the driver's worker service, but we ran into bugs when trying to
  # properly shutdown the driver's worker service, so we are temporarily using
  # this implementation which constantly queries the scheduler for new error
  # messages.
  if mode == raylib.SCRIPT_MODE:
    t = threading.Thread(target=print_error_messages, args=(worker,))
    # Making the thread a daemon causes it to exit when the main thread exits.
    t.daemon = True
    t.start()
  worker.set_mode(mode)
  FORMAT = "%(asctime)-15s %(message)s"
  # Configure the Python logging module. Note that if we do not provide our own
  # logger, then our logging will interfere with other Python modules that also
  # use the logging module.
  log_handler = logging.FileHandler(python_log_file_name)
  log_handler.setLevel(logging.DEBUG)
  log_handler.setFormatter(logging.Formatter(FORMAT))
  _logger().addHandler(log_handler)
  _logger().setLevel(logging.DEBUG)
  _logger().propagate = False
  if mode in [raylib.SCRIPT_MODE, raylib.SILENT_MODE, raylib.PYTHON_MODE]:
    # Add the directory containing the script that is running to the Python
    # paths of the workers. Also add the current directory. Note that this
    # assumes that the directory structures on the machines in the clusters are
    # the same.
    script_directory = os.path.abspath(os.path.dirname(sys.argv[0]))
    current_directory = os.path.abspath(os.path.curdir)
    worker.run_function_on_all_workers(lambda worker: sys.path.insert(1, script_directory))
    worker.run_function_on_all_workers(lambda worker: sys.path.insert(1, current_directory))
    # Export cached functions_to_run.
    for function in worker.cached_functions_to_run:
      worker.export_function_to_run_on_all_workers(function)
    # Export cached remote functions to the workers.
    for function_name, function_to_export in worker.cached_remote_functions:
      raylib.export_remote_function(worker.handle, function_name, function_to_export)
    # Export the cached reusable variables.
    for name, reusable_variable in reusables._cached_reusables:
      _export_reusable_variable(name, reusable_variable)
  # Initialize the serialization library.
  initialize_numbuf()
  worker.cached_functions_to_run = None
  worker.cached_remote_functions = None
  reusables._cached_reusables = None

def disconnect(worker=global_worker):
  """Disconnect this worker from the scheduler and object store."""
  if worker.handle is not None:
    raylib.disconnect(worker.handle)
    worker.handle = None
  # Reset the list of cached remote functions so that if more remote functions
  # are defined and then connect is called again, the remote functions will be
  # exported. This is mostly relevant for the tests.
  worker.cached_functions_to_run = []
  worker.cached_remote_functions = []
  reusables._cached_reusables = []

def register_class(cls, pickle=False, worker=global_worker):
  """Enable workers to serialize or deserialize objects of a particular class.

  This method runs the register_class function defined below on every worker,
  which will enable libnumbuf to properly serialize and deserialize objects of
  this class.

  Args:
    cls (type): The class that libnumbuf should serialize.
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
  if worker.mode == raylib.WORKER_MODE:
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
  if worker.mode == raylib.PYTHON_MODE:
    return objectid # In raylib.PYTHON_MODE, ray.get is the identity operation (the input will actually be a value not an objectid)
  if isinstance(objectid, list):
    [raylib.request_object(worker.handle, x) for x in objectid]
    values = [worker.get_object(x) for x in objectid]
    for i, value in enumerate(values):
      if isinstance(value, RayTaskError):
        raise RayGetError(objectid[i], value)
    return values
  raylib.request_object(worker.handle, objectid)
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
  if worker.mode == raylib.PYTHON_MODE:
    return value # In raylib.PYTHON_MODE, ray.put is the identity operation
  objectid = raylib.get_objectid(worker.handle)
  worker.put_object(objectid, value)
  return objectid

def wait(objectids, num_returns=1, timeout=None, worker=global_worker):
  """Return a list of IDs that are ready and a list of IDs that are not ready.

  If timeout is set, the function returns either when the requested number of
  IDs are ready or when the timeout is reached, whichever occurs first. If it is
  not set, the function simply waits until that number of objects is ready and
  returns that exact number of objectids.

  This method returns two lists. The first list consists of object IDs that
  correspond to objects that are stored in the object store. The second list
  corresponds to the rest of the object IDs (which may or may not be ready).

  Args:
    objectids (List[raylib.ObjectID]): List of object IDs for objects that may
      or may not be ready.
    num_returns (int): The number of object IDs that should be returned.
    timeout (float): The maximum amount of time in seconds that should be spent
      polling the scheduler.

  Returns:
    A list of object IDs that are ready and a list of the remaining object IDs.
  """
  check_connected(worker)
  if num_returns < 0:
    raise Exception("num_returns cannot be less than 0.")
  if num_returns > len(objectids):
    raise Exception("num_returns cannot be greater than the length of the input list: num_objects is {}, and the length is {}.".format(num_returns, len(objectids)))
  start_time = time.time()
  ready_indices = raylib.wait(worker.handle, objectids)
  # Polls scheduler until enough objects are ready.
  while len(ready_indices) < num_returns and (time.time() - start_time < timeout or timeout is None):
    ready_indices = raylib.wait(worker.handle, objectids)
    time.sleep(0.1)
  # Return indices for exactly the requested number of objects.
  ready_ids = [objectids[i] for i in ready_indices[:num_returns]]
  not_ready_ids = [objectids[i] for i in range(len(objectids)) if i not in ready_indices[:num_returns]]
  return ready_ids, not_ready_ids

def kill_workers(worker=global_worker):
  """Kill all of the workers in the cluster. This does not kill drivers.

  Note:
    Currently, we only support killing workers if all submitted tasks have been
    run. If some workers are still running tasks or if the scheduler still has
    tasks in its queue, then this method will not do anything.

  Returns:
    True if workers were successfully killed. False otherwise.
  """
  success = raylib.kill_workers(worker.handle)
  if not success:
    print "Could not kill all workers. We currently do not support killing workers when tasks are running."
  return success

def restart_workers_local(num_workers, worker_path, worker=global_worker):
  """Restart workers locally.

  This method kills all of the workers and starts new workers locally on the
  same node as the driver. This is intended for use in the case where Ray is
  being used on a single node.

  Args:
    num_workers (int): The number of workers to be started.
    worker_path (str): The path of the source code that workers will run.

  Returns:
    True if workers were successfully restarted. False otherwise.
  """
  if not kill_workers(worker):
    return False
  services.start_workers(worker.scheduler_address, worker.objstore_address, num_workers, worker_path)
  return True

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
  if not raylib.connected(worker.handle):
    raise Exception("Worker is attempting to enter main_loop but has not been connected yet.")
  # Notify the scheduler that the worker is ready to start receiving tasks.
  raylib.ready_for_new_task(worker.handle)

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
    function_name, serialized_args, return_objectids = task
    try:
      arguments = get_arguments_for_execution(worker.functions[function_name], serialized_args, worker) # get args from objstore
      outputs = worker.functions[function_name].executor(arguments) # execute the function
      if len(return_objectids) == 1:
        outputs = (outputs,)
      store_outputs_in_objstore(return_objectids, outputs, worker) # store output in local object store
    except Exception as e:
      # If the task threw an exception, then record the traceback. We determine
      # whether the exception was thrown in the task execution by whether the
      # variable "arguments" is defined.
      traceback_str = format_error_message(traceback.format_exc()) if "arguments" in locals() else None
      failure_object = RayTaskError(function_name, e, traceback_str)
      failure_objects = [failure_object for _ in range(len(return_objectids))]
      store_outputs_in_objstore(return_objectids, failure_objects, worker)
      # Notify the scheduler that the task failed.
      raylib.notify_failure(worker.handle, function_name, str(failure_object), raylib.FailedTask)
      _logger().info("While running function {}, worker threw exception with message: \n\n{}\n".format(function_name, str(failure_object)))
    # Notify the scheduler that the task is done. This happens regardless of
    # whether the task succeeded or failed.
    raylib.ready_for_new_task(worker.handle)
    try:
      # Reinitialize the values of reusable variables that were used in the task
      # above so that changes made to their state do not affect other tasks.
      reusables._reinitialize()
    except Exception as e:
      # The attempt to reinitialize the reusable variables threw an exception.
      # We record the traceback and notify the scheduler.
      traceback_str = format_error_message(traceback.format_exc())
      raylib.notify_failure(worker.handle, function_name, traceback_str, raylib.FailedReinitializeReusableVariable)
      _logger().info("While attempting to reinitialize the reusable variables after running function {}, the worker threw exception with message: \n\n{}\n".format(function_name, traceback_str))

  def process_remote_function(function_name, serialized_function):
    """Import a remote function."""
    try:
      function, num_return_vals, module = pickling.loads(serialized_function)
    except:
      # If an exception was thrown when the remote function was imported, we
      # record the traceback and notify the scheduler of the failure.
      traceback_str = format_error_message(traceback.format_exc())
      _logger().info("Failed to import remote function {}. Failed with message: \n\n{}\n".format(function_name, traceback_str))
      # Notify the scheduler that the remote function failed to import.
      raylib.notify_failure(worker.handle, function_name, traceback_str, raylib.FailedRemoteFunctionImport)
    else:
      # TODO(rkn): Why is the below line necessary?
      function.__module__ = module
      assert function_name == "{}.{}".format(function.__module__, function.__name__), "The remote function name does not match the name that was passed in."
      worker.functions[function_name] = remote(num_return_vals=num_return_vals)(function)
      _logger().info("Successfully imported remote function {}.".format(function_name))
      # Noify the scheduler that the remote function imported successfully.
      # We pass an empty error message string because the import succeeded.
      raylib.register_remote_function(worker.handle, function_name, num_return_vals)

  def process_reusable_variable(reusable_variable_name, initializer_str, reinitializer_str):
    """Import a reusable variable."""
    try:
      initializer = pickling.loads(initializer_str)
      reinitializer = pickling.loads(reinitializer_str)
      reusables.__setattr__(reusable_variable_name, Reusable(initializer, reinitializer))
    except:
      # If an exception was thrown when the reusable variable was imported, we
      # record the traceback and notify the scheduler of the failure.
      traceback_str = format_error_message(traceback.format_exc())
      _logger().info("Failed to import reusable variable {}. Failed with message: \n\n{}\n".format(reusable_variable_name, traceback_str))
      # Notify the scheduler that the reusable variable failed to import.
      raylib.notify_failure(worker.handle, reusable_variable_name, traceback_str, raylib.FailedReusableVariableImport)
    else:
      _logger().info("Successfully imported reusable variable {}.".format(reusable_variable_name))

  def process_function_to_run(serialized_function):
    """Run on arbitrary function on the worker."""
    try:
      # Deserialize the function.
      function = pickling.loads(serialized_function)
      # Run the function.
      function(worker)
    except:
      # If an exception was thrown when the function was run, we record the
      # traceback and notify the scheduler of the failure.
      traceback_str = traceback.format_exc()
      _logger().info("Failed to run function on worker. Failed with message: \n\n{}\n".format(traceback_str))
      # Notify the scheduler that running the function failed.
      name = function.__name__  if "function" in locals() and hasattr(function, "__name__") else ""
      raylib.notify_failure(worker.handle, name, traceback_str, raylib.FailedFunctionToRun)
    else:
      _logger().info("Successfully ran function on worker.")

  while True:
    command, command_args = raylib.wait_for_next_message(worker.handle)
    try:
      if command == "die":
        # We use this as a mechanism to allow the scheduler to kill workers.
        _logger().info("Received a 'die' command, and will exit now.")
        break
      elif command == "task":
        process_task(command_args)
      elif command == "function":
        function_name, serialized_function = command_args
        process_remote_function(function_name, serialized_function)
      elif command == "reusable_variable":
        name, initializer_str, reinitializer_str = command_args
        process_reusable_variable(name, initializer_str, reinitializer_str)
      elif command == "function_to_run":
        serialized_function = command_args
        process_function_to_run(serialized_function)
      else:
        _logger().info("Reached the end of the if-else loop in the main loop. This should be unreachable.")
        assert False, "This code should be unreachable."
    finally:
      # Allow releasing the variables BEFORE we wait for the next message or exit the block
      del command_args

def _submit_task(func_name, args, worker=global_worker):
  """This is a wrapper around worker.submit_task.

  We use this wrapper so that in the remote decorator, we can call _submit_task
  instead of worker.submit_task. The difference is that when we attempt to
  serialize remote functions, we don't attempt to serialize the worker object,
  which cannot be serialized.
  """
  return worker.submit_task(func_name, args)

def _mode(worker=global_worker):
  """This is a wrapper around worker.mode.

  We use this wrapper so that in the remote decorator, we can call _mode()
  instead of worker.mode. The difference is that when we attempt to serialize
  remote functions, we don't attempt to serialize the worker object, which
  cannot be serialized.
  """
  return worker.mode

def _logger():
  """Return the logger object.

  We use this wrapper because so that functions which do logging can be pickled.
  Normally a logger object is specific to a machine (it opens a local file), and
  so cannot be pickled.
  """
  return logger

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
  if _mode(worker) not in [raylib.SCRIPT_MODE, raylib.SILENT_MODE]:
    raise Exception("_export_reusable_variable can only be called on a driver.")
  raylib.export_reusable_variable(worker.handle, name, pickling.dumps(reusable.initializer), pickling.dumps(reusable.reinitializer))

def remote(*args, **kwargs):
  """This decorator is used to create remote functions.

  Args:
    num_return_vals (int): The number of object IDs that a call to this function
      should return.
  """
  worker = global_worker
  def make_remote_decorator(num_return_vals):
    def remote_decorator(func):
      def func_call(*args, **kwargs):
        """This gets run immediately when a worker calls a remote function."""
        check_connected()
        args = list(args)
        args.extend([kwargs[keyword] if kwargs.has_key(keyword) else default for keyword, default in keyword_defaults[len(args):]]) # fill in the remaining arguments
        if any([arg is funcsigs._empty for arg in args]):
          raise Exception("Not enough arguments were provided to {}.".format(func_name))
        if _mode() == raylib.PYTHON_MODE:
          # In raylib.PYTHON_MODE, remote calls simply execute the function. We copy the
          # arguments to prevent the function call from mutating them and to match
          # the usual behavior of immutable remote objects.
          try:
            _reusables()._running_remote_function_locally = True
            result = func(*copy.deepcopy(args))
          finally:
            _reusables()._reinitialize()
            _reusables()._running_remote_function_locally = False
          return result
        objectids = _submit_task(func_name, args)
        if len(objectids) == 1:
          return objectids[0]
        elif len(objectids) > 1:
          return objectids
      def func_executor(arguments):
        """This gets run when the remote function is executed."""
        _logger().info("Calling function {}".format(func.__name__))
        start_time = time.time()
        result = func(*arguments)
        end_time = time.time()
        _logger().info("Finished executing function {}, it took {} seconds".format(func.__name__, end_time - start_time))
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
      if worker.mode in [None, raylib.SCRIPT_MODE, raylib.SILENT_MODE]:
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
      if worker.mode in [raylib.SCRIPT_MODE, raylib.SILENT_MODE]:
        raylib.export_remote_function(worker.handle, func_name, to_export)
      elif worker.mode is None:
        worker.cached_remote_functions.append((func_name, to_export))
      return func_invoker

    return remote_decorator

  if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
    # This is the case where the decorator is just @ray.remote.
    num_return_vals = 1
    func = args[0]
    return make_remote_decorator(num_return_vals)(func)
  else:
    # This is the case where the decorator is something like
    # @ray.remote(num_return_vals=2).
    assert len(args) == 0 and "num_return_vals" in kwargs.keys(), "The @ray.remote decorator must be applied either with no arguments and no parentheses, for example '@ray.remote', or it must be applied with only the argument num_return_vals, like '@ray.remote(num_return_vals=2)'."
    num_return_vals = kwargs["num_return_vals"]
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
    if isinstance(arg, raylib.ObjectID):
      # get the object from the local object store
      _logger().info("Getting argument {} for function {}.".format(i, function.__name__))
      argument = worker.get_object(arg)
      if isinstance(argument, RayTaskError):
        # If the result is a RayTaskError, then the task that created this
        # object failed, and we should propagate the error message here.
        raise RayGetArgumentError(function.__name__, i, arg, argument)
      _logger().info("Successfully retrieved argument {} for function {}.".format(i, function.__name__))
    else:
      # pass the argument by value
      argument = serialization.deserialize_argument(arg)

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
    objectids (List[raylib.ObjectID]): The object IDs that were assigned to the
      outputs of the remote function call.
    outputs (Tuple): The value returned by the remote function. If the remote
      function was supposed to only return one value, then its output was
      wrapped in a tuple with one element prior to being passed into this
      function.
  """
  for i in range(len(objectids)):
    if isinstance(outputs[i], raylib.ObjectID):
      raise Exception("This remote function returned an ObjectID as its {}th return value. This is not allowed.".format(i))
  for i in range(len(objectids)):
    worker.put_object(objectids[i], outputs[i])
