from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import atexit
import collections
import colorama
import copy
import hashlib
import inspect
import json
import numpy as np
import os
import redis
import signal
import sys
import threading
import time
import traceback

# Ray modules
import ray.experimental.state as state
import ray.pickling as pickling
import ray.serialization as serialization
import ray.services as services
import ray.signature as signature
import ray.numbuf
import ray.local_scheduler
import ray.plasma
from ray.utils import random_string

SCRIPT_MODE = 0
WORKER_MODE = 1
PYTHON_MODE = 2
SILENT_MODE = 3

LOG_POINT = 0
LOG_SPAN_START = 1
LOG_SPAN_END = 2

ERROR_KEY_PREFIX = b"Error:"
DRIVER_ID_LENGTH = 20
ERROR_ID_LENGTH = 20

# This must match the definition of NIL_ACTOR_ID in task.h.
NIL_ID = 20 * b"\xff"
NIL_LOCAL_SCHEDULER_ID = NIL_ID
NIL_FUNCTION_ID = NIL_ID
NIL_ACTOR_ID = NIL_ID

# When performing ray.get, wait 1 second before attemping to reconstruct and
# fetch the object again.
GET_TIMEOUT_MILLISECONDS = 1000

# This must be kept in sync with the `error_types` array in
# common/state/error_table.h.
OBJECT_HASH_MISMATCH_ERROR_TYPE = b"object_hash_mismatch"
PUT_RECONSTRUCTION_ERROR_TYPE = b"put_reconstruction"

# This must be kept in sync with the `scheduling_state` enum in common/task.h.
TASK_STATUS_RUNNING = 8


class FunctionID(object):
  def __init__(self, function_id):
    self.function_id = function_id

  def id(self):
    return self.function_id


contained_objectids = []


def numbuf_serialize(value):
  """This serializes a value and tracks the object IDs inside the value.

  We also define a custom ObjectID serializer which also closes over the global
  variable contained_objectids, and whenever the custom serializer is called,
  it adds the releevant ObjectID to the list contained_objectids. The list
  contained_objectids should be reset between calls to numbuf_serialize.

  Args:
    value: A Python object that will be serialized.

  Returns:
    The serialized object.
  """
  assert len(contained_objectids) == 0, "This should be unreachable."
  return ray.numbuf.serialize_list([value])


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
    if isinstance(exception, RayGetError) or isinstance(exception,
                                                        RayGetArgumentError):
      self.exception = exception
    else:
      self.exception = None
    self.traceback_str = traceback_str

  def __str__(self):
    """Format a RayTaskError as a string."""
    if self.traceback_str is None:
      # This path is taken if getting the task arguments failed.
      return ("Remote function {}{}{} failed with:\n\n{}"
              .format(colorama.Fore.RED, self.function_name,
                      colorama.Fore.RESET, self.exception))
    else:
      # This path is taken if the task execution failed.
      return ("Remote function {}{}{} failed with:\n\n{}"
              .format(colorama.Fore.RED, self.function_name,
                      colorama.Fore.RESET, self.traceback_str))


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
    return ("Could not get objectid {}. It was created by remote function "
            "{}{}{} which failed with:\n\n{}"
            .format(self.objectid, colorama.Fore.RED,
                    self.task_error.function_name, colorama.Fore.RESET,
                    self.task_error))


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
    return ("Failed to get objectid {} as argument {} for remote function "
            "{}{}{}. It was created by remote function {}{}{} which failed "
            "with:\n{}".format(self.objectid, self.argument_index,
                               colorama.Fore.RED, self.function_name,
                               colorama.Fore.RESET, colorama.Fore.RED,
                               self.task_error.function_name,
                               colorama.Fore.RESET, self.task_error))


class EnvironmentVariable(object):
  """An Python object that can be shared between tasks.

  Attributes:
    initializer (Callable[[], object]): A function used to create and
      initialize the environment variable.
    reinitializer (Optional[Callable[[object], object]]): An optional function
      used to reinitialize the environment variable after it has been used.
      This argument can be used as an optimization if there is a fast way to
      reinitialize the state of the variable other than rerunning the
      initializer.
  """

  def __init__(self, initializer, reinitializer=None):
    """Initialize an EnvironmentVariable object."""
    if not callable(initializer):
      raise Exception("When creating an EnvironmentVariable, initializer must "
                      "be a function.")
    self.initializer = initializer
    if reinitializer is None:
      # If no reinitializer is passed in, use a wrapped version of the
      # initializer.
      def reinitializer(value):
        return initializer()
    if not callable(reinitializer):
      raise Exception("When creating an EnvironmentVariable, reinitializer "
                      "must be a function.")
    self.reinitializer = reinitializer


class RayEnvironmentVariables(object):
  """An object used to store Python variables that are shared between tasks.

  Each worker process will have a single RayEnvironmentVariables object. This
  class serves two purposes. First, some objects are not serializable, and so
  the code that creates those objects must be run on the worker that uses them.
  This class is responsible for running the code that creates those objects.
  Second, some of these objects are expensive to create, and so they should be
  shared between tasks. However, if a task mutates a variable that is shared
  between tasks, then the behavior of the overall program may be
  nondeterministic (it could depend on scheduling decisions). To fix this, if a
  task uses a one of these shared objects, then that shared object will be
  reinitialized after the task finishes. Since the initialization may be
  expensive, the user can pass in custom reinitialization code that resets the
  state of the shared variable to the way it was after initialization. If the
  reinitialization code does not do this, then the behavior of the overall
  program is undefined.

  Attributes:
    _names (List[str]): A list of the names of all the environment variables.
    _reinitializers (Dict[str, Callable]): A dictionary mapping the name of the
      environment variables to the corresponding reinitializer.
    _running_remote_function_locally (bool): A flag used to indicate if a
      remote function is running locally on the driver so that we can simulate
      the same behavior as running a remote function remotely.
    _environment_variables: A dictionary mapping the name of an environment
      variable to the value of the environment variable.
    _local_mode_environment_variables: A copy of _environment_variables used on
      the driver when running remote functions locally on the driver. This is
      needed because there are two ways in which environment variables can be
      used on the driver. The first is that the driver's copy can be
      manipulated. This copy is never reset (think of the driver as a single
      long-running task). The second way is that a remote function can be run
      locally on the driver, and this remote function needs access to a copy of
      the environment variable, and that copy must be reinitialized after use.
    _cached_environment_variables (List[Tuple[str, EnvironmentVariable]]): A
      list of pairs. The first element of each pair is the name of an
      environment variable, and the second element is the EnvironmentVariable
      object. This list is used to store environment variables that are defined
      before the driver is connected. Once the driver is connected, these
      variables will be exported.
    _used (List[str]): A list of the names of all the environment variables
      that have been accessed within the scope of the current task. This is
      reset to the empty list after each task.
  """

  def __init__(self):
    """Initialize an RayEnvironmentVariables object."""
    self._names = set()
    self._reinitializers = {}
    self._running_remote_function_locally = False
    self._environment_variables = {}
    self._local_mode_environment_variables = {}
    self._cached_environment_variables = []
    self._used = set()
    self._slots = ("_names",
                   "_reinitializers",
                   "_running_remote_function_locally",
                   "_environment_variables",
                   "_local_mode_environment_variables",
                   "_cached_environment_variables",
                   "_used",
                   "_slots",
                   "_create_environment_variable",
                   "_reinitialize",
                   "__getattribute__",
                   "__setattr__",
                   "__delattr__")
    # CHECKPOINT: Attributes must not be added after _slots. The above
    # attributes are protected from deletion.

  def _create_environment_variable(self, name, environment_variable):
    """Create an environment variable locally.

    Args:
      name (str): The name of the environment variable.
      environment_variable (EnvironmentVariable): The environment variable
        object to use to create the environment variable variable.
    """
    self._names.add(name)
    self._reinitializers[name] = environment_variable.reinitializer
    self._environment_variables[name] = environment_variable.initializer()
    # We create a second copy of the environment variable on the driver to use
    # inside of remote functions that run locally. This occurs when we start
    # Ray in PYTHON_MODE and when we call a remote function locally.
    if _mode() in [SCRIPT_MODE, SILENT_MODE, PYTHON_MODE]:
      self._local_mode_environment_variables[name] = (environment_variable
                                                      .initializer())

  def _reinitialize(self):
    """Reinitialize the environment variables that the current task used."""
    for name in self._used:
      current_value = self._environment_variables[name]
      new_value = self._reinitializers[name](current_value)
      # If we are on the driver, reset the copy of the environment variable in
      # the _local_mode_environment_variables dictionary.
      if _mode() in [SCRIPT_MODE, SILENT_MODE, PYTHON_MODE]:
        assert self._running_remote_function_locally
        self._local_mode_environment_variables[name] = new_value
      else:
        self._environment_variables[name] = new_value
    self._used.clear()  # Reset the _used list.

  def __getattribute__(self, name):
    """Get an attribute. This handles environment variables as a special case.

    When __getattribute__ is called with the name of an environment variable,
    that name is added to the list of variables that were used in the current
    task.

    Args:
      name (str): The name of the attribute to get.
    """
    if name == "_slots":
      return object.__getattribute__(self, name)
    if name in self._slots:
      return object.__getattribute__(self, name)
    # Handle various fields that are not environment variables.
    if name not in self._names:
      return object.__getattribute__(self, name)
    # Make a note of the fact that the environment variable has been used.
    if name in self._names and name not in self._used:
      self._used.add(name)
    if self._running_remote_function_locally:
      return self._local_mode_environment_variables[name]
    else:
      return self._environment_variables[name]

  def __setattr__(self, name, value):
    """Set an attribute. This handles environment variables as a special case.

    This is used to create environment variables. When it is called, it runs
    the function for initializing the variable to create the variable. If this
    is called on the driver, then the functions for initializing and
    reinitializing the variable are shipped to the workers.

    If this is called before ray.init has been run, then the environment
    variable will be cached and it will be created and exported when connect is
    called.

    Args:
      name (str): The name of the attribute to set. This is either a
        whitelisted name or it is treated as the name of an environment
        variable.
      value: If name is a whitelisted name, then value can be any value. If
        name is the name of an environment variable, then this is an
        EnvironmentVariable object.
    """
    try:
      slots = self._slots
    except AttributeError:
      slots = ()
    if slots == ():
      return object.__setattr__(self, name, value)
    if name in slots:
      return object.__setattr__(self, name, value)
    environment_variable = value
    if not issubclass(type(environment_variable), EnvironmentVariable):
      raise Exception("To set an environment variable, you must pass in an "
                      "EnvironmentVariable object")
    # If ray.init has not been called, cache the environment variable to export
    # later. Otherwise, export the environment variable to the workers and
    # define it locally.
    if _mode() is None:
      self._cached_environment_variables.append((name, environment_variable))
    else:
      # If we are on the driver, export the environment variable to all the
      # workers.
      if _mode() in [SCRIPT_MODE, SILENT_MODE]:
        _export_environment_variable(name, environment_variable)
      # Define the environment variable locally.
      self._create_environment_variable(name, environment_variable)
      # Create an empty attribute with the name of the environment variable.
      # This allows the Python interpreter to do tab complete properly.
      object.__setattr__(self, name, None)

  def __delattr__(self, name):
    """We do not allow attributes of RayEnvironmentVariables to be deleted.

    Args:
      name (str): The name of the attribute to delete.
    """
    raise Exception("Attempted deletion of attribute {}. Attributes of a "
                    "RayEnvironmentVariables object may not be deleted."
                    .format(name))


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
      functions to the scheduler. If cached_remote_functions is None, that
      means that connect has been called already.
    cached_functions_to_run (List): A list of functions to run on all of the
      workers that should be exported as soon as connect is called.
  """

  def __init__(self):
    """Initialize a Worker object."""
    # The functions field is a dictionary that maps a driver ID to a dictionary
    # of functions that have been registered for that driver (this inner
    # dictionary maps function IDs to a tuple of the function name and the
    # function itself). This should only be used on workers that execute remote
    # functions.
    self.functions = collections.defaultdict(lambda: {})
    # The function_properties field is a dictionary that maps a driver ID to a
    # dictionary of functions that have been registered for that driver (this
    # inner dictionary maps function IDs to a tuple of the number of values
    # returned by that function, the number of CPUs required by that function,
    # and the number of GPUs required by that function). This is used when
    # submitting a function (which can be done both on workers and on drivers).
    self.function_properties = collections.defaultdict(lambda: {})
    self.connected = False
    self.mode = None
    self.cached_remote_functions = []
    self.cached_functions_to_run = []
    self.fetch_and_register_actor = None
    self.make_actor = None
    self.actors = {}
    # Use a defaultdict for the actor counts. If this is accessed with a
    # missing key, the default value of 0 is returned, and that key value pair
    # is added to the dict.
    self.actor_counters = collections.defaultdict(lambda: 0)

  def set_mode(self, mode):
    """Set the mode of the worker.

    The mode SCRIPT_MODE should be used if this Worker is a driver that is
    being run as a Python script or interactively in a shell. It will print
    information about task failures.

    The mode WORKER_MODE should be used if this Worker is not a driver. It will
    not print information about tasks.

    The mode PYTHON_MODE should be used if this Worker is a driver and if you
    want to run the driver in a manner equivalent to serial Python for
    debugging purposes. It will not send remote function calls to the scheduler
    and will insead execute them in a blocking fashion.

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
      value: The value to put in the object store.
    """
    # Make sure that the value is not an object ID.
    if isinstance(value, ray.local_scheduler.ObjectID):
      raise Exception("Calling `put` on an ObjectID is not allowed "
                      "(similarly, returning an ObjectID from a remote "
                      "function is not allowed). If you really want to do "
                      "this, you can wrap the ObjectID in a list and call "
                      "`put` on it (or return it).")

    # Serialize and put the object in the object store.
    try:
      ray.numbuf.store_list(objectid.id(), self.plasma_client.conn, [value])
    except ray.numbuf.numbuf_plasma_object_exists_error as e:
      # The object already exists in the object store, so there is no need to
      # add it again. TODO(rkn): We need to compare the hashes and make sure
      # that the objects are in fact the same. We also should return an error
      # code to the caller instead of printing a message.
      print("This object already exists in the object store.")

    global contained_objectids
    # Optionally do something with the contained_objectids here.
    contained_objectids = []

  def get_object(self, object_ids):
    """Get the value or values in the object store associated with object_ids.

    Return the values from the local object store for object_ids. This will
    block until all the values for object_ids have been written to the local
    object store.

    Args:
      object_ids (List[object_id.ObjectID]): A list of the object IDs whose
        values should be retrieved.
    """
    # Make sure that the values are object IDs.
    for object_id in object_ids:
      if not isinstance(object_id, ray.local_scheduler.ObjectID):
        raise Exception("Attempting to call `get` on the value {}, which is "
                        "not an ObjectID.".format(object_id))
    # Do an initial fetch for remote objects.
    self.plasma_client.fetch([object_id.id() for object_id in object_ids])

    # Get the objects. We initially try to get the objects immediately.
    final_results = ray.numbuf.retrieve_list(
        [object_id.id() for object_id in object_ids],
        self.plasma_client.conn,
        0)
    # Construct a dictionary mapping object IDs that we haven't gotten yet to
    # their original index in the object_ids argument.
    unready_ids = dict((object_id, i) for (i, (object_id, val)) in
                       enumerate(final_results) if val is None)
    was_blocked = (len(unready_ids) > 0)
    # Try reconstructing any objects we haven't gotten yet. Try to get them
    # until GET_TIMEOUT_MILLISECONDS milliseconds passes, then repeat.
    while len(unready_ids) > 0:
      for unready_id in unready_ids:
        self.local_scheduler_client.reconstruct_object(unready_id)
      # Do another fetch for objects that aren't available locally yet, in case
      # they were evicted since the last fetch.
      self.plasma_client.fetch(list(unready_ids.keys()))
      results = ray.numbuf.retrieve_list(list(unready_ids.keys()),
                                         self.plasma_client.conn,
                                         GET_TIMEOUT_MILLISECONDS)
      # Remove any entries for objects we received during this iteration so we
      # don't retrieve the same object twice.
      for object_id, val in results:
        if val is not None:
          index = unready_ids[object_id]
          final_results[index] = (object_id, val)
          unready_ids.pop(object_id)

    # If there were objects that we weren't able to get locally, let the local
    # scheduler know that we're now unblocked.
    if was_blocked:
      self.local_scheduler_client.notify_unblocked()

    # Unwrap the object from the list (it was wrapped put_object).
    assert len(final_results) == len(object_ids)
    for i in range(len(final_results)):
      assert final_results[i][0] == object_ids[i].id()
    return [result[1][0] for result in final_results]

  def submit_task(self, function_id, func_name, args, actor_id=None):
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
    with log_span("ray:submit_task", worker=self):
      check_main_thread()
      actor_id = (ray.local_scheduler.ObjectID(NIL_ACTOR_ID)
                  if actor_id is None else actor_id)
      # Put large or complex arguments that are passed by value in the object
      # store first.
      args_for_local_scheduler = []
      for arg in args:
        if isinstance(arg, ray.local_scheduler.ObjectID):
          args_for_local_scheduler.append(arg)
        elif ray.local_scheduler.check_simple_value(arg):
          args_for_local_scheduler.append(arg)
        else:
          args_for_local_scheduler.append(put(arg))

      # Look up the various function properties.
      num_return_vals, num_cpus, num_gpus = self.function_properties[
          self.task_driver_id.id()][function_id.id()]

      # Submit the task to local scheduler.
      task = ray.local_scheduler.Task(
          self.task_driver_id,
          ray.local_scheduler.ObjectID(function_id.id()),
          args_for_local_scheduler,
          num_return_vals,
          self.current_task_id,
          self.task_index,
          actor_id, self.actor_counters[actor_id],
          [num_cpus, num_gpus])
      # Increment the worker's task index to track how many tasks have been
      # submitted by the current task so far.
      self.task_index += 1
      self.actor_counters[actor_id] += 1
      self.local_scheduler_client.submit(task)

      return task.returns()

  def run_function_on_all_workers(self, function):
    """Run arbitrary code on all of the workers.

    This function will first be run on the driver, and then it will be exported
    to all of the workers to be run. It will also be run on any new workers
    that register later. If ray.init has not been called yet, then cache the
    function and export it later.

    Args:
      function (Callable): The function to run on all of the workers. It should
        not take any arguments. If it returns anything, its return values will
        not be used.
    """
    check_main_thread()
    if self.mode not in [None, SCRIPT_MODE, SILENT_MODE, PYTHON_MODE]:
      raise Exception("run_function_on_all_workers can only be called on a "
                      "driver.")
    # If ray.init has not been called yet, then cache the function and export
    # it when connect is called. Otherwise, run the function on all workers.
    if self.mode is None:
      self.cached_functions_to_run.append(function)
    else:
      function_to_run_id = random_string()
      key = "FunctionsToRun:{}".format(function_to_run_id)
      # First run the function on the driver. Pass in the number of workers on
      # this node that have already started executing this remote function,
      # and increment that value. Subtract 1 so that the counter starts at 0.
      counter = self.redis_client.hincrby(self.node_ip_address, key, 1) - 1
      function({"counter": counter})
      # Run the function on all workers.
      self.redis_client.hmset(key, {"driver_id": self.task_driver_id.id(),
                                    "function_id": function_to_run_id,
                                    "function": pickling.dumps(function)})
      self.redis_client.rpush("Exports", key)

  def push_error_to_driver(self, driver_id, error_type, message, data=None):
    """Push an error message to the driver to be printed in the background.

    Args:
      driver_id: The ID of the driver to push the error message to.
      error_type (str): The type of the error.
      message (str): The message that will be printed in the background on the
        driver.
      data: This should be a dictionary mapping strings to strings. It will be
        serialized with json and stored in Redis.
    """
    error_key = ERROR_KEY_PREFIX + driver_id + b":" + random_string()
    data = {} if data is None else data
    self.redis_client.hmset(error_key, {"type": error_type,
                                        "message": message,
                                        "data": data})
    self.redis_client.rpush("ErrorKeys", error_key)


def get_gpu_ids():
  """Get the IDs of the GPU that are available to the worker.

  Each ID is an integer in the range [0, NUM_GPUS - 1], where NUM_GPUS is the
  number of GPUs that the node has.
  """
  return global_worker.local_scheduler_client.gpu_ids()


global_worker = Worker()
"""Worker: The global Worker object for this worker process.

We use a global Worker object to ensure that there is a single worker object
per worker process.
"""

global_state = state.GlobalState()

env = RayEnvironmentVariables()
"""RayEnvironmentVariables: The environment variables that are shared by tasks.

Each worker process has its own RayEnvironmentVariables object, and these
objects should be the same in all workers. This is used for storing variables
that are not serializable but must be used by remote tasks. In addition, it is
used to reinitialize these variables after they are used so that changes to
their state made by one task do not affect other tasks.
"""


class RayConnectionError(Exception):
  pass


def check_main_thread():
  """Check that we are currently on the main thread.

  Raises:
    Exception: An exception is raised if this is called on a thread other than
      the main thread.
  """
  if threading.current_thread().getName() != "MainThread":
    raise Exception("The Ray methods are not thread safe and must be called "
                    "from the main thread. This method was called from thread "
                    "{}.".format(threading.current_thread().getName()))


def check_connected(worker=global_worker):
  """Check if the worker is connected.

  Raises:
    Exception: An exception is raised if the worker is not connected.
  """
  if not worker.connected:
    raise RayConnectionError("This command cannot be called before Ray has "
                             "been started. You can start Ray with "
                             "'ray.init()'.")


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
  """.format(task_status["function_name"], task_status["operationid"],
             task_status["error_message"]))


def error_applies_to_driver(error_key, worker=global_worker):
  """Return True if the error is for this driver and false otherwise."""
  # TODO(rkn): Should probably check that this is only called on a driver.
  # Check that the error key is formatted as in push_error_to_driver.
  assert len(error_key) == (len(ERROR_KEY_PREFIX) + DRIVER_ID_LENGTH + 1 +
                            ERROR_ID_LENGTH), error_key
  # If the driver ID in the error message is a sequence of all zeros, then the
  # message is intended for all drivers.
  generic_driver_id = DRIVER_ID_LENGTH * b"\x00"
  driver_id = error_key[len(ERROR_KEY_PREFIX):(len(ERROR_KEY_PREFIX) +
                                               DRIVER_ID_LENGTH)]
  return (driver_id == worker.task_driver_id.id() or
          driver_id == generic_driver_id)


def error_info(worker=global_worker):
  """Return information about failed tasks."""
  check_connected(worker)
  check_main_thread()
  error_keys = worker.redis_client.lrange("ErrorKeys", 0, -1)
  errors = []
  for error_key in error_keys:
    if error_applies_to_driver(error_key, worker=worker):
      error_contents = worker.redis_client.hgetall(error_key)
      # If the error is an object hash mismatch, look up the function name for
      # the nondeterministic task. TODO(rkn): Change this so that we don't have
      # to look up additional information. Ideally all relevant information
      # would already be in error_contents.
      error_type = error_contents[b"type"]
      if error_type in [OBJECT_HASH_MISMATCH_ERROR_TYPE,
                        PUT_RECONSTRUCTION_ERROR_TYPE]:
        function_id = error_contents[b"data"]
        if function_id == NIL_FUNCTION_ID:
          function_name = b"Driver"
        else:
          function_name = worker.redis_client.hget(
              "RemoteFunction:{}:{}".format(worker.task_driver_id,
                                            function_id),
              "name")
        error_contents[b"data"] = function_name
      errors.append(error_contents)

  return errors


def initialize_numbuf(worker=global_worker):
  """Initialize the serialization library.

  This defines a custom serializer for object IDs and also tells numbuf to
  serialize several exception classes that we define for error handling.
  """
  ray.serialization.set_callbacks()

  # Define a custom serializer and deserializer for handling Object IDs.
  def objectid_custom_serializer(obj):
    contained_objectids.append(obj)
    return obj.id()

  def objectid_custom_deserializer(serialized_obj):
    return ray.local_scheduler.ObjectID(serialized_obj)
  serialization.add_class_to_whitelist(
      ray.local_scheduler.ObjectID, pickle=False,
      custom_serializer=objectid_custom_serializer,
      custom_deserializer=objectid_custom_deserializer)

  if worker.mode in [SCRIPT_MODE, SILENT_MODE]:
    # These should only be called on the driver because register_class will
    # export the class to all of the workers.
    register_class(RayTaskError)
    register_class(RayGetError)
    register_class(RayGetArgumentError)
    # Tell Ray to serialize lambdas with pickle.
    register_class(type(lambda: 0), pickle=True)
    # Tell Ray to serialize sets with pickle.
    register_class(type(set()), pickle=True)
    # Tell Ray to serialize types with pickle.
    register_class(type(int), pickle=True)


def get_address_info_from_redis_helper(redis_address, node_ip_address):
  redis_ip_address, redis_port = redis_address.split(":")
  # For this command to work, some other client (on the same machine as Redis)
  # must have run "CONFIG SET protected-mode no".
  redis_client = redis.StrictRedis(host=redis_ip_address, port=int(redis_port))
  # The client table prefix must be kept in sync with the file
  # "src/common/redis_module/ray_redis_module.cc" where it is defined.
  REDIS_CLIENT_TABLE_PREFIX = "CL:"
  client_keys = redis_client.keys("{}*".format(REDIS_CLIENT_TABLE_PREFIX))
  # Filter to live clients on the same node and do some basic checking.
  plasma_managers = []
  local_schedulers = []
  for key in client_keys:
    info = redis_client.hgetall(key)

    # Ignore clients that were deleted.
    deleted = info[b"deleted"]
    deleted = bool(int(deleted))
    if deleted:
      continue

    assert b"ray_client_id" in info
    assert b"node_ip_address" in info
    assert b"client_type" in info
    if info[b"node_ip_address"].decode("ascii") == node_ip_address:
      if info[b"client_type"].decode("ascii") == "plasma_manager":
        plasma_managers.append(info)
      elif info[b"client_type"].decode("ascii") == "local_scheduler":
        local_schedulers.append(info)
  # Make sure that we got at least one plasma manager and local scheduler.
  assert len(plasma_managers) >= 1
  assert len(local_schedulers) >= 1
  # Build the address information.
  object_store_addresses = []
  for manager in plasma_managers:
    address = manager[b"address"].decode("ascii")
    port = services.get_port(address)
    object_store_addresses.append(
        services.ObjectStoreAddress(
            name=manager[b"store_socket_name"].decode("ascii"),
            manager_name=manager[b"manager_socket_name"].decode("ascii"),
            manager_port=port))
  scheduler_names = [scheduler[b"local_scheduler_socket_name"].decode("ascii")
                     for scheduler in local_schedulers]
  client_info = {"node_ip_address": node_ip_address,
                 "redis_address": redis_address,
                 "object_store_addresses": object_store_addresses,
                 "local_scheduler_socket_names": scheduler_names,
                 }
  return client_info


def get_address_info_from_redis(redis_address, node_ip_address, num_retries=5):
  counter = 0
  while True:
    try:
      return get_address_info_from_redis_helper(redis_address, node_ip_address)
    except Exception as e:
      if counter == num_retries:
        raise
      # Some of the information may not be in Redis yet, so wait a little bit.
      print("Some processes that the driver needs to connect to have not "
            "registered with Redis, so retrying. Have you run "
            "./scripts/start_ray.sh on this node?")
      time.sleep(1)
    counter += 1


def _init(address_info=None,
          start_ray_local=False,
          object_id_seed=None,
          num_workers=None,
          num_local_schedulers=None,
          driver_mode=SCRIPT_MODE,
          redirect_output=False,
          start_workers_from_local_scheduler=True,
          num_cpus=None,
          num_gpus=None):
  """Helper method to connect to an existing Ray cluster or start a new one.

  This method handles two cases. Either a Ray cluster already exists and we
  just attach this driver to it, or we start all of the processes associated
  with a Ray cluster and attach to the newly started cluster.

  Args:
    address_info (dict): A dictionary with address information for processes in
      a partially-started Ray cluster. If start_ray_local=True, any processes
      not in this dictionary will be started. If provided, address_info will be
      modified to include processes that are newly started.
    start_ray_local (bool): If True then this will start any processes not
      already in address_info, including Redis, a global scheduler, local
      scheduler(s), object store(s), and worker(s). It will also kill these
      processes when Python exits. If False, this will attach to an existing
      Ray cluster.
    object_id_seed (int): Used to seed the deterministic generation of object
      IDs. The same value can be used across multiple runs of the same job in
      order to generate the object IDs in a consistent manner. However, the
      same ID should not be used for different jobs.
    num_workers (int): The number of workers to start. This is only provided if
      start_ray_local is True.
    num_local_schedulers (int): The number of local schedulers to start. This
      is only provided if start_ray_local is True.
    driver_mode (bool): The mode in which to start the driver. This should be
      one of ray.SCRIPT_MODE, ray.PYTHON_MODE, and ray.SILENT_MODE.
    redirect_output (bool): True if stdout and stderr for all the processes
      should be redirected to files and false otherwise.
    start_workers_from_local_scheduler (bool): If this flag is True, then start
      the initial workers from the local scheduler. Else, start them from
      Python. The latter case is for debugging purposes only.
    num_cpus: A list containing the number of CPUs the local schedulers should
      be configured with.
    num_gpus: A list containing the number of GPUs the local schedulers should
      be configured with.

  Returns:
    Address information about the started processes.

  Raises:
    Exception: An exception is raised if an inappropriate combination of
      arguments is passed in.
  """
  check_main_thread()
  if driver_mode not in [SCRIPT_MODE, PYTHON_MODE, SILENT_MODE]:
    raise Exception("Driver_mode must be in [ray.SCRIPT_MODE, "
                    "ray.PYTHON_MODE, ray.SILENT_MODE].")

  # Get addresses of existing services.
  if address_info is None:
    address_info = {}
  else:
    assert isinstance(address_info, dict)
  node_ip_address = address_info.get("node_ip_address")
  redis_address = address_info.get("redis_address")

  # Start any services that do not yet exist.
  if driver_mode == PYTHON_MODE:
    # If starting Ray in PYTHON_MODE, don't start any other processes.
    pass
  elif start_ray_local:
    # In this case, we launch a scheduler, a new object store, and some
    # workers, and we connect to them. We do not launch any processes that are
    # already registered in address_info.
    # Use the address 127.0.0.1 in local mode.
    node_ip_address = ("127.0.0.1" if node_ip_address is None
                       else node_ip_address)
    # Use 1 local scheduler if num_local_schedulers is not provided. If
    # existing local schedulers are provided, use that count as
    # num_local_schedulers.
    local_schedulers = address_info.get("local_scheduler_socket_names", [])
    if num_local_schedulers is None:
      if len(local_schedulers) > 0:
        num_local_schedulers = len(local_schedulers)
      else:
        num_local_schedulers = 1
    # Start the scheduler, object store, and some workers. These will be killed
    # by the call to cleanup(), which happens when the Python script exits.
    address_info = services.start_ray_head(
        address_info=address_info,
        node_ip_address=node_ip_address,
        num_workers=num_workers,
        num_local_schedulers=num_local_schedulers,
        redirect_output=redirect_output,
        start_workers_from_local_scheduler=start_workers_from_local_scheduler,
        num_cpus=num_cpus,
        num_gpus=num_gpus)
  else:
    if redis_address is None:
      raise Exception("If start_ray_local=False, then redis_address must be "
                      "provided.")
    if num_workers is not None:
      raise Exception("If start_ray_local=False, then num_workers must not be "
                      "provided.")
    if num_local_schedulers is not None:
      raise Exception("If start_ray_local=False, then num_local_schedulers "
                      "must not be provided.")
    if num_cpus is not None or num_gpus is not None:
      raise Exception("If start_ray_local=False, then num_cpus and num_gpus "
                      "must not be provided.")
    # Get the node IP address if one is not provided.
    if node_ip_address is None:
      node_ip_address = services.get_node_ip_address(redis_address)
    # Get the address info of the processes to connect to from Redis.
    address_info = get_address_info_from_redis(redis_address, node_ip_address)

  # Connect this driver to Redis, the object store, and the local scheduler.
  # Choose the first object store and local scheduler if there are multiple.
  # The corresponding call to disconnect will happen in the call to cleanup()
  # when the Python script exits.
  if driver_mode == PYTHON_MODE:
    driver_address_info = {}
  else:
    driver_address_info = {
        "node_ip_address": node_ip_address,
        "redis_address": address_info["redis_address"],
        "store_socket_name": address_info["object_store_addresses"][0].name,
        "manager_socket_name": (address_info["object_store_addresses"][0]
                                .manager_name),
        "local_scheduler_socket_name": (address_info
                                        ["local_scheduler_socket_names"][0])}
  connect(driver_address_info, object_id_seed=object_id_seed, mode=driver_mode,
          worker=global_worker, actor_id=NIL_ACTOR_ID)
  return address_info


def init(redis_address=None, node_ip_address=None, object_id_seed=None,
         num_workers=None, driver_mode=SCRIPT_MODE, redirect_output=False,
         num_cpus=None, num_gpus=None):
  """Either connect to an existing Ray cluster or start one and connect to it.

  This method handles two cases. Either a Ray cluster already exists and we
  just attach this driver to it, or we start all of the processes associated
  with a Ray cluster and attach to the newly started cluster.

  Args:
    node_ip_address (str): The IP address of the node that we are on.
    redis_address (str): The address of the Redis server to connect to. If this
      address is not provided, then this command will start Redis, a global
      scheduler, a local scheduler, a plasma store, a plasma manager, and some
      workers. It will also kill these processes when Python exits.
    object_id_seed (int): Used to seed the deterministic generation of object
      IDs. The same value can be used across multiple runs of the same job in
      order to generate the object IDs in a consistent manner. However, the
      same ID should not be used for different jobs.
    num_workers (int): The number of workers to start. This is only provided if
      redis_address is not provided.
    driver_mode (bool): The mode in which to start the driver. This should be
      one of ray.SCRIPT_MODE, ray.PYTHON_MODE, and ray.SILENT_MODE.
    redirect_output (bool): True if stdout and stderr for all the processes
      should be redirected to files and false otherwise.
    num_cpus (int): Number of cpus the user wishes all local schedulers to be
      configured with.
    num_gpus (int): Number of gpus the user wishes all local schedulers to be
      configured with.

  Returns:
    Address information about the started processes.

  Raises:
    Exception: An exception is raised if an inappropriate combination of
      arguments is passed in.
  """
  info = {"node_ip_address": node_ip_address,
          "redis_address": redis_address}
  return _init(address_info=info, start_ray_local=(redis_address is None),
               num_workers=num_workers, driver_mode=driver_mode,
               redirect_output=redirect_output, num_cpus=num_cpus,
               num_gpus=num_gpus)


def cleanup(worker=global_worker):
  """Disconnect the worker, and terminate any processes started in init.

  This will automatically run at the end when a Python process that uses Ray
  exits. It is ok to run this twice in a row. Note that we manually call
  services.cleanup() in the tests because we need to start and stop many
  clusters in the tests, but the import and exit only happen once.
  """
  disconnect(worker)
  if hasattr(worker, "local_scheduler_client"):
    del worker.local_scheduler_client
  if hasattr(worker, "plasma_client"):
    worker.plasma_client.shutdown()

  if worker.mode in [SCRIPT_MODE, SILENT_MODE]:
    # If this is a driver, push the finish time to Redis and clean up any
    # other services that were started with the driver.
    worker.redis_client.hmset(b"Drivers:" + worker.worker_id,
                              {"end_time": time.time()})
    services.cleanup()
  else:
    # If this is not a driver, make sure there are no orphan processes, besides
    # possibly the worker itself.
    for process_type, processes in services.all_processes.items():
      if process_type == services.PROCESS_TYPE_WORKER:
        assert(len(processes)) <= 1
      else:
        assert(len(processes) == 0)

  worker.set_mode(None)


atexit.register(cleanup)

# Define a custom excepthook so that if the driver exits with an exception, we
# can push that exception to Redis.
normal_excepthook = sys.excepthook


def custom_excepthook(type, value, tb):
  # If this is a driver, push the exception to redis.
  if global_worker.mode in [SCRIPT_MODE, SILENT_MODE]:
    error_message = "".join(traceback.format_tb(tb))
    global_worker.redis_client.hmset(b"Drivers:" + global_worker.worker_id,
                                     {"exception": error_message})
  # Call the normal excepthook.
  normal_excepthook(type, value, tb)


sys.excepthook = custom_excepthook


def print_error_messages(worker):
  """Print error messages in the background on the driver.

  This runs in a separate thread on the driver and prints error messages in the
  background.
  """
  # TODO(rkn): All error messages should have a "component" field indicating
  # which process the error came from (e.g., a worker or a plasma store).
  # Currently all error messages come from workers.

  helpful_message = """
You can inspect errors by running

    ray.error_info()

If this driver is hanging, start a new one with

    ray.init(redis_address="{}")
""".format(worker.redis_address)

  worker.error_message_pubsub_client = worker.redis_client.pubsub()
  # Exports that are published after the call to
  # error_message_pubsub_client.psubscribe and before the call to
  # error_message_pubsub_client.listen will still be processed in the loop.
  worker.error_message_pubsub_client.psubscribe("__keyspace@0__:ErrorKeys")
  num_errors_received = 0

  # Get the exports that occurred before the call to psubscribe.
  with worker.lock:
    error_keys = worker.redis_client.lrange("ErrorKeys", 0, -1)
    for error_key in error_keys:
      if error_applies_to_driver(error_key, worker=worker):
        error_message = worker.redis_client.hget(error_key,
                                                 "message").decode("ascii")
        print(error_message)
        print(helpful_message)
      num_errors_received += 1

  try:
    for msg in worker.error_message_pubsub_client.listen():
      with worker.lock:
        for error_key in worker.redis_client.lrange("ErrorKeys",
                                                    num_errors_received, -1):
          if error_applies_to_driver(error_key, worker=worker):
            error_message = worker.redis_client.hget(error_key,
                                                     "message").decode("ascii")
            print(error_message)
            print(helpful_message)
          num_errors_received += 1
  except redis.ConnectionError:
    # When Redis terminates the listen call will throw a ConnectionError, which
    # we catch here.
    pass


def fetch_and_register_remote_function(key, worker=global_worker):
  """Import a remote function."""
  (driver_id, function_id_str, function_name, serialized_function,
      num_return_vals, module, num_cpus, num_gpus) = worker.redis_client.hmget(
          key, ["driver_id",
                "function_id",
                "name",
                "function",
                "num_return_vals",
                "module",
                "num_cpus",
                "num_gpus"])
  function_id = ray.local_scheduler.ObjectID(function_id_str)
  function_name = function_name.decode("ascii")
  num_return_vals = int(num_return_vals)
  num_cpus = int(num_cpus)
  num_gpus = int(num_gpus)
  module = module.decode("ascii")

  # This is a placeholder in case the function can't be unpickled. This will be
  # overwritten if the function is successfully registered.
  def f():
    raise Exception("This function was not imported properly.")
  remote_f_placeholder = remote(function_id=function_id)(lambda *xs: f())
  worker.functions[driver_id][function_id.id()] = (function_name,
                                                   remote_f_placeholder)
  worker.function_properties[driver_id][function_id.id()] = (num_return_vals,
                                                             num_cpus,
                                                             num_gpus)

  try:
    function = pickling.loads(serialized_function)
  except:
    # If an exception was thrown when the remote function was imported, we
    # record the traceback and notify the scheduler of the failure.
    traceback_str = format_error_message(traceback.format_exc())
    # Log the error message.
    worker.push_error_to_driver(driver_id, "register_remote_function",
                                traceback_str,
                                data={"function_id": function_id.id(),
                                      "function_name": function_name})
  else:
    # TODO(rkn): Why is the below line necessary?
    function.__module__ = module
    worker.functions[driver_id][function_id.id()] = (
        function_name, remote(function_id=function_id)(function))
    # Add the function to the function table.
    worker.redis_client.rpush("FunctionTable:{}".format(function_id.id()),
                              worker.worker_id)


def fetch_and_register_environment_variable(key, worker=global_worker):
  """Import an environment variable."""
  (driver_id, environment_variable_name, serialized_initializer,
   serialized_reinitializer) = worker.redis_client.hmget(
      key, ["driver_id", "name", "initializer", "reinitializer"])
  environment_variable_name = environment_variable_name.decode("ascii")
  try:
    initializer = pickling.loads(serialized_initializer)
    reinitializer = pickling.loads(serialized_reinitializer)
    env.__setattr__(environment_variable_name,
                    EnvironmentVariable(initializer, reinitializer))
  except:
    # If an exception was thrown when the environment variable was imported, we
    # record the traceback and notify the scheduler of the failure.
    traceback_str = format_error_message(traceback.format_exc())
    # Log the error message.
    worker.push_error_to_driver(driver_id, "register_environment_variable",
                                traceback_str,
                                data={"name": environment_variable_name})


def fetch_and_execute_function_to_run(key, worker=global_worker):
  """Run on arbitrary function on the worker."""
  driver_id, serialized_function = worker.redis_client.hmget(
      key, ["driver_id", "function"])
  # Get the number of workers on this node that have already started executing
  # this remote function, and increment that value. Subtract 1 so the counter
  # starts at 0.
  counter = worker.redis_client.hincrby(worker.node_ip_address, key, 1) - 1
  try:
    # Deserialize the function.
    function = pickling.loads(serialized_function)
    # Run the function.
    function({"counter": counter})
  except:
    # If an exception was thrown when the function was run, we record the
    # traceback and notify the scheduler of the failure.
    traceback_str = traceback.format_exc()
    # Log the error message.
    name = function.__name__ if ("function" in locals() and
                                 hasattr(function, "__name__")) else ""
    worker.push_error_to_driver(driver_id, "function_to_run", traceback_str,
                                data={"name": name})


def import_thread(worker):
  worker.import_pubsub_client = worker.redis_client.pubsub()
  # Exports that are published after the call to
  # import_pubsub_client.psubscribe and before the call to
  # import_pubsub_client.listen will still be processed in the loop.
  worker.import_pubsub_client.psubscribe("__keyspace@0__:Exports")
  # Keep track of the number of imports that we've imported.
  num_imported = 0

  # Get the exports that occurred before the call to psubscribe.
  with worker.lock:
    export_keys = worker.redis_client.lrange("Exports", 0, -1)
    for key in export_keys:
      if key.startswith(b"RemoteFunction"):
        fetch_and_register_remote_function(key, worker=worker)
      elif key.startswith(b"EnvironmentVariables"):
        fetch_and_register_environment_variable(key, worker=worker)
      elif key.startswith(b"FunctionsToRun"):
        fetch_and_execute_function_to_run(key, worker=worker)
      elif key.startswith(b"ActorClass"):
        # If this worker is an actor that is supposed to construct this class,
        # fetch the actor and class information and construct the class.
        class_id = key.split(b":", 1)[1]
        if worker.actor_id != NIL_ACTOR_ID and worker.class_id == class_id:
          worker.fetch_and_register_actor(key, worker)
      else:
        raise Exception("This code should be unreachable.")
      num_imported += 1

  try:
    for msg in worker.import_pubsub_client.listen():
      with worker.lock:
        if msg["type"] == "psubscribe":
          continue
        assert msg["data"] == b"rpush"
        num_imports = worker.redis_client.llen("Exports")
        assert num_imports >= num_imported
        for i in range(num_imported, num_imports):
          key = worker.redis_client.lindex("Exports", i)
          if key.startswith(b"RemoteFunction"):
            with log_span("ray:import_remote_function", worker=worker):
              fetch_and_register_remote_function(key, worker=worker)
          elif key.startswith(b"EnvironmentVariables"):
            with log_span("ray:import_environment_variable", worker=worker):
              fetch_and_register_environment_variable(key, worker=worker)
          elif key.startswith(b"FunctionsToRun"):
            with log_span("ray:import_function_to_run", worker=worker):
              fetch_and_execute_function_to_run(key, worker=worker)
          elif key.startswith(b"Actor"):
            # Only get the actor if the actor ID matches the actor ID of this
            # worker.
            actor_id, = worker.redis_client.hmget(key, "actor_id")
            if worker.actor_id == actor_id:
              worker.fetch_and_register["Actor"](key, worker)
          else:
            raise Exception("This code should be unreachable.")
          num_imported += 1
  except redis.ConnectionError:
    # When Redis terminates the listen call will throw a ConnectionError, which
    # we catch here.
    pass


def connect(info, object_id_seed=None, mode=WORKER_MODE, worker=global_worker,
            actor_id=NIL_ACTOR_ID):
  """Connect this worker to the local scheduler, to Plasma, and to Redis.

  Args:
    info (dict): A dictionary with address of the Redis server and the sockets
      of the plasma store, plasma manager, and local scheduler.
    object_id_seed: A seed to use to make the generation of object IDs
      deterministic.
    mode: The mode of the worker. One of SCRIPT_MODE, WORKER_MODE, PYTHON_MODE,
      and SILENT_MODE.
    actor_id: The ID of the actor running on this worker. If this worker is not
      an actor, then this is NIL_ACTOR_ID.
  """
  check_main_thread()
  # Do some basic checking to make sure we didn't call ray.init twice.
  error_message = "Perhaps you called ray.init twice by accident?"
  assert not worker.connected, error_message
  assert worker.cached_functions_to_run is not None, error_message
  assert worker.cached_remote_functions is not None, error_message
  assert env._cached_environment_variables is not None, error_message
  # Initialize some fields.
  worker.worker_id = random_string()
  worker.actor_id = actor_id
  worker.connected = True
  worker.set_mode(mode)
  # The worker.events field is used to aggregate logging information and
  # display it in the web UI. Note that Python lists protected by the GIL,
  # which is important because we will append to this field from multiple
  # threads.
  worker.events = []
  # If running Ray in PYTHON_MODE, there is no need to create call
  # create_worker or to start the worker service.
  if mode == PYTHON_MODE:
    return
  # Set the node IP address.
  worker.node_ip_address = info["node_ip_address"]
  worker.redis_address = info["redis_address"]
  # Create a Redis client.
  redis_ip_address, redis_port = info["redis_address"].split(":")
  worker.redis_client = redis.StrictRedis(host=redis_ip_address,
                                          port=int(redis_port))
  worker.lock = threading.Lock()

  # Create an object for interfacing with the global state.
  global_state._initialize_global_state(redis_ip_address, int(redis_port))

  # Register the worker with Redis.
  if mode in [SCRIPT_MODE, SILENT_MODE]:
    # The concept of a driver is the same as the concept of a "job". Register
    # the driver/job with Redis here.
    import __main__ as main
    driver_info = {
        "node_ip_address": worker.node_ip_address,
        "driver_id": worker.worker_id,
        "start_time": time.time(),
        "plasma_store_socket": info["store_socket_name"],
        "plasma_manager_socket": info["manager_socket_name"],
        "local_scheduler_socket": info["local_scheduler_socket_name"]}
    driver_info["name"] = (main.__file__ if hasattr(main, "__file__")
                           else "INTERACTIVE MODE")
    worker.redis_client.hmset(b"Drivers:" + worker.worker_id, driver_info)
    is_worker = False
  elif mode == WORKER_MODE:
    # Register the worker with Redis.
    worker.redis_client.hmset(
        b"Workers:" + worker.worker_id,
        {"node_ip_address": worker.node_ip_address,
         "plasma_store_socket": info["store_socket_name"],
         "plasma_manager_socket": info["manager_socket_name"],
         "local_scheduler_socket": info["local_scheduler_socket_name"]})
    is_worker = True
  else:
    raise Exception("This code should be unreachable.")

  # Create an object store client.
  worker.plasma_client = ray.plasma.PlasmaClient(info["store_socket_name"],
                                                 info["manager_socket_name"])
  # Create the local scheduler client.
  if worker.actor_id != NIL_ACTOR_ID:
    num_gpus = int(worker.redis_client.hget("Actor:{}".format(actor_id),
                                            "num_gpus"))
  else:
    num_gpus = 0
  worker.local_scheduler_client = ray.local_scheduler.LocalSchedulerClient(
      info["local_scheduler_socket_name"], worker.worker_id, worker.actor_id,
      is_worker, num_gpus)

  # If this is a driver, set the current task ID, the task driver ID, and set
  # the task index to 0.
  if mode in [SCRIPT_MODE, SILENT_MODE]:
    # If the user provided an object_id_seed, then set the current task ID
    # deterministically based on that seed (without altering the state of the
    # user's random number generator). Otherwise, set the current task ID
    # randomly to avoid object ID collisions.
    numpy_state = np.random.get_state()
    if object_id_seed is not None:
      np.random.seed(object_id_seed)
    else:
      # Try to use true randomness.
      np.random.seed(None)
    worker.current_task_id = ray.local_scheduler.ObjectID(np.random.bytes(20))
    # When tasks are executed on remote workers in the context of multiple
    # drivers, the task driver ID is used to keep track of which driver is
    # responsible for the task so that error messages will be propagated to the
    # correct driver.
    worker.task_driver_id = ray.local_scheduler.ObjectID(worker.worker_id)
    # Reset the state of the numpy random number generator.
    np.random.set_state(numpy_state)
    # Set other fields needed for computing task IDs.
    worker.task_index = 0
    worker.put_index = 0

    # Create an entry for the driver task in the task table. This task is added
    # immediately with status RUNNING. This allows us to push errors related to
    # this driver task back to the driver.  For example, if the driver creates
    # an object that is later evicted, we should notify the user that we're
    # unable to reconstruct the object, since we cannot rerun the driver.
    driver_task = ray.local_scheduler.Task(
        worker.task_driver_id,
        ray.local_scheduler.ObjectID(NIL_FUNCTION_ID),
        [],
        0,
        worker.current_task_id,
        worker.task_index,
        ray.local_scheduler.ObjectID(NIL_ACTOR_ID),
        worker.actor_counters[actor_id],
        [0, 0])
    worker.redis_client.execute_command(
        "RAY.TASK_TABLE_ADD",
        driver_task.task_id().id(),
        TASK_STATUS_RUNNING,
        NIL_LOCAL_SCHEDULER_ID,
        ray.local_scheduler.task_to_string(driver_task))
    # Set the driver's current task ID to the task ID assigned to the driver
    # task.
    worker.current_task_id = driver_task.task_id()

  # If this is an actor, get the ID of the corresponding class for the actor.
  if worker.actor_id != NIL_ACTOR_ID:
    actor_key = "Actor:{}".format(worker.actor_id)
    class_id = worker.redis_client.hget(actor_key, "class_id")
    worker.class_id = class_id

  # If this is a worker, then start a thread to import exports from the driver.
  if mode == WORKER_MODE:
    t = threading.Thread(target=import_thread, args=(worker,))
    # Making the thread a daemon causes it to exit when the main thread exits.
    t.daemon = True
    t.start()

  # If this is a driver running in SCRIPT_MODE, start a thread to print error
  # messages asynchronously in the background. Ideally the scheduler would push
  # messages to the driver's worker service, but we ran into bugs when trying
  # to properly shutdown the driver's worker service, so we are temporarily
  # using this implementation which constantly queries the scheduler for new
  # error messages.
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
    worker.run_function_on_all_workers(
        lambda worker_info: sys.path.insert(1, script_directory))
    worker.run_function_on_all_workers(
        lambda worker_info: sys.path.insert(1, current_directory))
    # TODO(rkn): Here we first export functions to run, then environment
    # variables, then remote functions. The order matters. For example, one of
    # the functions to run may set the Python path, which is needed to import a
    # module used to define an environment variable, which in turn is used
    # inside a remote function. We may want to change the order to simply be
    # the order in which the exports were defined on the driver. In addition,
    # we will need to retain the ability to decide what the first few exports
    # are (mostly to set the Python path). Additionally, note that the first
    # exports to be defined on the driver will be the ones defined in separate
    # modules that are imported by the driver.
    # Export cached functions_to_run.
    for function in worker.cached_functions_to_run:
      worker.run_function_on_all_workers(function)
    # Export cached environment variables to the workers.
    for name, environment_variable in env._cached_environment_variables:
      env.__setattr__(name, environment_variable)
    # Export cached remote functions to the workers.
    for info in worker.cached_remote_functions:
      (function_id, func_name, func,
       func_invoker, num_return_vals, num_cpus, num_gpus) = info
      export_remote_function(function_id, func_name, func, func_invoker,
                             num_return_vals, num_cpus, num_gpus, worker)
  worker.cached_functions_to_run = None
  worker.cached_remote_functions = None
  env._cached_environment_variables = None


def disconnect(worker=global_worker):
  """Disconnect this worker from the scheduler and object store."""
  # Reset the list of cached remote functions so that if more remote functions
  # are defined and then connect is called again, the remote functions will be
  # exported. This is mostly relevant for the tests.
  worker.connected = False
  worker.cached_functions_to_run = []
  worker.cached_remote_functions = []
  env._cached_environment_variables = []


def register_class(cls, pickle=False, worker=global_worker):
  """Enable workers to serialize or deserialize objects of a particular class.

  This method runs the register_class function defined below on every worker,
  which will enable numbuf to properly serialize and deserialize objects of
  this class.

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

  def register_class_for_serialization(worker_info):
    serialization.add_class_to_whitelist(cls, pickle=pickle)
  worker.run_function_on_all_workers(register_class_for_serialization)


class RayLogSpan(object):
  """An object used to enable logging a span of events with a with statement.

  Attributes:
    event_type (str): The type of the event being logged.
    contents: Additional information to log.
  """
  def __init__(self, event_type, contents=None, worker=global_worker):
    """Initialize a RayLogSpan object."""
    self.event_type = event_type
    self.contents = contents
    self.worker = worker

  def __enter__(self):
    """Log the beginning of a span event."""
    log(event_type=self.event_type,
        contents=self.contents,
        kind=LOG_SPAN_START,
        worker=self.worker)

  def __exit__(self, type, value, tb):
    """Log the end of a span event. Log any exception that occurred."""
    if type is None:
      log(event_type=self.event_type, kind=LOG_SPAN_END, worker=self.worker)
    else:
      log(event_type=self.event_type,
          contents={"type": str(type),
                    "value": value,
                    "traceback": traceback.format_exc()},
          kind=LOG_SPAN_END,
          worker=self.worker)


def log_span(event_type, contents=None, worker=global_worker):
  return RayLogSpan(event_type, contents=contents, worker=worker)


def log_event(event_type, contents=None, worker=global_worker):
  log(event_type, kind=LOG_POINT, contents=contents, worker=worker)


def log(event_type, kind, contents=None, worker=global_worker):
  """Log an event to the global state store.

  This adds the event to a buffer of events locally. The buffer can be flushed
  and written to the global state store by calling flush_log().

  Args:
    event_type (str): The type of the event.
    contents: More general data to store with the event.
    kind (int): Either LOG_POINT, LOG_SPAN_START, or LOG_SPAN_END. This is
      LOG_POINT if the event being logged happens at a single point in time. It
      is LOG_SPAN_START if we are starting to log a span of time, and it is
      LOG_SPAN_END if we are finishing logging a span of time.
  """
  # TODO(rkn): This code currently takes around half a microsecond. Since we
  # call it tens of times per task, this adds up. We will need to redo the
  # logging code, perhaps in C.
  contents = {} if contents is None else contents
  assert isinstance(contents, dict)
  # Make sure all of the keys and values in the dictionary are strings.
  contents = {str(k): str(v) for k, v in contents.items()}
  worker.events.append((time.time(), event_type, kind, contents))


def flush_log(worker=global_worker):
  """Send the logged worker events to the global state store."""
  event_log_key = (b"event_log:" + worker.worker_id + b":" +
                   worker.current_task_id.id())
  event_log_value = json.dumps(worker.events)
  worker.local_scheduler_client.log_event(event_log_key, event_log_value)
  worker.events = []


def get(object_ids, worker=global_worker):
  """Get a remote object or a list of remote objects from the object store.

  This method blocks until the object corresponding to the object ID is
  available in the local object store. If this object is not in the local
  object store, it will be shipped from an object store that has it (once the
  object has been created). If object_ids is a list, then the objects
  corresponding to each object in the list will be returned.

  Args:
    object_ids: Object ID of the object to get or a list of object IDs to get.

  Returns:
    A Python object or a list of Python objects.
  """
  check_connected(worker)
  with log_span("ray:get", worker=worker):
    check_main_thread()

    if worker.mode == PYTHON_MODE:
      # In PYTHON_MODE, ray.get is the identity operation (the input will
      # actually be a value not an objectid).
      return object_ids
    if isinstance(object_ids, list):
      values = worker.get_object(object_ids)
      for i, value in enumerate(values):
        if isinstance(value, RayTaskError):
          raise RayGetError(object_ids[i], value)
      return values
    else:
      value = worker.get_object([object_ids])[0]
      if isinstance(value, RayTaskError):
        # If the result is a RayTaskError, then the task that created this
        # object failed, and we should propagate the error message here.
        raise RayGetError(object_ids, value)
      return value


def put(value, worker=global_worker):
  """Store an object in the object store.

  Args:
    value: The Python object to be stored.

  Returns:
    The object ID assigned to this value.
  """
  check_connected(worker)
  with log_span("ray:put", worker=worker):
    check_main_thread()

    if worker.mode == PYTHON_MODE:
      # In PYTHON_MODE, ray.put is the identity operation.
      return value
    object_id = worker.local_scheduler_client.compute_put_id(
        worker.current_task_id, worker.put_index)
    worker.put_object(object_id, value)
    worker.put_index += 1
    return object_id


def wait(object_ids, num_returns=1, timeout=None, worker=global_worker):
  """Return a list of IDs that are ready and a list of IDs that are not ready.

  If timeout is set, the function returns either when the requested number of
  IDs are ready or when the timeout is reached, whichever occurs first. If it
  is not set, the function simply waits until that number of objects is ready
  and returns that exact number of objectids.

  This method returns two lists. The first list consists of object IDs that
  correspond to objects that are stored in the object store. The second list
  corresponds to the rest of the object IDs (which may or may not be ready).

  Args:
    object_ids (List[ObjectID]): List of object IDs for objects that may or may
      not be ready. Note that these IDs must be unique.
    num_returns (int): The number of object IDs that should be returned.
    timeout (int): The maximum amount of time in milliseconds to wait before
      returning.

  Returns:
    A list of object IDs that are ready and a list of the remaining object IDs.
  """
  check_connected(worker)
  with log_span("ray:wait", worker=worker):
    check_main_thread()
    object_id_strs = [object_id.id() for object_id in object_ids]
    timeout = timeout if timeout is not None else 2 ** 30
    ready_ids, remaining_ids = worker.plasma_client.wait(object_id_strs,
                                                         timeout, num_returns)
    ready_ids = [ray.local_scheduler.ObjectID(object_id)
                 for object_id in ready_ids]
    remaining_ids = [ray.local_scheduler.ObjectID(object_id)
                     for object_id in remaining_ids]
    return ready_ids, remaining_ids


def wait_for_function(function_id, driver_id, timeout=10,
                      worker=global_worker):
  """Wait until the function to be executed is present on this worker.

  This method will simply loop until the import thread has imported the
  relevant function. If we spend too long in this loop, that may indicate a
  problem somewhere and we will push an error message to the user.

  If this worker is an actor, then this will wait until the actor has been
  defined.

  Args:
    is_actor (bool): True if this worker is an actor, and false otherwise.
    function_id (str): The ID of the function that we want to execute.
    driver_id (str): The ID of the driver to push the error message to if this
      times out.
  """
  start_time = time.time()
  # Only send the warning once.
  warning_sent = False
  num_warnings_sent = 0
  while True:
    with worker.lock:
      if worker.actor_id == NIL_ACTOR_ID and (function_id.id() in
                                              worker.functions[driver_id]):
        break
      elif worker.actor_id != NIL_ACTOR_ID and (worker.actor_id in
                                                worker.actors):
        break
      if time.time() - start_time > timeout * (num_warnings_sent + 1):
        warning_message = ("This worker was asked to execute a function that "
                           "it does not have registered. You may have to "
                           "restart Ray.")
        if not warning_sent:
          worker.push_error_to_driver(driver_id, "wait_for_function",
                                      warning_message)
        warning_sent = True
    time.sleep(0.001)


def format_error_message(exception_message, task_exception=False):
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
  if task_exception:
    # For errors that occur inside of tasks, remove lines 1, 2, 3, and 4,
    # which are always the same, they just contain information about the main
    # loop.
    lines = lines[0:1] + lines[5:]
  return "\n".join(lines)


def main_loop(worker=global_worker):
  """The main loop a worker runs to receive and execute tasks.

  This method is an infinite loop. It waits to receive commands from the
  scheduler. A command may consist of a task to execute, a remote function to
  import, an environment variable to import, or an order to terminate the
  worker process. The worker executes the command, notifies the scheduler of
  any errors that occurred while executing the command, and waits for the next
  command.
  """

  def exit(signum, frame):
    cleanup(worker=worker)
    sys.exit(0)

  signal.signal(signal.SIGTERM, exit)

  def process_task(task):
    """Execute a task assigned to this worker.

    This method deserializes a task from the scheduler, and attempts to execute
    the task. If the task succeeds, the outputs are stored in the local object
    store. If the task throws an exception, RayTaskError objects are stored in
    the object store to represent the failed task (these will be retrieved by
    calls to get or by subsequent tasks that use the outputs of this task).
    After the task executes, the worker resets any environment variables that
    were accessed by the task.
    """
    try:
      # The ID of the driver that this task belongs to. This is needed so that
      # if the task throws an exception, we propagate the error message to the
      # correct driver.
      worker.task_driver_id = task.driver_id()
      worker.current_task_id = task.task_id()
      worker.current_function_id = task.function_id().id()
      worker.task_index = 0
      worker.put_index = 0
      function_id = task.function_id()
      args = task.arguments()
      return_object_ids = task.returns()
      function_name, function_executor = (worker.functions
                                          [worker.task_driver_id.id()]
                                          [function_id.id()])

      # Get task arguments from the object store.
      with log_span("ray:task:get_arguments", worker=worker):
        arguments = get_arguments_for_execution(function_name, args, worker)

      # Execute the task.
      with log_span("ray:task:execute", worker=worker):
        if task.actor_id().id() == NIL_ACTOR_ID:
          outputs = function_executor.executor(arguments)
        else:
          outputs = function_executor(
              worker.actors[task.actor_id().id()], *arguments)

      # Store the outputs in the local object store.
      with log_span("ray:task:store_outputs", worker=worker):
        if len(return_object_ids) == 1:
          outputs = (outputs,)
        store_outputs_in_objstore(return_object_ids, outputs, worker)
    except Exception as e:
      # We determine whether the exception was caused by the call to
      # get_arguments_for_execution or by the execution of the remote function
      # or by the call to store_outputs_in_objstore. Depending on which case
      # occurred, we format the error message differently.
      # whether the variables "arguments" and "outputs" are defined.
      if "arguments" in locals() and "outputs" not in locals():
        if task.actor_id().id() == NIL_ACTOR_ID:
          # The error occurred during the task execution.
          traceback_str = format_error_message(traceback.format_exc(),
                                               task_exception=True)
        else:
          # The error occurred during the execution of an actor task.
          traceback_str = format_error_message(traceback.format_exc())
      elif "arguments" in locals() and "outputs" in locals():
        # The error occurred after the task executed.
        traceback_str = format_error_message(traceback.format_exc())
      else:
        # The error occurred before the task execution.
        if isinstance(e, RayGetError) or isinstance(e, RayGetArgumentError):
          # In this case, getting the task arguments failed.
          traceback_str = None
        else:
          traceback_str = traceback.format_exc()
      failure_object = RayTaskError(function_name, e, traceback_str)
      failure_objects = [failure_object for _ in range(len(return_object_ids))]
      store_outputs_in_objstore(return_object_ids, failure_objects, worker)
      # Log the error message.
      worker.push_error_to_driver(worker.task_driver_id.id(), "task",
                                  str(failure_object),
                                  data={"function_id": function_id.id(),
                                        "function_name": function_name})
    try:
      # Reinitialize the values of environment variables that were used in the
      # task above so that changes made to their state do not affect other
      # tasks.
      with log_span("ray:task:reinitialize_environment_variables",
                    worker=worker):
        env._reinitialize()
    except Exception as e:
      # The attempt to reinitialize the environment variables threw an
      # exception. We record the traceback and notify the scheduler.
      traceback_str = format_error_message(traceback.format_exc())
      worker.push_error_to_driver(worker.task_driver_id.id(),
                                  "reinitialize_environment_variable",
                                  traceback_str,
                                  data={"function_id": function_id.id(),
                                        "function_name": function_name})

  check_main_thread()
  while True:
    with log_span("ray:get_task", worker=worker):
      task = worker.local_scheduler_client.get_task()

    function_id = task.function_id()
    # Wait until the function to be executed has actually been registered on
    # this worker. We will push warnings to the user if we spend too long in
    # this loop.
    with log_span("ray:wait_for_function", worker=worker):
      wait_for_function(function_id, task.driver_id().id(), worker=worker)

    # Execute the task.
    # TODO(rkn): Consider acquiring this lock with a timeout and pushing a
    # warning to the user if we are waiting too long to acquire the lock
    # because that may indicate that the system is hanging, and it'd be good to
    # know where the system is hanging.
    log(event_type="ray:acquire_lock", kind=LOG_SPAN_START, worker=worker)
    with worker.lock:
      log(event_type="ray:acquire_lock", kind=LOG_SPAN_END, worker=worker)

      function_name, _ = (worker.functions[task.driver_id().id()]
                          [function_id.id()])
      contents = {"function_name": function_name,
                  "task_id": task.task_id().hex()}
      with log_span("ray:task", contents=contents, worker=worker):
        process_task(task)

    # Push all of the log events to the global state store.
    flush_log()


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


def _env():
  """Return the env object.

  We use this wrapper because so that functions which use the env object can be
  pickled.
  """
  return env


def _export_environment_variable(name, environment_variable,
                                 worker=global_worker):
  """Export an environment variable to the workers.

  This is only called by a driver.

  Args:
    name (str): The name of the variable to export.
    environment_variable (EnvironmentVariable): The environment variable object
      containing code for initializing and reinitializing the variable.
  """
  check_main_thread()
  if _mode(worker) not in [SCRIPT_MODE, SILENT_MODE]:
    raise Exception("_export_environment_variable can only be called on a "
                    "driver.")
  environment_variable_id = name
  key = "EnvironmentVariables:{}:{}".format(random_string(),
                                            environment_variable_id)
  worker.redis_client.hmset(key, {
      "driver_id": worker.task_driver_id.id(),
      "name": name,
      "initializer": pickling.dumps(environment_variable.initializer),
      "reinitializer": pickling.dumps(environment_variable.reinitializer)})
  worker.redis_client.rpush("Exports", key)


def export_remote_function(function_id, func_name, func, func_invoker,
                           num_return_vals, num_cpus, num_gpus,
                           worker=global_worker):
  check_main_thread()
  if _mode(worker) not in [SCRIPT_MODE, SILENT_MODE]:
    raise Exception("export_remote_function can only be called on a driver.")

  worker.function_properties[worker.task_driver_id.id()][function_id.id()] = (
      num_return_vals, num_cpus, num_gpus)
  key = "RemoteFunction:{}:{}".format(worker.task_driver_id, function_id.id())

  # Work around limitations of Python pickling.
  func_name_global_valid = func.__name__ in func.__globals__
  func_name_global_value = func.__globals__.get(func.__name__)
  # Allow the function to reference itself as a global variable
  func.__globals__[func.__name__] = func_invoker
  try:
    pickled_func = pickling.dumps(func)
  finally:
    # Undo our changes
    if func_name_global_valid:
      func.__globals__[func.__name__] = func_name_global_value
    else:
      del func.__globals__[func.__name__]

  worker.redis_client.hmset(key, {"driver_id": worker.task_driver_id.id(),
                                  "function_id": function_id.id(),
                                  "name": func_name,
                                  "module": func.__module__,
                                  "function": pickled_func,
                                  "num_return_vals": num_return_vals,
                                  "num_cpus": num_cpus,
                                  "num_gpus": num_gpus})
  worker.redis_client.rpush("Exports", key)


def in_ipython():
  """Return true if we are in an IPython interpreter and false otherwise."""
  try:
    __IPYTHON__
    return True
  except NameError:
    return False


def compute_function_id(func_name, func):
  """Compute an function ID for a function.

  Args:
    func_name: The name of the function (this includes the module name plus the
      function name).
    func: The actual function.

  Returns:
    This returns the function ID.
  """
  function_id_hash = hashlib.sha1()
  # Include the function name in the hash.
  function_id_hash.update(func_name.encode("ascii"))
  # If we are running a script or are in IPython, include the source code in
  # the hash. If we are in a regular Python interpreter we skip this part
  # because the source code is not accessible.
  import __main__ as main
  if hasattr(main, "__file__") or in_ipython():
    function_id_hash.update(inspect.getsource(func).encode("ascii"))
  # Compute the function ID.
  function_id = function_id_hash.digest()
  assert len(function_id) == 20
  function_id = FunctionID(function_id)

  return function_id


def remote(*args, **kwargs):
  """This decorator is used to create remote functions.

  Args:
    num_return_vals (int): The number of object IDs that a call to this
      function should return.
    num_cpus (int): The number of CPUs needed to execute this function. This
      should only be passed in when defining the remote function on the driver.
    num_gpus (int): The number of GPUs needed to execute this function. This
      should only be passed in when defining the remote function on the driver.
  """
  worker = global_worker

  def make_remote_decorator(num_return_vals, num_cpus, num_gpus, func_id=None):
    def remote_decorator(func_or_class):
      if inspect.isfunction(func_or_class):
        return remote_function_decorator(func_or_class)
      if inspect.isclass(func_or_class):
        return worker.make_actor(func_or_class, num_cpus, num_gpus)
      raise Exception("The @ray.remote decorator must be applied to either a "
                      "function or to a class.")

    def remote_function_decorator(func):
      func_name = "{}.{}".format(func.__module__, func.__name__)
      if func_id is None:
        function_id = compute_function_id(func_name, func)
      else:
        function_id = func_id

      def func_call(*args, **kwargs):
        """This gets run immediately when a worker calls a remote function."""
        check_connected()
        check_main_thread()
        args = signature.extend_args(function_signature, args, kwargs)

        if _mode() == PYTHON_MODE:
          # In PYTHON_MODE, remote calls simply execute the function. We copy
          # the arguments to prevent the function call from mutating them and
          # to match the usual behavior of immutable remote objects.
          try:
            _env()._running_remote_function_locally = True
            result = func(*copy.deepcopy(args))
          finally:
            _env()._reinitialize()
            _env()._running_remote_function_locally = False
          return result
        objectids = _submit_task(function_id, func_name, args)
        if len(objectids) == 1:
          return objectids[0]
        elif len(objectids) > 1:
          return objectids

      def func_executor(arguments):
        """This gets run when the remote function is executed."""
        result = func(*arguments)
        return result

      def func_invoker(*args, **kwargs):
        """This is used to invoke the function."""
        raise Exception("Remote functions cannot be called directly. Instead "
                        "of running '{}()', try '{}.remote()'."
                        .format(func_name, func_name))
      func_invoker.remote = func_call
      func_invoker.executor = func_executor
      func_invoker.is_remote = True
      func_name = "{}.{}".format(func.__module__, func.__name__)
      func_invoker.func_name = func_name
      if sys.version_info >= (3, 0):
        func_invoker.__doc__ = func.__doc__
      else:
        func_invoker.func_doc = func.func_doc

      signature.check_signature_supported(func)
      function_signature = signature.extract_signature(func)

      # Everything ready - export the function
      if worker.mode in [SCRIPT_MODE, SILENT_MODE]:
        export_remote_function(function_id, func_name, func, func_invoker,
                               num_return_vals, num_cpus, num_gpus)
      elif worker.mode is None:
        worker.cached_remote_functions.append((function_id, func_name, func,
                                               func_invoker, num_return_vals,
                                               num_cpus, num_gpus))
      return func_invoker

    return remote_decorator

  num_return_vals = (kwargs["num_return_vals"] if "num_return_vals"
                     in kwargs.keys() else 1)
  num_cpus = kwargs["num_cpus"] if "num_cpus" in kwargs.keys() else 1
  num_gpus = kwargs["num_gpus"] if "num_gpus" in kwargs.keys() else 0

  if _mode() == WORKER_MODE:
    if "function_id" in kwargs:
      function_id = kwargs["function_id"]
      return make_remote_decorator(num_return_vals, num_cpus, num_gpus,
                                   function_id)

  if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
    # This is the case where the decorator is just @ray.remote.
    return make_remote_decorator(num_return_vals, num_cpus, num_gpus)(args[0])
  else:
    # This is the case where the decorator is something like
    # @ray.remote(num_return_vals=2).
    error_string = ("The @ray.remote decorator must be applied either with no "
                    "arguments and no parentheses, for example '@ray.remote', "
                    "or it must be applied using some of the arguments "
                    "'num_return_vals', 'num_cpus', or 'num_gpus', like "
                    "'@ray.remote(num_return_vals=2)'.")
    assert len(args) == 0 and ("num_return_vals" in kwargs or
                               "num_cpus" in kwargs or
                               "num_gpus" in kwargs), error_string
    for key in kwargs:
      assert key in ["num_return_vals", "num_cpus", "num_gpus"], error_string
    assert "function_id" not in kwargs
    return make_remote_decorator(num_return_vals, num_cpus, num_gpus)


def get_arguments_for_execution(function_name, serialized_args,
                                worker=global_worker):
  """Retrieve the arguments for the remote function.

  This retrieves the values for the arguments to the remote function that were
  passed in as object IDs. Argumens that were passed by value are not changed.
  This is called by the worker that is executing the remote function.

  Args:
    function_name (str): The name of the remote function whose arguments are
      being retrieved.
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
    if isinstance(arg, ray.local_scheduler.ObjectID):
      # get the object from the local object store
      argument = worker.get_object([arg])[0]
      if isinstance(argument, RayTaskError):
        # If the result is a RayTaskError, then the task that created this
        # object failed, and we should propagate the error message here.
        raise RayGetArgumentError(function_name, i, arg, argument)
    else:
      # pass the argument by value
      argument = arg

    arguments.append(argument)
  return arguments


def store_outputs_in_objstore(objectids, outputs, worker=global_worker):
  """Store the outputs of a remote function in the local object store.

  This stores the values that were returned by a remote function in the local
  object store. If any of the return values are object IDs, then these object
  IDs are aliased with the object IDs that the scheduler assigned for the
  return values. This is called by the worker that executes the remote
  function.

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
    worker.put_object(objectids[i], outputs[i])
