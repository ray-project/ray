import os
import time
import traceback
import copy
import logging
from types import ModuleType
import typing
import funcsigs
import numpy as np
import colorama

import ray
import serialization
import ray.internal.graph_pb2
import ray.graph
import services

class RayFailedObject(object):
  """An object used internally to represent a task that threw an exception.

  If a task throws an exception during execution, a RayFailedObject is stored in
  the object store for each of the tasks outputs. When an object is retrieved
  from the object store, the Python method that retrieved it should check to see
  if the object is a RayFailedObject and if it is then an exception should be
  thrown containing the error message.

  Attributes
    error_message (str): The error message raised by the task that failed.
  """

  def __init__(self, error_message=None):
    """Initialize a RayFailedObject.

    Args:
      error_message (str): The error message raised by the task for which a
        RayFailedObject is being created.
    """
    self.error_message = error_message

  def deserialize(self, primitives):
    """Create a RayFailedObject from a primitive object.

    This initializes a RayFailedObject from a primitive object created by the
    serialize method. This method is required in order for Ray to serialize
    custom Python classes.

    Note:
      This method should not be called by users.

    Args:
      primitives (str): The object's error message.
    """
    self.error_message = primitives

  def serialize(self):
    """Turn a RayFailedObject into a primitive object.

    This method is required in order for Ray to serialize
    custom Python classes.

    Note:
      The output of this method should only be used by the deserialize method.
      This method should not be called by users.

    Args:
      primitives (str): The object's error message.

    Returns:
      A primitive representation of a RayFailedObject.
    """
    return self.error_message

class RayDealloc(object):
  """An object used internally to properly implement reference counting.

  When we call get_object with a particular object reference, we create a
  RayDealloc object with the information necessary to properly handle closing
  the relevant memory segment when the object is no longer needed by the worker.
  The RayDealloc object is stored as a field in the object returned by
  get_object so that its destructor is only called when the worker no longer has
  any references to the object.

  Attributes
    handle (worker capsule): A Python object wrapping a C++ Worker object.
    segmentid (int): The id of the segment that contains the object that holds
      this RayDealloc object.
  """

  def __init__(self, handle, segmentid):
    """Initialize a RayDealloc object.

    Args:
      handle (worker capsule): A Python object wrapping a C++ Worker object.
      segmentid (int): The id of the segment that contains the object that holds
        this RayDealloc object.
    """
    self.handle = handle
    self.segmentid = segmentid

  def __del__(self):
    """Deallocate the relevant segment to avoid a memory leak."""
    ray.lib.unmap_object(self.handle, self.segmentid)

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

  """

  def __init__(self):
    """Initialize a Worker object."""
    self.functions = {}
    self.handle = None

  def set_mode(self, mode):
    """Set the mode of the worker.

    The mode ray.SCRIPT_MODE should be used if this Worker is a driver that is
    being run as a Python script. It will print information about task failures.

    The mode ray.SHELL_MODE should be used if this Worker is a driver that is
    being run interactively in a Python shell. It will print information about
    task failures and successes.

    The mode ray.WORKER_MODE should be used if this Worker is not a driver. It
    will not print information about tasks.

    The mode ray.PYTHON_MODE should be used if this Worker is a driver and if
    you want to run the driver in a manner equivalent to serial Python for
    debugging purposes. It will not send remote function calls to the scheduler
    and will insead execute them in a blocking fashion.

    args:
      mode: One of ray.SCRIPT_MODE, ray.WORKER_MODE, ray.SHELL_MODE, and
        ray.PYTHON_MODE.
    """
    self.mode = mode
    colorama.init()

  def put_object(self, objref, value):
    """Put value in the local object store with object reference objref.

    This assumes that the value for objref has not yet been placed in the
    local object store.

    Args:
      objref (ray.ObjRef): The object reference of the value to be put.
      value (serializable object): The value to put in the object store.
    """
    if serialization.is_arrow_serializable(value):
      ray.lib.put_arrow(self.handle, objref, value)
    else:
      object_capsule, contained_objrefs = serialization.serialize(self.handle, value) # contained_objrefs is a list of the objrefs contained in object_capsule
      ray.lib.put_object(self.handle, objref, object_capsule, contained_objrefs)

  def get_object(self, objref):
    """Get the value in the local object store associated with objref.

    Return the value from the local object store for objref. This will block
    until the value for objref has been written to the local object store.

    Args:
      objref (ray.ObjRef): The object reference of the value to retrieve.
    """
    if ray.lib.is_arrow(self.handle, objref):
      result, segmentid = ray.lib.get_arrow(self.handle, objref)
    else:
      object_capsule, segmentid = ray.lib.get_object(self.handle, objref)
      result = serialization.deserialize(self.handle, object_capsule)
    if isinstance(result, int):
      result = serialization.Int(result)
    elif isinstance(result, long):
      result = serialization.Long(result)
    elif isinstance(result, float):
      result = serialization.Float(result)
    elif isinstance(result, bool):
      ray.lib.unmap_object(self.handle, segmentid) # need to unmap here because result is passed back "by value" and we have no reference to unmap later
      return result # can't subclass bool, and don't need to because there is a global True/False
    elif isinstance(result, list):
      result = serialization.List(result)
    elif isinstance(result, dict):
      result = serialization.Dict(result)
    elif isinstance(result, tuple):
      result = serialization.Tuple(result)
    elif isinstance(result, str):
      result = serialization.Str(result)
    elif isinstance(result, np.ndarray):
      result = result.view(serialization.NDArray)
    elif isinstance(result, np.generic):
      return result
      # TODO(pcm): close the associated memory segment; if we don't, this leaks memory (but very little, so it is ok for now)
    elif result == None:
      ray.lib.unmap_object(self.handle, segmentid) # need to unmap here because result is passed back "by value" and we have no reference to unmap later
      return None # can't subclass None and don't need to because there is a global None
    result.ray_objref = objref # TODO(pcm): This could be done only for the "get" case in the future if we want to increase performance
    result.ray_deallocator = RayDealloc(self.handle, segmentid)
    return result

  def alias_objrefs(self, alias_objref, target_objref):
    """Make two object references refer to the same object."""
    ray.lib.alias_objrefs(self.handle, alias_objref, target_objref)

  def register_function(self, function):
    """Register a function with the scheduler.

    Notify the scheduler that this worker can execute the function with name
    func_name. After this call, the scheduler can send tasks for executing
    the function to this worker.

    Args:
      function (Callable): The remote function that this worker can execute.
    """
    ray.lib.register_function(self.handle, function.func_name, len(function.return_types))
    self.functions[function.func_name] = function

  def submit_task(self, func_name, args):
    """Submit a remote task to the scheduler.

    Tell the scheduler to schedule the execution of the function with name
    func_name with arguments args. Retrieve object references for the outputs of
    the function from the scheduler and immediately return them.

    Args:
      func_name (str): The name of the function to be executed.
      args (List[Any]): The arguments to pass into the function. Arguments can
        be object references or they can be values. If they are values, they
        must be serializable objecs.
    """
    task_capsule = serialization.serialize_task(self.handle, func_name, args)
    objrefs = ray.lib.submit_task(self.handle, task_capsule)
    if self.mode == ray.SHELL_MODE or self.mode == ray.SCRIPT_MODE:
      print_task_info(ray.lib.task_info(self.handle), self.mode)
    return objrefs

global_worker = Worker()
"""Worker: The global Worker object for this worker process.

We use a global Worker object to ensure that there is a single worker object
per worker process.
"""

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

def print_task_info(task_data, mode):
  """Print information about tasks.

  Args:
    task_data (Dict): A dictionary containing information about tasks that have
      failed, succeeded, or are still running.
    mode: The mode of the Worker object.
  """
  num_tasks_succeeded = task_data["num_succeeded"]
  num_tasks_in_progress = len(task_data["running_tasks"])
  num_tasks_failed = len(task_data["failed_tasks"])
  if num_tasks_failed > 0:
    for task_status in task_data["failed_tasks"]:
      print_failed_task(task_status)
    print "Error: {} task{} failed.".format(num_tasks_failed, "s" if num_tasks_failed > 1 else "")
  if mode == ray.SHELL_MODE:
    info_strings = []
    if num_tasks_succeeded > 0:
      info_strings.append("{}{} task{} succeeded{}".format(colorama.Fore.BLUE, num_tasks_succeeded, "s" if num_tasks_succeeded > 1 else "", colorama.Fore.RESET))
    if num_tasks_in_progress > 0:
      info_strings.append("{}{} task{} in progress{}".format(colorama.Fore.GREEN, num_tasks_in_progress, "s" if num_tasks_in_progress > 1 else "", colorama.Fore.RESET))
    if num_tasks_failed > 0:
      info_strings.append("{}{} task{} failed{}".format(colorama.Fore.RED, num_tasks_failed, "s" if num_tasks_failed > 1 else "", colorama.Fore.RESET))
    if len(info_strings) > 0:
      print ", ".join(info_strings)

def scheduler_info(worker=global_worker):
  """Return information about the state of the scheduler."""
  return ray.lib.scheduler_info(worker.handle)

def visualize_computation_graph(file_path=None, view=False, worker=global_worker):
  """Write the computation graph to a pdf file.

  Args:
    file_path (str): The name of a pdf file that the rendered computation graph
      will be written to. If this argument is None, a temporary path will be
      used.
    view (bool): If true, the result the python graphviz package will try to
      open the result in a viewer.

  Examples:
    In ray/scripts, call "python shell.py" and try the following code.

    >>> x = da.zeros([20, 20])
    >>> y = da.zeros([20, 20])
    >>> z = da.dot(x, y)
    >>> ray.visualize_computation_graph(view=True)
  """

  if file_path is None:
    file_path = ray.config.get_log_file_path("computation-graph.pdf")

  base_path, extension = os.path.splitext(file_path)
  if extension != ".pdf":
    raise Exception("File path must be a .pdf file")
  proto_path = base_path + ".binaryproto"

  ray.lib.dump_computation_graph(worker.handle, proto_path)
  graph = ray.internal.graph_pb2.CompGraph()
  graph.ParseFromString(open(proto_path).read())
  ray.graph.graph_to_graphviz(graph).render(base_path, view=view)

  print "Wrote graph dot description to file {}".format(base_path)
  print "Wrote graph protocol buffer description to file {}".format(proto_path)
  print "Wrote computation graph to file {}.pdf".format(base_path)

def task_info(worker=global_worker):
  """Return information about failed tasks."""
  return ray.lib.task_info(worker.handle)

def register_module(module, worker=global_worker):
  """Register each remote function in a module with the scheduler.

  This registers each remote function in the module with the scheduler, so tasks
  with those functions can be scheduled on this worker.

  args:
    module (module): The module of functions to register.
  """
  logging.info("registering functions in module {}.".format(module.__name__))
  for name in dir(module):
    val = getattr(module, name)
    if hasattr(val, "is_remote") and val.is_remote:
      logging.info("registering {}.".format(val.func_name))
      worker.register_function(val)

def connect(scheduler_address, objstore_address, worker_address, is_driver=False, worker=global_worker, mode=ray.WORKER_MODE):
  """Connect this worker to the scheduler and an object store.

  Args:
    scheduler_address (str): The ip address and port of the scheduler.
    objstore_address (str): The ip address and port of the local object store.
    worker_address (str): The ip address and port of this worker. The port can
      be chosen arbitrarily.
    is_driver (bool): True if this worker is a driver and false otherwise.
    mode: The mode of the worker. One of ray.SCRIPT_MODE, ray.WORKER_MODE,
      ray.SHELL_MODE, and ray.PYTHON_MODE.
  """
  if hasattr(worker, "handle"):
    del worker.handle
  worker.scheduler_address = scheduler_address
  worker.objstore_address = objstore_address
  worker.worker_address = worker_address
  worker.handle = ray.lib.create_worker(worker.scheduler_address, worker.objstore_address, worker.worker_address, is_driver)
  worker.set_mode(mode)
  FORMAT = "%(asctime)-15s %(message)s"
  logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename=ray.config.get_log_file_path("-".join(["worker", worker_address]) + ".log"))
  ray.lib.set_log_config(ray.config.get_log_file_path("-".join(["worker", worker_address, "c++"]) + ".log"))

def disconnect(worker=global_worker):
  """Disconnect this worker from the scheduler and object store."""
  if worker.handle is not None:
    ray.lib.disconnect(worker.handle)

def get(objref, worker=global_worker):
  """Get a remote object from an object store.

  This method blocks until the object corresponding to objref is available in
  the local object store. If this object is not in the local object store, it
  will be shipped from an object store that has it (once the object has been
  created).

  Args:
    objref (ray.ObjRef): Object reference to the object to get.

  Returns:
    A Python object
  """
  if worker.mode == ray.PYTHON_MODE:
    return objref # In ray.PYTHON_MODE, ray.get is the identity operation (the input will actually be a value not an objref)
  ray.lib.request_object(worker.handle, objref)
  if worker.mode == ray.SHELL_MODE or worker.mode == ray.SCRIPT_MODE:
    print_task_info(ray.lib.task_info(worker.handle), worker.mode)
  value = worker.get_object(objref)
  if isinstance(value, RayFailedObject):
    raise Exception("The task that created this object reference failed with error message:\n{}".format(value.error_message))
  return value

def put(value, worker=global_worker):
  """Store an object in the object store.

  Args:
    value (serializable object): The Python object to be stored.

  Returns:
    The object reference assigned to this value.
  """
  if worker.mode == ray.PYTHON_MODE:
    return value # In ray.PYTHON_MODE, ray.put is the identity operation
  objref = ray.lib.get_objref(worker.handle)
  worker.put_object(objref, value)
  if worker.mode == ray.SHELL_MODE or worker.mode == ray.SCRIPT_MODE:
    print_task_info(ray.lib.task_info(worker.handle), worker.mode)
  return objref

def kill_workers(worker=global_worker):
  """Kill all of the workers in the cluster. This does not kill drivers.

  Note:
    Currently, we only support killing workers if all submitted tasks have been
    run. If some workers are still running tasks or if the scheduler still has
    tasks in its queue, then this method will not do anything.

  Returns:
    True if workers were successfully killed. False otherwise.
  """
  success = ray.lib.kill_workers(worker.handle)
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

  This method takes an backtrace from an exception and makes it nicer by
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
  lines = [10 * " " + line for line in lines]
  return "\n".join(lines)

def main_loop(worker=global_worker):
  """The main loop a worker runs to receive and execute tasks.

  This method is an infinite loop. It waits to receive tasks from the scheduler.
  When it receives a task, it first deserializes the task. Then it retrieves the
  values for any arguments that were passed in as object references. Then it
  passes the arguments to the actual function. Then it stores the outputs of the
  function in the local object store. Then it notifies the scheduler that it
  completed the task.

  If the process of getting the arguments for execution (which does some type
  checking) or the process of executing the task fail, then the main loop will
  catch the exception and store RayFailedObject objects containing the relevant
  error messages in the object store in place of the actual outputs. These
  objects are used to propagate the error messages.
  """
  if not ray.lib.connected(worker.handle):
    raise Exception("Worker is attempting to enter main_loop but has not been connected yet.")
  ray.lib.start_worker_service(worker.handle)
  def process_task(task): # wrapping these lines in a function should cause the local variables to go out of scope more quickly, which is useful for inspecting reference counts
    func_name, args, return_objrefs = serialization.deserialize_task(worker.handle, task)
    try:
      arguments = get_arguments_for_execution(worker.functions[func_name], args, worker) # get args from objstore
      outputs = worker.functions[func_name].executor(arguments) # execute the function
      if len(return_objrefs) == 1:
        outputs = (outputs,)
    except Exception:
      exception_message = format_error_message(traceback.format_exc())
      # Here we are storing RayFailedObjects in the object store to indicate
      # failure (this is only interpreted by the worker).
      failure_objects = [RayFailedObject(exception_message) for _ in range(len(return_objrefs))]
      store_outputs_in_objstore(return_objrefs, failure_objects, worker)
      ray.lib.notify_task_completed(worker.handle, False, exception_message) # notify the scheduler that the task threw an exception
      logging.info("Worker threw exception with message: \n\n{}\n, while running function {}.".format(exception_message, func_name))
    else:
      store_outputs_in_objstore(return_objrefs, outputs, worker) # store output in local object store
      ray.lib.notify_task_completed(worker.handle, True, "") # notify the scheduler that the task completed successfully
  while True:
    task = ray.lib.wait_for_next_task(worker.handle)
    if task is None:
      # We use this as a mechanism to allow the scheduler to kill workers. When
      # the scheduler wants to kill a worker, it gives the worker a null task,
      # causing the worker program to exit the main loop here.
      break
    process_task(task)

def remote(arg_types, return_types, worker=global_worker):
  """This decorator is used to create remote functions.

  Args:
    arg_types (List[type]): List of Python types of the function arguments.
    return_types (List[type]): List of Python types of the return values.
  """
  def remote_decorator(func):
    def func_executor(arguments):
      """This gets run when the remote function is executed."""
      logging.info("Calling function {}".format(func.__name__))
      start_time = time.time()
      result = func(*arguments)
      end_time = time.time()
      check_return_values(func_call, result) # throws an exception if result is invalid
      logging.info("Finished executing function {}, it took {} seconds".format(func.__name__, end_time - start_time))
      return result
    def func_call(*args, **kwargs):
      """This gets run immediately when a worker calls a remote function."""
      args = list(args)
      args.extend([kwargs[keyword] if kwargs.has_key(keyword) else default for keyword, default in func_call.keyword_defaults[len(args):]]) # fill in the remaining arguments
      if worker.mode == ray.PYTHON_MODE:
        # In ray.PYTHON_MODE, remote calls simply execute the function. We copy
        # the arguments to prevent the function call from mutating them and to
        # match the usual behavior of immutable remote objects.
        return func(*copy.deepcopy(args))
      check_arguments(func_call, args) # throws an exception if args are invalid
      objrefs = worker.submit_task(func_call.func_name, args)
      if len(objrefs) == 1:
        return objrefs[0]
      elif len(objrefs) > 1:
        return objrefs
    func_call.func_name = "{}.{}".format(func.__module__, func.__name__)
    func_call.executor = func_executor
    func_call.arg_types = arg_types
    func_call.return_types = return_types
    func_call.is_remote = True
    func_call.sig_params = [(k, v) for k, v in funcsigs.signature(func).parameters.iteritems()]
    func_call.keyword_defaults = [(k, v.default) for k, v in func_call.sig_params]
    func_call.has_vararg_param = any([v.kind == v.VAR_POSITIONAL for k, v in func_call.sig_params])
    func_call.has_kwargs_param = any([v.kind == v.VAR_KEYWORD for k, v in func_call.sig_params])
    check_signature_supported(func_call)
    return func_call
  return remote_decorator

def check_signature_supported(function):
  """Check if we support the signature of this function.

  We currently do not allow remote functions to have **kwargs. We also do not
  support keyword argumens in conjunction with a *args argument.

  Args:
    function (Callable): The function to check.

  Raises:
    Exception: An exception is raised if the signature is not supported.
  """
  # check if the user specified kwargs
  if function.has_kwargs_param:
    raise "Function {} has a **kwargs argument, which is currently not supported.".format(function.__name__)
  # check if the user specified a variable number of arguments and any keyword arguments
  if function.has_vararg_param and any([d != funcsigs._empty for _, d in function.keyword_defaults]):
    raise "Function {} has a *args argument as well as a keyword argument, which is currently not supported.".format(function.__name__)


def check_return_values(function, result):
  """Check the types and number of return values.

  Args:
    function (Callable): The remote function whose outputs are being checked.
    result: The value returned by an invocation of the remote function. The
    expected types and number are defined in the remote decorator.

  Raises:
    Exception: An exception is raised if the return values have incorrect types
      or the function returned the wrong number of return values.
  """
  # If the @remote decorator declares that the function has no return values,
  # then all we do is check that there were in fact no return values.
  if len(function.return_types) == 0:
    if result is not None:
      raise Exception("The @remote decorator for function {} has 0 return values, but {} returned more than 0 values.".format(function.__name__, function.__name__))
    return
  # If a function has multiple return values, Python returns a tuple of the
  # values. If there is a single return value, then Python does not return a
  # tuple, it simply returns the value. That is why we place result with
  # (result,) when there is only one return value, so we can treat these two
  # cases similarly.
  if len(function.return_types) == 1:
    result = (result,)
  # Below we check that the number of values returned by the function match the
  # number of return values declared in the @remote decorator.
  if len(result) != len(function.return_types):
    raise Exception("The @remote decorator for function {} has {} return values with types {}, but {} returned {} values.".format(function.__name__, len(function.return_types), function.return_types, function.__name__, len(result)))
  # Here we do some limited type checking to make sure the return values have
  # the right types.
  for i in range(len(result)):
    if (not issubclass(type(result[i]), function.return_types[i])) and (not isinstance(result[i], ray.lib.ObjRef)):
      raise Exception("The {}th return value for function {} has type {}, but the @remote decorator expected a return value of type {} or an ObjRef.".format(i, function.__name__, type(result[i]), function.return_types[i]))

def typecheck_arg(arg, expected_type, i, function):
  """Check that an argument has the expected type.

  Args:
    arg: An argument to function.
    expected_type (type): The expected type of arg.
    i (int): The position of the argument to the function.
    function (Callable): The remote function whose argument is being checked.

  Raises:
    Exception: An exception is raised if arg does not have the expected type.
  """
  if issubclass(type(arg), expected_type):
    # Passed the type-checck
    # TODO(rkn): This check doesn't really work, e.g., issubclass(type([1, 2, 3]), typing.List[str]) == True
    pass
  elif isinstance(arg, long) and issubclass(int, expected_type):
    # TODO(mehrdadn): Should long really be convertible to int?
    pass
  else:
    raise Exception("Argument {} for function {} has type {} but an argument of type {} was expected.".format(i, function.__name__, type(arg), expected_type))

def check_arguments(function, args):
  """Check that the arguments to the remote function have the right types.

  This is called by the worker that calls the remote function (not the worker
  that executes the remote function).

  Args:
    function (Callable): The remote function whose arguments are being checked.
    args (List): The arguments to the function

  Raises:
    Exception: An exception is raised the args do not all have the right types.
  """
  # check the number of args
  if len(args) != len(function.arg_types) and not function.has_vararg_param:
    raise Exception("Function {} expects {} arguments, but received {}.".format(function.__name__, len(function.arg_types), len(args)))
  elif len(args) < len(function.arg_types) - 1 and function.has_vararg_param:
    raise Exception("Function {} expects at least {} arguments, but received {}.".format(function.__name__, len(function.arg_types) - 1, len(args)))

  for (i, arg) in enumerate(args):
    if i <= len(function.arg_types) - 1:
      expected_type = function.arg_types[i]
    elif function.has_vararg_param:
      expected_type = function.arg_types[-1]
    else:
      assert False, "This code should be unreachable."

    if isinstance(arg, ray.lib.ObjRef):
      # TODO(rkn): When we have type information in the ObjRef, do type checking here.
      pass
    else:
      typecheck_arg(arg, expected_type, i, function)

def get_arguments_for_execution(function, args, worker=global_worker):
  """Retrieve the arguments for the remote function.

  This retrieves the values for the arguments to the remote function that were
  passed in as object references. Argumens that were passed by value are not
  changed. This also does some type checking. This is called by the worker that
  is executing the remote function.

  Args:
    function (Callable): The remote function whose arguments are being
      retrieved.
    args (List): The arguments to the function.

  Returns:
    The retrieved arguments in addition to the arguments that were passed by
    value.

  Raises:
    Exception: An exception is raised the args do not all have the right types.
  """
  # TODO(rkn): Eventually, all of the type checking can be put in `check_arguments` above so that the error will happen immediately when calling a remote function.
  arguments = []
  # # check the number of args
  # if len(args) != len(function.arg_types) and function.arg_types[-1] is not None:
  #   raise Exception("Function {} expects {} arguments, but received {}.".format(function.__name__, len(function.arg_types), len(args)))
  # elif len(args) < len(function.arg_types) - 1 and function.arg_types[-1] is None:
  #   raise Exception("Function {} expects at least {} arguments, but received {}.".format(function.__name__, len(function.arg_types) - 1, len(args)))

  for (i, arg) in enumerate(args):
    if i <= len(function.arg_types) - 1:
      expected_type = function.arg_types[i]
    elif function.has_vararg_param and len(function.arg_types) >= 1:
      expected_type = function.arg_types[-1]
    else:
      assert False, "This code should be unreachable."

    if isinstance(arg, ray.lib.ObjRef):
      # get the object from the local object store
      logging.info("Getting argument {} for function {}.".format(i, function.__name__))
      argument = worker.get_object(arg)
      logging.info("Successfully retrieved argument {} for function {}.".format(i, function.__name__))
    else:
      # pass the argument by value
      argument = arg

    typecheck_arg(argument, expected_type, i, function)
    arguments.append(argument)
  return arguments

def store_outputs_in_objstore(objrefs, outputs, worker=global_worker):
  """Store the outputs of a remote function in the local object store.

  This stores the values that were returned by a remote function in the local
  object store. If any of the return values are object references, then these
  object references are aliased with the object references that the scheduler
  assigned for the return values. This is called by the worker that executes the
  remote function.

  Note:
    The arguments objrefs and outputs should have the same length.

  Args:
    objrefs (List[ray.ObjRef]): The object references that were assigned to the
      outputs of the remote function call.
    outputs (Tuple): The value returned by the remote function. If the remote
      function was supposed to only return one value, then its output was
      wrapped in a tuple with one element prior to being passed into this
      function.
  """
  for i in range(len(objrefs)):
    if isinstance(outputs[i], ray.lib.ObjRef):
      # An ObjRef is being returned, so we must alias objrefs[i] so that it refers to the same object that outputs[i] refers to
      logging.info("Aliasing objrefs {} and {}".format(objrefs[i].val, outputs[i].val))
      worker.alias_objrefs(objrefs[i], outputs[i])
      pass
    else:
      worker.put_object(objrefs[i], outputs[i])
