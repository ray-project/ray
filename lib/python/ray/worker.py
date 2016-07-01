import time
import datetime
import logging
import os
from types import ModuleType
import typing
import funcsigs
import numpy as np
import colorama
import copy

import ray
from ray.config import LOG_DIRECTORY, LOG_TIMESTAMP
import serialization
import ray.internal.graph_pb2
import ray.graph
import services

class RayFailedObject(object):
  """If a task throws an exception during execution, a RayFailedObject is stored in the object store for each of the tasks outputs."""

  def __init__(self, error_message=None):
    self.error_message = error_message

  def deserialize(self, primitives):
    self.error_message = primitives

  def serialize(self):
    return self.error_message

class RayDealloc(object):
  def __init__(self, handle, segmentid):
    self.handle = handle
    self.segmentid = segmentid

  def __del__(self):
    ray.lib.unmap_object(self.handle, self.segmentid)

class Worker(object):
  """The methods in this class are considered unexposed to the user. The functions outside of this class are considered exposed."""

  def __init__(self):
    self.functions = {}
    self.handle = None

  def set_mode(self, mode):
    self.mode = mode
    colorama.init()

  def put_object(self, objref, value):
    """Put `value` in the local object store with objref `objref`. This assumes that the value for `objref` has not yet been placed in the local object store."""
    if serialization.is_arrow_serializable(value):
      ray.lib.put_arrow(self.handle, objref, value)
    else:
      object_capsule, contained_objrefs = serialization.serialize(self.handle, value) # contained_objrefs is a list of the objrefs contained in object_capsule
      ray.lib.put_object(self.handle, objref, object_capsule, contained_objrefs)

  def get_object(self, objref):
    """
    Return the value from the local object store for objref `objref`. This will
    block until the value for `objref` has been written to the local object store.

    WARNING: get_object can only be called on a canonical objref.
    """
    if ray.lib.is_arrow(self.handle, objref):
      result, segmentid = ray.lib.get_arrow(self.handle, objref)
    else:
      object_capsule, segmentid = ray.lib.get_object(self.handle, objref)
      result = serialization.deserialize(self.handle, object_capsule)
    if isinstance(result, int):
      result = serialization.Int(result)
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
    """Make `alias_objref` refer to the same object that `target_objref` refers to."""
    ray.lib.alias_objrefs(self.handle, alias_objref, target_objref)

  def register_function(self, function):
    """Notify the scheduler that this worker can execute the function with name `func_name`. Store the function `function` locally."""
    ray.lib.register_function(self.handle, function.func_name, len(function.return_types))
    self.functions[function.func_name] = function

  def submit_task(self, func_name, args):
    """Tell the scheduler to schedule the execution of the function with name `func_name` with arguments `args`. Retrieve object references for the outputs of the function from the scheduler and immediately return them."""
    task_capsule = serialization.serialize_task(self.handle, func_name, args)
    objrefs = ray.lib.submit_task(self.handle, task_capsule)
    if self.mode == ray.SHELL_MODE or self.mode == ray.SCRIPT_MODE:
      print_task_info(ray.lib.task_info(self.handle), self.mode)
    return objrefs

# We make `global_worker` a global variable so that there is one worker per worker process.
global_worker = Worker()

# This is a helper method. It should not be called by users.
def print_failed_task(task_status):
  print """
    Error: Task failed
      Function Name: {}
      Task ID: {}
      Error Message: {}
  """.format(task_status["function_name"], task_status["operationid"], task_status["error_message"])

# This is a helper method. It should not be called by users.
def print_task_info(task_data, mode):
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
  return ray.lib.scheduler_info(worker.handle);

def visualize_computation_graph(file_path=None, view=False, worker=global_worker):
  """
  Write the computation graph to a pdf file.

  Args:
    file_path: A .pdf file that the rendered computation graph will be written to

    view: If true, the result the python graphviz package will try to open the
      result in a viewer

  Example:
    In ray/scripts, call "python shell.py" and paste in the following code.

    x = da.zeros([20, 20])
    y = da.zeros([20, 20])
    z = da.dot(x, y)

    ray.visualize_computation_graph("computation_graph.pdf")
  """

  if file_path is None:
    file_path = os.path.join(ray.config.LOG_DIRECTORY, (ray.config.LOG_TIMESTAMP + "-computation-graph.pdf").format(datetime.datetime.now()))

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
  """Tell the scheduler to return task information. Currently includes a list of all failed tasks since the start of the cluster."""
  return ray.lib.task_info(worker.handle);

def register_module(module, recursive=False, worker=global_worker):
  logging.info("registering functions in module {}.".format(module.__name__))
  for name in dir(module):
    val = getattr(module, name)
    if hasattr(val, "is_remote") and val.is_remote:
      logging.info("registering {}.".format(val.func_name))
      worker.register_function(val)
    # elif recursive and isinstance(val, ModuleType):
    #   register_module(val, recursive, worker)

def connect(scheduler_address, objstore_address, worker_address, is_driver=False, worker=global_worker, mode=ray.WORKER_MODE):
  if hasattr(worker, "handle"):
    del worker.handle
  worker.scheduler_address = scheduler_address
  worker.objstore_address = objstore_address
  worker.worker_address = worker_address
  worker.handle = ray.lib.create_worker(worker.scheduler_address, worker.objstore_address, worker.worker_address, is_driver)
  worker.set_mode(mode)
  FORMAT = "%(asctime)-15s %(message)s"
  log_basename = os.path.join(LOG_DIRECTORY, (LOG_TIMESTAMP + "-worker-{}").format(datetime.datetime.now(), worker_address))
  logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename=log_basename + ".log")
  ray.lib.set_log_config(log_basename + "-c++.log")

def disconnect(worker=global_worker):
  ray.lib.disconnect(worker.handle)

def get(objref, worker=global_worker):
  if worker.mode == ray.PYTHON_MODE:
    return objref # In ray.PYTHON_MODE, ray.get is the identity operation (the input will actually be a value not an objref)
  ray.lib.request_object(worker.handle, objref)
  if worker.mode == ray.SHELL_MODE or worker.mode == ray.SCRIPT_MODE:
    print_task_info(ray.lib.task_info(worker.handle), worker.mode)
  value = worker.get_object(objref)
  if isinstance(value, RayFailedObject):
    raise Exception("The task that created this object reference failed with error message: {}".format(value.error_message))
  return value

def put(value, worker=global_worker):
  if worker.mode == ray.PYTHON_MODE:
    return value # In ray.PYTHON_MODE, ray.put is the identity operation
  objref = ray.lib.get_objref(worker.handle)
  worker.put_object(objref, value)
  if worker.mode == ray.SHELL_MODE or worker.mode == ray.SCRIPT_MODE:
    print_task_info(ray.lib.task_info(worker.handle), worker.mode)
  return objref

def kill_workers(worker=global_worker):
  """
  This method kills all of the workers in the cluster. It does not kill drivers.
  """
  success = ray.lib.kill_workers(worker.handle)
  if not success:
    print "Could not kill all workers; check that there are no tasks currently running."
  return success

def restart_workers_local(num_workers, worker_path, worker=global_worker):
  """
  This method kills all of the workers and starts new workers locally on the
    same node as the driver. This is intended for use in the case where Ray is
    being used on a single node.

  :param num_workers: the number of workers to be started
  :param worker_path: path of the source code that will be run on the worker
  """
  if not kill_workers(worker):
    return False
  services.start_workers(worker.scheduler_address, worker.objstore_address, num_workers, worker_path)

def main_loop(worker=global_worker):
  if not ray.lib.connected(worker.handle):
    raise Exception("Worker is attempting to enter main_loop but has not been connected yet.")
  ray.lib.start_worker_service(worker.handle)
  def process_task(task): # wrapping these lines in a function should cause the local variables to go out of scope more quickly, which is useful for inspecting reference counts
    func_name, args, return_objrefs = serialization.deserialize_task(worker.handle, task)
    arguments = get_arguments_for_execution(worker.functions[func_name], args, worker) # get args from objstore
    try:
      outputs = worker.functions[func_name].executor(arguments) # execute the function
      if len(return_objrefs) == 1:
        outputs = (outputs,)
    except Exception as e:
      # Here we are storing RayFailedObjects in the object store to indicate
      # failure (this is only interpreted by the worker).
      failure_objects = [RayFailedObject(str(e)) for _ in range(len(return_objrefs))]
      store_outputs_in_objstore(return_objrefs, failure_objects, worker)
      ray.lib.notify_task_completed(worker.handle, False, str(e)) # notify the scheduler that the task threw an exception
      logging.info("Worker through exception with message: {}, while running function {}.".format(str(e), func_name))
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
  def remote_decorator(func):
    def func_executor(arguments):
      """This is what gets executed remotely on a worker after a remote function is scheduled by the scheduler."""
      logging.info("Calling function {}".format(func.__name__))
      start_time = time.time()
      result = func(*arguments)
      end_time = time.time()
      check_return_values(func_call, result) # throws an exception if result is invalid
      logging.info("Finished executing function {}, it took {} seconds".format(func.__name__, end_time - start_time))
      return result
    def func_call(*args, **kwargs):
      """This is what gets run immediately when a worker calls a remote function."""
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

# helper method, this should not be called by the user
# we currently do not support the functionality that we test for in this method,
# but in the future we could
def check_signature_supported(function):
  # check if the user specified kwargs
  if function.has_kwargs_param:
    raise "Function {} has a **kwargs argument, which is currently not supported.".format(function.__name__)
  # check if the user specified a variable number of arguments and any keyword arguments
  if function.has_vararg_param and any([d != funcsigs._empty for k, d in function.keyword_defaults]):
    raise "Function {} has a *args argument as well as a keyword argument, which is currently not supported.".format(function.__name__)


# helper method, this should not be called by the user
def check_return_values(function, result):
  if len(function.return_types) == 1:
    result = (result,)
    # if not isinstance(result, function.return_types[0]):
    #   raise Exception("The @remote decorator for function {} expects one return value with type {}, but {} returned a {}.".format(function.__name__, function.return_types[0], function.__name__, type(result)))
  else:
    if len(result) != len(function.return_types):
      raise Exception("The @remote decorator for function {} has {} return values with types {}, but {} returned {} values.".format(function.__name__, len(function.return_types), function.return_types, function.__name__, len(result)))
    for i in range(len(result)):
      if (not issubclass(type(result[i]), function.return_types[i])) and (not isinstance(result[i], ray.lib.ObjRef)):
        raise Exception("The {}th return value for function {} has type {}, but the @remote decorator expected a return value of type {} or an ObjRef.".format(i, function.__name__, type(result[i]), function.return_types[i]))

# helper method, this should not be called by the user
def check_arguments(function, args):
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
      if not issubclass(type(arg), expected_type): # TODO(rkn): This check doesn't really work, e.g., issubclass(type([1, 2, 3]), typing.List[str]) == True
        raise Exception("Argument {} for function {} has type {} but an argument of type {} was expected.".format(i, function.__name__, type(arg), expected_type))

# helper method, this should not be called by the user
def get_arguments_for_execution(function, args, worker=global_worker):
  # TODO(rkn): Eventually, all of the type checking can be put in `check_arguments` above so that the error will happen immediately when calling a remote function.
  arguments = []
  """
  # check the number of args
  if len(args) != len(function.arg_types) and function.arg_types[-1] is not None:
    raise Exception("Function {} expects {} arguments, but received {}.".format(function.__name__, len(function.arg_types), len(args)))
  elif len(args) < len(function.arg_types) - 1 and function.arg_types[-1] is None:
    raise Exception("Function {} expects at least {} arguments, but received {}.".format(function.__name__, len(function.arg_types) - 1, len(args)))
  """

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

    if not issubclass(type(argument), expected_type):
      raise Exception("Argument {} for function {} has type {} but an argument of type {} was expected.".format(i, function.__name__, type(argument), expected_type))
    arguments.append(argument)
  return arguments

# helper method, this should not be called by the user
def store_outputs_in_objstore(objrefs, outputs, worker=global_worker):
  for i in range(len(objrefs)):
    if isinstance(outputs[i], ray.lib.ObjRef):
      # An ObjRef is being returned, so we must alias objrefs[i] so that it refers to the same object that outputs[i] refers to
      logging.info("Aliasing objrefs {} and {}".format(objrefs[i].val, outputs[i].val))
      worker.alias_objrefs(objrefs[i], outputs[i])
      pass
    else:
      worker.put_object(objrefs[i], outputs[i])
