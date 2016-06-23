import time
import datetime
import logging
import os
from types import ModuleType
import typing
import funcsigs
import numpy as np
import pynumbuf
import colorama

import ray
from ray.config import LOG_DIRECTORY, LOG_TIMESTAMP
import serialization

class RayDealloc(object):
  def __init__(self, handle, segmentid):
    self.handle = handle
    self.segmentid = segmentid

  def __del__(self):
    # TODO(pcm): This will be used to free the segment
    pass

class Worker(object):
  """The methods in this class are considered unexposed to the user. The functions outside of this class are considered exposed."""

  def __init__(self):
    self.functions = {}
    self.handle = None

  def set_print_task_info(self, print_task_info):
    self.print_task_info = print_task_info
    colorama.init()

  def put_object(self, objref, value):
    """Put `value` in the local object store with objref `objref`. This assumes that the value for `objref` has not yet been placed in the local object store."""
    if pynumbuf.serializable(value):
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
      return result # can't subclass bool, and don't need to because there is a global True/False
      # TODO(pcm): close the associated memory segment; if we don't, this leaks memory (but very little, so it is ok for now)
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
    elif result == None:
      return None # can't subclass None and don't need to because there is a global None
      # TODO(pcm): close the associated memory segment; if we don't, this leaks memory (but very little, so it is ok for now)
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
    if self.print_task_info:
      print_task_info(ray.lib.task_info(self.handle))
    return objrefs

# We make `global_worker` a global variable so that there is one worker per worker process.
global_worker = Worker()

# This is a helper method. It should not be called by users.
def print_task_info(task_data):
  num_tasks_succeeded = task_data["num_succeeded"]
  num_tasks_in_progress = len(task_data["running_tasks"])
  num_tasks_failed = len(task_data["failed_tasks"])
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

def connect(scheduler_addr, objstore_addr, worker_addr, worker=global_worker, print_task_info=False):
  if hasattr(worker, "handle"):
    del worker.handle
  worker.handle = ray.lib.create_worker(scheduler_addr, objstore_addr, worker_addr)
  FORMAT = "%(asctime)-15s %(message)s"
  log_basename = os.path.join(LOG_DIRECTORY, (LOG_TIMESTAMP + "-worker-{}").format(datetime.datetime.now(), worker_addr))
  logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename=log_basename + ".log")
  ray.lib.set_log_config(log_basename + "-c++.log")
  worker.set_print_task_info(print_task_info)

def disconnect(worker=global_worker):
  ray.lib.disconnect(worker.handle)

def get(objref, worker=global_worker):
  ray.lib.request_object(worker.handle, objref)
  if worker.print_task_info:
    print_task_info(ray.lib.task_info(worker.handle))
  return worker.get_object(objref)

def put(value, worker=global_worker):
  objref = ray.lib.get_objref(worker.handle)
  worker.put_object(objref, value)
  if worker.print_task_info:
    print_task_info(ray.lib.task_info(worker.handle))
  return objref

def main_loop(worker=global_worker):
  if not ray.lib.connected(worker.handle):
    raise Exception("Worker is attempting to enter main_loop but has not been connected yet.")
  ray.lib.start_worker_service(worker.handle)
  def process_task(task): # wrapping these lines in a function should cause the local variables to go out of scope more quickly, which is useful for inspecting reference counts
    func_name, args, return_objrefs = serialization.deserialize_task(worker.handle, task)
    arguments = get_arguments_for_execution(worker.functions[func_name], args, worker) # get args from objstore
    try:
      outputs = worker.functions[func_name].executor(arguments) # execute the function
    except Exception as e:
      ray.lib.notify_task_completed(worker.handle, False, str(e)) # notify the scheduler that the task threw an exception
    else:
      store_outputs_in_objstore(return_objrefs, outputs, worker) # store output in local object store
      ray.lib.notify_task_completed(worker.handle, True, "") # notify the scheduler that the task completed successfully
  while True:
    task = ray.lib.wait_for_next_task(worker.handle)
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
      check_arguments(func_call, args) # throws an exception if args are invalid
      objrefs = worker.submit_task(func_call.func_name, args)
      return objrefs[0] if len(objrefs) == 1 else objrefs
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
  if len(objrefs) == 1:
    outputs = (outputs,)

  for i in range(len(objrefs)):
    if isinstance(outputs[i], ray.lib.ObjRef):
      # An ObjRef is being returned, so we must alias objrefs[i] so that it refers to the same object that outputs[i] refers to
      logging.info("Aliasing objrefs {} and {}".format(objrefs[i].val, outputs[i].val))
      worker.alias_objrefs(objrefs[i], outputs[i])
      pass
    else:
      worker.put_object(objrefs[i], outputs[i])
