import typing

import orchpy

class Worker(object):
  """The methods in this class are considered unexposed to the user. The functions outside of this class are considered exposed."""

  def __init__(self):
    self.functions = {}
    self.connected = False
    self.handle = None

  def put_object(self, objref, value):
    """Put `value` in the local object store with objref `objref`. This assumes that the value for `objref` has not yet been placed in the local object store."""
    object_capsule = orchpy.lib.serialize_object(value)
    orchpy.lib.put_object(self.handle, objref, object_capsule)

  def get_object(self, objref):
    """Return the value from the local object store for objref `objref`. This will block until the value for `objref` has been written to the local object store."""
    object_capsule = orchpy.lib.get_object(self.handle, objref)
    return orchpy.lib.deserialize_object(object_capsule)

  def register_function(self, function):
    """Notify the scheduler that this worker can execute the function with name `func_name`. Store the function `function` locally."""
    orchpy.lib.register_function(self.handle, function.func_name, len(function.return_types))
    self.functions[function.func_name] = function

  def remote_call(self, func_name, args):
    """Tell the scheduler to schedule the execution of the function with name `func_name` with arguments `args`. Retrieve object references for the outputs of the function from the scheduler and immediately return them."""
    call_capsule = orchpy.lib.serialize_call(func_name, args)
    return orchpy.lib.remote_call(self.handle, call_capsule)

# We make `global_worker` a global variable so that there is one worker per worker process.
global_worker = Worker()

def connect(scheduler_addr, objstore_addr, worker_addr, worker=global_worker):
  if worker.connected:
    raise Exception("Worker called connect, but worker is already connected")
  worker.handle = orchpy.lib.create_worker(scheduler_addr, objstore_addr, worker_addr)
  worker.connected = True

def pull(objref, worker=global_worker):
  object_capsule = orchpy.lib.pull_object(worker.handle, objref)
  return orchpy.lib.deserialize_object(object_capsule)

def push(value, worker=global_worker):
  object_capsule = orchpy.lib.serialize_object(value)
  return orchpy.lib.push_object(worker.handle, object_capsule)

def main_loop(worker=global_worker):
  if not worker.connected:
    raise Exception("Worker is attempting to enter main_loop but has not been connected yet.")
  orchpy.lib.start_worker_service(worker.handle)
  while True:
    call = orchpy.lib.wait_for_next_task(worker.handle)
    func_name, args, return_objrefs = orchpy.lib.deserialize_call(call)
    arguments = get_arguments_for_execution(worker.functions[func_name], args, worker) # get args from objstore
    outputs = worker.functions[func_name].executor(arguments) # execute the function
    store_outputs_in_objstore(return_objrefs, outputs, worker) # store output in local object store
    # TODO(rkn): notify the scheduler that the task has completed, orchpy.lib.notify_task_completed(worker.handle)

def distributed(arg_types, return_types, worker=global_worker):
  def distributed_decorator(func):
    def func_executor(arguments):
      """This is what gets executed remotely on a worker after a distributed function is scheduled by the scheduler."""
      print "Calling function {} with arguments {}".format(func.__name__, arguments)
      result = func(*arguments)
      if len(return_types) != 1 and len(result) != len(return_types):
        raise Exception("The @distributed decorator for function {} has {} return values with types {}, but {} returned {} values.".format(func.__name__, len(return_types), return_types, func.__name__, len(result)))
      return result
    def func_call(*args):
      """This is what gets run immediately when a worker calls a distributed function."""
      # TODO(rkn): check types
      return worker.remote_call(func_call.func_name, list(args))
    func_call.func_name = "{}.{}".format(func.__module__, func.__name__)
    func_call.executor = func_executor
    func_call.arg_types = arg_types
    func_call.return_types = return_types
    return func_call
  return distributed_decorator

# helper method, this should not be called by the user
def get_arguments_for_execution(function, args, worker=global_worker):
  arguments = []
  # check the number of args
  if len(args) != len(function.types) and function.types[-1] is not None:
    raise Exception("Function {} expects {} arguments, but received {}.".format(function.__name__, len(function.types), len(args)))
  elif len(args) < len(function.types) - 1 and function.types[-1] is None:
    raise Exception("Function {} expects at least {} arguments, but received {}.".format(function.__name__, len(function.types) - 1, len(args)))

  for (i, arg) in enumerate(args):
    print "Pulling argument {} for function {}.".format(i, function.__name__)
    if i < len(function.types) - 1:
      expected_type = function.types[i]
    elif i == len(function.types) - 1 and function.types[-1] is not None:
      expected_type = function.types[-1]
    elif function.types[-1] is None and len(function.types > 1):
      expected_type = function.types[-2]
    else:
      assert False, "This code should be unreachable."

    argument = worker.get_object(arg) if type(arg) == orchpy.ObjRef else arg
    if type(arg) == orchpy.ObjRef:
      # get the object from the local object store
      # TODO(rkn): Do we know that it is already there? Maybe we should call pull(arg, worker).
      argument = worker.get_object(arg)
    else:
      # pass the argument by value
      argument = arg

    if expected_type != type(argument):
      raise Exception("Argument {} for function {} has type {} but an argument of type {} was expected.".format(i, function.__name__, type(argument), arg_type))
    arguments.append(argument)
  return arguments

# helper method, this should not be called by the user
def store_outputs_in_objstore(objrefs, outputs, worker=global_worker):
  if len(objrefs) == 1:
    worker.put_object(objrefs[0], outputs)
  else:
    for i in range(len(objrefs)):
      worker.put_object(objrefs[i], outputs[i])
