from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import inspect
import json
import numpy as np
import random
import redis
import traceback

import ray.local_scheduler
import ray.pickling as pickling
import ray.signature as signature
import ray.worker
import ray.experimental.state as state

# This is a variable used by each actor to indicate the IDs of the GPUs that
# the worker is currently allowed to use.
gpu_ids = []


def get_gpu_ids():
  """Get the IDs of the GPU that are available to the worker.

  Each ID is an integer in the range [0, NUM_GPUS - 1], where NUM_GPUS is the
  number of GPUs that the node has.
  """
  return gpu_ids


def random_string():
  return np.random.bytes(20)


def random_actor_id():
  return ray.local_scheduler.ObjectID(random_string())


def get_actor_method_function_id(attr):
  """Get the function ID corresponding to an actor method.

  Args:
    attr (str): The attribute name of the method.

  Returns:
    Function ID corresponding to the method.
  """
  function_id_hash = hashlib.sha1()
  function_id_hash.update(attr.encode("ascii"))
  function_id = function_id_hash.digest()
  assert len(function_id) == 20
  return ray.local_scheduler.ObjectID(function_id)


def fetch_and_register_actor(key, worker):
  """Import an actor."""
  (driver_id, actor_id_str, actor_name,
   module, pickled_class, assigned_gpu_ids,
   actor_method_names) = worker.redis_client.hmget(
       key, ["driver_id", "actor_id", "name", "module", "class", "gpu_ids",
             "actor_method_names"])
  actor_id = ray.local_scheduler.ObjectID(actor_id_str)
  actor_name = actor_name.decode("ascii")
  module = module.decode("ascii")
  actor_method_names = json.loads(actor_method_names.decode("ascii"))
  global gpu_ids
  gpu_ids = json.loads(assigned_gpu_ids.decode("ascii"))

  # Create a temporary actor with some temporary methods so that if the actor
  # fails to be unpickled, the temporary actor can be used (just to produce
  # error messages and to prevent the driver from hanging).
  class TemporaryActor(object):
    pass
  worker.actors[actor_id_str] = TemporaryActor()

  def temporary_actor_method(*xs):
    raise Exception("The actor with name {} failed to be imported, and so "
                    "cannot execute this method".format(actor_name))
  for actor_method_name in actor_method_names:
    function_id = get_actor_method_function_id(actor_method_name).id()
    worker.functions[driver_id][function_id] = (actor_method_name,
                                                temporary_actor_method)

  try:
    unpickled_class = pickling.loads(pickled_class)
  except Exception:
    # If an exception was thrown when the actor was imported, we record the
    # traceback and notify the scheduler of the failure.
    traceback_str = ray.worker.format_error_message(traceback.format_exc())
    # Log the error message.
    worker.push_error_to_driver(driver_id, "register_actor", traceback_str,
                                data={"actor_id": actor_id.id()})
  else:
    # TODO(pcm): Why is the below line necessary?
    unpickled_class.__module__ = module
    worker.actors[actor_id_str] = unpickled_class.__new__(unpickled_class)
    for (k, v) in inspect.getmembers(
        unpickled_class, predicate=(lambda x: (inspect.isfunction(x) or
                                               inspect.ismethod(x)))):
      function_id = get_actor_method_function_id(k).id()
      worker.functions[driver_id][function_id] = (k, v)
      # We do not set worker.function_properties[driver_id][function_id]
      # because we currently do need the actor worker to submit new tasks for
      # the actor.


def attempt_to_reserve_gpus(num_gpus, driver_id, local_scheduler, worker):
  """Attempt to acquire GPUs on a particular local scheduler for an actor.

  Args:
    num_gpus: The number of GPUs to acquire.
    driver_id: The ID of the driver responsible for creating the actor.
    local_scheduler: Information about the local scheduler.

  Returns:
    A list of the GPU IDs that were successfully acquired. This should have
      length either equal to num_gpus or equal to 0.
  """
  local_scheduler_id = local_scheduler[b"ray_client_id"]
  local_scheduler_total_gpus = int(float(
      local_scheduler[b"num_gpus"].decode("ascii")))

  gpus_to_acquire = []

  # Attempt to acquire GPU IDs atomically.
  with worker.redis_client.pipeline() as pipe:
    while True:
      try:
        # If this key is changed before the transaction below (the multi/exec
        # block), then the transaction will not take place.
        pipe.watch(local_scheduler_id)

        result = worker.redis_client.hget(local_scheduler_id, "gpus_in_use")
        gpus_in_use = dict() if result is None else json.loads(result)
        print("gpus_in_use before:", gpus_in_use)
        all_gpu_ids_in_use = []
        for key in gpus_in_use:
          all_gpu_ids_in_use += gpus_in_use[key]
        print("all_gpu_ids_in_use", all_gpu_ids_in_use)

        assert len(all_gpu_ids_in_use) <= local_scheduler_total_gpus
        assert len(set(all_gpu_ids_in_use)) == len(all_gpu_ids_in_use)

        pipe.multi()

        if local_scheduler_total_gpus - len(all_gpu_ids_in_use) >= num_gpus:
          # There are enough available GPUs, so try to reserve some.
          all_gpu_ids = set(range(local_scheduler_total_gpus))
          for gpu_id in all_gpu_ids_in_use:
            all_gpu_ids.remove(gpu_id)
          gpus_to_acquire = list(all_gpu_ids)[:num_gpus]

          print("gpus_to_acquire", gpus_to_acquire)

          driver_id_hex = ray.experimental.state.binary_to_hex(driver_id)
          if driver_id_hex not in gpus_in_use:
            gpus_in_use[driver_id_hex] = []
          gpus_in_use[driver_id_hex] += gpus_to_acquire

          print("gpus_in_use after:", gpus_in_use)
          # Stick the updated GPU IDs back in Redis
          pipe.hset(local_scheduler_id, "gpus_in_use", json.dumps(gpus_in_use))

        pipe.execute()
        # If a WatchError is not raise, then the operations should have gone
        # through atomically.
        break
      except redis.WatchError:
        # Another client must have changed the watched key between the time we
        # started WATCHing it and the pipeline's execution. We should just
        # retry.
        print("TRANSACTION FAILED")
        gpus_to_acquire = []
        continue

  return gpus_to_acquire


def select_local_scheduler(local_schedulers, num_gpus, worker):
  """Select a local scheduler to assign this actor to.

  Args:
    local_schedulers: A list of dictionaries of information about the local
      schedulers.
    num_gpus (int): The number of GPUs that must be reserved for this actor.

  Returns:
    A tuple of the ID of the local scheduler that has been chosen and a list of
      the gpu_ids that are reserved for the actor.

  Raises:
    Exception: An exception is raised if no local scheduler can be found with
      sufficient resources.
  """
  driver_id = worker.task_driver_id.id()

  if num_gpus == 0:
    local_scheduler_id = random.choice(local_schedulers)[b"ray_client_id"]
    gpus_aquired = []
  else:
    # All of this logic is for finding a local scheduler that has enough
    # available GPUs.
    local_scheduler_id = None
    # Loop through all of the local schedulers.
    for local_scheduler in local_schedulers:
      # Try to reserve enough GPUs on this local scheduler.
      gpus_aquired = attempt_to_reserve_gpus(num_gpus, driver_id,
                                             local_scheduler, worker)
      if len(gpus_aquired) == num_gpus:
        local_scheduler_id = local_scheduler[b"ray_client_id"]
        break
      else:
        # We should have either acquired as many GPUs as we need or none.
        assert len(gpus_aquired) == 0

    if local_scheduler_id is None:
      raise Exception("Could not find a node with enough GPUs to create this "
                      "actor. The local scheduler information is {}."
                      .format(local_schedulers))
  return local_scheduler_id, gpus_aquired


def export_actor(actor_id, Class, actor_method_names, num_cpus, num_gpus,
                 worker):
  """Export an actor to redis.

  Args:
    actor_id: The ID of the actor.
    Class: Name of the class to be exported as an actor.
    actor_method_names (list): A list of the names of this actor's methods.
    num_cpus (int): The number of CPUs that this actor requires.
    num_gpus (int): The number of GPUs that this actor requires.
  """
  ray.worker.check_main_thread()
  if worker.mode is None:
    raise NotImplemented("TODO(pcm): Cache actors")
  key = "Actor:{}".format(actor_id.id())
  pickled_class = pickling.dumps(Class)

  # For now, all actor methods have 1 return value.
  driver_id = worker.task_driver_id.id()
  for actor_method_name in actor_method_names:
    function_id = get_actor_method_function_id(actor_method_name).id()
    worker.function_properties[driver_id][function_id] = (1, num_cpus,
                                                          num_gpus)

  # Select a local scheduler for the actor.
  local_schedulers = state.get_local_schedulers(worker.redis_client)
  local_scheduler_id, gpu_ids = select_local_scheduler(local_schedulers,
                                                       num_gpus, worker)

  worker.redis_client.publish("actor_notifications",
                              actor_id.id() + driver_id + local_scheduler_id)

  d = {"driver_id": driver_id,
       "actor_id": actor_id.id(),
       "name": Class.__name__,
       "module": Class.__module__,
       "class": pickled_class,
       "gpu_ids": json.dumps(gpu_ids),
       "actor_method_names": json.dumps(list(actor_method_names))}
  worker.redis_client.hmset(key, d)
  worker.redis_client.rpush("Exports", key)


def actor(*args, **kwargs):
  def make_actor_decorator(num_cpus=1, num_gpus=0):
    def make_actor(Class):
      # The function actor_method_call gets called if somebody tries to call a
      # method on their local actor stub object.
      def actor_method_call(actor_id, attr, function_signature, *args,
                            **kwargs):
        ray.worker.check_connected()
        ray.worker.check_main_thread()
        args = signature.extend_args(function_signature, args, kwargs)

        function_id = get_actor_method_function_id(attr)
        # TODO(pcm): Extend args with keyword args.
        object_ids = ray.worker.global_worker.submit_task(function_id, "",
                                                          args,
                                                          actor_id=actor_id)
        if len(object_ids) == 1:
          return object_ids[0]
        elif len(object_ids) > 1:
          return object_ids

      class NewClass(object):
        def __init__(self, *args, **kwargs):
          self._ray_actor_id = random_actor_id()
          self._ray_actor_methods = {
              k: v for (k, v) in inspect.getmembers(
                  Class, predicate=(lambda x: (inspect.isfunction(x) or
                                               inspect.ismethod(x))))}
          # Extract the signatures of each of the methods. This will be used to
          # catch some errors if the methods are called with inappropriate
          # arguments.
          self._ray_method_signatures = dict()
          for k, v in self._ray_actor_methods.items():
            # Print a warning message if the method signature is not supported.
            # We don't raise an exception because if the actor inherits from a
            # class that has a method whose signature we don't support, we
            # there may not be much the user can do about it.
            signature.check_signature_supported(v, warn=True)
            self._ray_method_signatures[k] = signature.extract_signature(
                v, ignore_first=True)

          export_actor(self._ray_actor_id, Class,
                       self._ray_actor_methods.keys(), num_cpus, num_gpus,
                       ray.worker.global_worker)
          # Call __init__ as a remote function.
          if "__init__" in self._ray_actor_methods.keys():
            actor_method_call(self._ray_actor_id, "__init__",
                              self._ray_method_signatures["__init__"],
                              *args, **kwargs)
          else:
            print("WARNING: this object has no __init__ method.")

        # Make tab completion work.
        def __dir__(self):
          return self._ray_actor_methods

        def __getattribute__(self, attr):
          # The following is needed so we can still access self.actor_methods.
          if attr in ["_ray_actor_id", "_ray_actor_methods",
                      "_ray_method_signatures"]:
            return super(NewClass, self).__getattribute__(attr)
          if attr in self._ray_actor_methods.keys():
            return lambda *args, **kwargs: actor_method_call(
                self._ray_actor_id, attr, self._ray_method_signatures[attr],
                *args, **kwargs)
          # There is no method with this name, so raise an exception.
          raise AttributeError("'{}' Actor object has no attribute '{}'"
                               .format(Class, attr))

        def __repr__(self):
          return "Actor(" + self._ray_actor_id.hex() + ")"

      return NewClass
    return make_actor

  if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
    # In this case, the actor decorator was applied directly to a class
    # definition.
    Class = args[0]
    return make_actor_decorator(num_cpus=1, num_gpus=0)(Class)

  # In this case, the actor decorator is something like @ray.actor(num_gpus=1).
  if len(args) == 0 and len(kwargs) > 0 and all([key
                                                 in ["num_cpus", "num_gpus"]
                                                 for key in kwargs.keys()]):
    num_cpus = kwargs["num_cpus"] if "num_cpus" in kwargs.keys() else 1
    num_gpus = kwargs["num_gpus"] if "num_gpus" in kwargs.keys() else 0
    return make_actor_decorator(num_cpus=num_cpus, num_gpus=num_gpus)

  raise Exception("The ray.actor decorator must either be applied with no "
                  "arguments as in '@ray.actor', or it must be applied using "
                  "some of the arguments 'num_cpus' or 'num_gpus' as in "
                  "'ray.actor(num_gpus=1)'.")


ray.worker.global_worker.fetch_and_register["Actor"] = fetch_and_register_actor
