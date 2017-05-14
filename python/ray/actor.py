from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import inspect
import json
import numpy as np
import redis
import traceback

import ray.local_scheduler
import ray.pickling as pickling
import ray.signature as signature
import ray.worker
from ray.utils import random_string, binary_to_hex, hex_to_binary


def random_actor_id():
  return ray.local_scheduler.ObjectID(random_string())


def random_actor_class_id():
  return random_string()


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


def fetch_and_register_actor(actor_class_key, worker):
  """Import an actor.

  This will be called by the worker's import thread when the worker receives
  the actor_class export, assuming that the worker is an actor for that class.
  """
  actor_id_str = worker.actor_id
  (driver_id, class_id, class_name,
   module, pickled_class, actor_method_names) = worker.redis_client.hmget(
       actor_class_key, ["driver_id", "class_id", "class_name", "module",
                         "class", "actor_method_names"])

  actor_name = class_name.decode("ascii")
  module = module.decode("ascii")
  actor_method_names = json.loads(actor_method_names.decode("ascii"))

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
                                data={"actor_id": actor_id_str})
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
    True if the GPUs were successfully reserved and false otherwise.
  """
  assert num_gpus != 0
  local_scheduler_id = local_scheduler["DBClientID"]
  local_scheduler_total_gpus = int(local_scheduler["NumGPUs"])

  success = False

  # Attempt to acquire GPU IDs atomically.
  with worker.redis_client.pipeline() as pipe:
    while True:
      try:
        # If this key is changed before the transaction below (the multi/exec
        # block), then the transaction will not take place.
        pipe.watch(local_scheduler_id)

        # Figure out which GPUs are currently in use.
        result = worker.redis_client.hget(local_scheduler_id, "gpus_in_use")
        gpus_in_use = dict() if result is None else json.loads(result)
        num_gpus_in_use = 0
        for key in gpus_in_use:
          num_gpus_in_use += gpus_in_use[key]
        assert num_gpus_in_use <= local_scheduler_total_gpus

        pipe.multi()

        if local_scheduler_total_gpus - num_gpus_in_use >= num_gpus:
          # There are enough available GPUs, so try to reserve some. We use the
          # hex driver ID in hex as a dictionary key so that the dictionary is
          # JSON serializable.
          driver_id_hex = binary_to_hex(driver_id)
          if driver_id_hex not in gpus_in_use:
            gpus_in_use[driver_id_hex] = 0
          gpus_in_use[driver_id_hex] += num_gpus

          # Stick the updated GPU IDs back in Redis
          pipe.hset(local_scheduler_id, "gpus_in_use", json.dumps(gpus_in_use))
          success = True

        pipe.execute()
        # If a WatchError is not raised, then the operations should have gone
        # through atomically.
        break
      except redis.WatchError:
        # Another client must have changed the watched key between the time we
        # started WATCHing it and the pipeline's execution. We should just
        # retry.
        success = False
        continue

  return success


def select_local_scheduler(local_schedulers, num_gpus, worker):
  """Select a local scheduler to assign this actor to.

  Args:
    local_schedulers: A list of dictionaries of information about the local
      schedulers.
    num_gpus (int): The number of GPUs that must be reserved for this actor.

  Returns:
    The ID of the local scheduler that has been chosen.

  Raises:
    Exception: An exception is raised if no local scheduler can be found with
      sufficient resources.
  """
  driver_id = worker.task_driver_id.id()

  local_scheduler_id = None
  # Loop through all of the local schedulers in a random order.
  local_schedulers = np.random.permutation(local_schedulers)
  for local_scheduler in local_schedulers:
    if local_scheduler["NumCPUs"] < 1:
      continue
    if local_scheduler["NumGPUs"] < num_gpus:
      continue
    if num_gpus == 0:
      local_scheduler_id = hex_to_binary(local_scheduler["DBClientID"])
      break
    else:
      # Try to reserve enough GPUs on this local scheduler.
      success = attempt_to_reserve_gpus(num_gpus, driver_id, local_scheduler,
                                        worker)
      if success:
        local_scheduler_id = hex_to_binary(local_scheduler["DBClientID"])
        break

  if local_scheduler_id is None:
    raise Exception("Could not find a node with enough GPUs or other "
                    "resources to create this actor. The local scheduler "
                    "information is {}.".format(local_schedulers))

  return local_scheduler_id


def export_actor_class(class_id, Class, actor_method_names, worker):
  if worker.mode is None:
    raise NotImplemented("TODO(pcm): Cache actors")
  key = b"ActorClass:" + class_id
  d = {"driver_id": worker.task_driver_id.id(),
       "class_name": Class.__name__,
       "module": Class.__module__,
       "class": pickling.dumps(Class),
       "actor_method_names": json.dumps(list(actor_method_names))}
  worker.redis_client.hmset(key, d)
  worker.redis_client.rpush("Exports", key)


def export_actor(actor_id, class_id, actor_method_names, num_cpus, num_gpus,
                 worker):
  """Export an actor to redis.

  Args:
    actor_id: The ID of the actor.
    actor_method_names (list): A list of the names of this actor's methods.
    num_cpus (int): The number of CPUs that this actor requires.
    num_gpus (int): The number of GPUs that this actor requires.
  """
  ray.worker.check_main_thread()
  if worker.mode is None:
    raise Exception("Actors cannot be created before Ray has been started. "
                    "You can start Ray with 'ray.init()'.")
  key = "Actor:{}".format(actor_id.id())

  # For now, all actor methods have 1 return value.
  driver_id = worker.task_driver_id.id()
  for actor_method_name in actor_method_names:
    # TODO(rkn): When we create a second actor, we are probably overwriting
    # the values from the first actor here. This may or may not be a problem.
    function_id = get_actor_method_function_id(actor_method_name).id()
    worker.function_properties[driver_id][function_id] = (1, num_cpus, 0)

  # Get a list of the local schedulers from the client table.
  client_table = ray.global_state.client_table()
  local_schedulers = []
  for ip_address, clients in client_table.items():
    for client in clients:
      if client["ClientType"] == "local_scheduler" and not client["Deleted"]:
        local_schedulers.append(client)
  # Select a local scheduler for the actor.
  local_scheduler_id = select_local_scheduler(local_schedulers, num_gpus,
                                              worker)
  assert local_scheduler_id is not None

  # We must put the actor information in Redis before publishing the actor
  # notification so that when the newly created actor attempts to fetch the
  # information from Redis, it is already there.
  worker.redis_client.hmset(key, {"class_id": class_id,
                                  "num_gpus": num_gpus})

  # Really we should encode this message as a flatbuffer object. However, we're
  # having trouble getting that to work. It almost works, but in Python 2.7,
  # builder.CreateString fails on byte strings that contain characters outside
  # range(128).

  # TODO(rkn): There is actually no guarantee that the local scheduler that we
  # are publishing to has already subscribed to the actor_notifications
  # channel. Therefore, this message may be missed and the workload will hang.
  # This is a bug.
  worker.redis_client.publish("actor_notifications",
                              actor_id.id() + driver_id + local_scheduler_id)


def actor(*args, **kwargs):
  raise Exception("The @ray.actor decorator is deprecated. Instead, please "
                  "use @ray.remote.")


def make_actor(Class, num_cpus, num_gpus):
  class_id = random_actor_class_id()
  # The list exported will have length 0 if the class has not been exported
  # yet, and length one if it has. This is just implementing a bool, but we
  # don't use a bool because we need to modify it inside of the NewClass
  # constructor.
  exported = []

  # The function actor_method_call gets called if somebody tries to call a
  # method on their local actor stub object.
  def actor_method_call(actor_id, attr, function_signature, *args, **kwargs):
    ray.worker.check_connected()
    ray.worker.check_main_thread()
    args = signature.extend_args(function_signature, args, kwargs)

    function_id = get_actor_method_function_id(attr)
    object_ids = ray.worker.global_worker.submit_task(function_id, "", args,
                                                      actor_id=actor_id)
    if len(object_ids) == 1:
      return object_ids[0]
    elif len(object_ids) > 1:
      return object_ids

  class ActorMethod(object):
    def __init__(self, method_name, actor_id, method_signature):
      self.method_name = method_name
      self.actor_id = actor_id
      self.method_signature = method_signature

    def __call__(self, *args, **kwargs):
      raise Exception("Actor methods cannot be called directly. Instead "
                      "of running 'object.{}()', try 'object.{}.remote()'."
                      .format(self.method_name, self.method_name))

    def remote(self, *args, **kwargs):
      return actor_method_call(self.actor_id, self.method_name,
                               self.method_signature, *args, **kwargs)

  class NewClass(object):
    def __init__(self, *args, **kwargs):
      raise Exception("Actor classes cannot be instantiated directly. "
                      "Instead of running '{}()', try '{}.remote()'."
                      .format(Class.__name__, Class.__name__))

    @classmethod
    def remote(cls, *args, **kwargs):
      actor_object = cls.__new__(cls)
      actor_object._manual_init(*args, **kwargs)
      return actor_object

    def _manual_init(self, *args, **kwargs):
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

      # Create objects to wrap method invocations. This is done so that we
      # can invoke methods with actor.method.remote() instead of
      # actor.method().
      self._actor_method_invokers = dict()
      for k, v in self._ray_actor_methods.items():
        self._actor_method_invokers[k] = ActorMethod(
            k, self._ray_actor_id, self._ray_method_signatures[k])

      # Export the actor class if it has not been exported yet.
      if len(exported) == 0:
        export_actor_class(class_id, Class, self._ray_actor_methods.keys(),
                           ray.worker.global_worker)
        exported.append(0)
      # Export the actor.
      export_actor(self._ray_actor_id, class_id,
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
      if attr in ["_manual_init", "_ray_actor_id", "_ray_actor_methods",
                  "_actor_method_invokers", "_ray_method_signatures"]:
        return super(NewClass, self).__getattribute__(attr)
      if attr in self._ray_actor_methods.keys():
        return self._actor_method_invokers[attr]
      # There is no method with this name, so raise an exception.
      raise AttributeError("'{}' Actor object has no attribute '{}'"
                           .format(Class, attr))

    def __repr__(self):
      return "Actor(" + self._ray_actor_id.hex() + ")"

  return NewClass


ray.worker.global_worker.fetch_and_register_actor = fetch_and_register_actor
ray.worker.global_worker.make_actor = make_actor
