from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import numpy as np
import photon
import inspect
import random

import ray.pickling as pickling
import ray.worker
import ray.experimental.state as state

def random_string():
  return np.random.bytes(20)

def random_actor_id():
  return photon.ObjectID(random_string())

def get_actor_method_function_id(attr):
  """Get the function ID corresponding to an actor method.

  Args:
    attr (string): The attribute name of the method.
  Returns:
    Function ID corresponding to the method.
  """
  function_id = hashlib.sha1()
  function_id.update(attr.encode("ascii"))
  return photon.ObjectID(function_id.digest())

def fetch_and_register_actor(key, worker):
  """Import an actor."""
  driver_id, actor_id_str, actor_name, module, pickled_class, class_export_counter = \
    worker.redis_client.hmget(key, ["driver_id", "actor_id", "name", "module", "class", "class_export_counter"])
  actor_id = photon.ObjectID(actor_id_str)
  actor_name = actor_name.decode("ascii")
  module = module.decode("ascii")
  class_export_counter = int(class_export_counter)
  try:
    unpickled_class = pickling.loads(pickled_class)
  except:
    raise NotImplemented("TODO(pcm)")
  else:
    # TODO(pcm): Why is the below line necessary?
    unpickled_class.__module__ = module
    worker.actors[actor_id_str] = unpickled_class.__new__(unpickled_class)
    for (k, v) in inspect.getmembers(unpickled_class, predicate=inspect.isfunction):
      function_id = get_actor_method_function_id(k).id()
      worker.function_names[function_id] = k
      worker.functions[function_id] = v
  print("registering actor...")

def export_actor(actor_id, Class, worker):
  """Export an actor to redis.

  Args:
    actor_id: The ID of the actor.
    Class: Name of the class to be exported as an actor.
    worker: The worker class 
  """
  ray.worker.check_main_thread()
  if worker.mode is None:
    raise NotImplemented("TODO(pcm): Cache actors")
  key = "Actor:{}".format(actor_id.id())
  pickled_class = pickling.dumps(Class)

  # select local scheduler for the actor
  local_schedulers = state.get_local_schedulers()
  local_scheduler_id = random.choice(local_schedulers)

  worker.redis_client.publish("actor_notifications", actor_id.id() + local_scheduler_id)
  import time
  time.sleep(2) # XXX
  

  # select worker to put the actor on
  # workers = worker.redis_client.keys("Workers:*")
  # actor_worker_id = random.choice(workers)[len("Workers:"):]

  d = {"driver_id": worker.task_driver_id.id(),
       "actor_id": actor_id.id(),
       # "actor_worker_id": actor_worker_id,
       "name": Class.__name__,
       "module": Class.__module__,
       "class": pickled_class,
       "class_export_counter": worker.driver_export_counter}
  worker.redis_client.hmset(key, d)
  worker.redis_client.rpush("Exports", key)
  worker.driver_export_counter += 1
  

def actor(Class):
  # This function gets called if somebody tries to call a method on their
  # local actor stub object
  
  def actor_method_call(actor_id, attr, *args, **kwargs):
    ray.worker.check_connected()
    ray.worker.check_main_thread()
    args = list(args)
    function_id = get_actor_method_function_id(attr)
    # TODO(pcm): Extend args with keyword args
    object_ids = ray.worker.global_worker.submit_task(function_id, "", args, actor_id=actor_id)
    if len(object_ids) == 1:
      return object_ids[0]
    elif len(object_ids) > 1:
      return object_ids

  class NewClass(object):
    def __init__(self, *args, **kwargs):
      self._ray_actor_id = random_actor_id()
      self._ray_actor_methods = {k: v for (k, v) in inspect.getmembers(Class, predicate=inspect.isfunction)}
      export_actor(self._ray_actor_id, Class, ray.worker.global_worker)
      # Call __init__ as a remote function
      actor_method_call(self._ray_actor_id, "__init__", *args, **kwargs)
    # Make IPython tab completion work
    def __dir__(self):
      return self._ray_actor_methods
    def __getattribute__(self, attr):
      # The following is needed so we can still access self.actor_methods.
      if attr in ["_ray_actor_id", "_ray_actor_methods"]:
        return super(NewClass, self).__getattribute__(attr)
      if attr in self._ray_actor_methods.keys():
        return lambda *args, **kwargs: actor_method_call(self._ray_actor_id, attr, *args, **kwargs)
    def __repr__(self):
      return "Actor(" + self._ray_actor_id.hex() + ")"

  return NewClass

ray.worker.global_worker.fetch_and_register["Actor"] = fetch_and_register_actor
