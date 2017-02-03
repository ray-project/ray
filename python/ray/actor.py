import numpy as np
import photon
import inspect
import random

import ray.pickling as pickling
import ray.worker

def random_string():
  return np.random.bytes(20)

def random_actor_id():
  return photon.ObjectID(random_string())

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

  # select worker to put the actor on
  workers = worker.redis_client.keys("Workers:*")
  actor_worker_id = random.choice(workers)[len("Workers:"):]

  d = {"driver_id": worker.task_driver_id.id(),
       "actor_id": actor_id.id(),
       "actor_worker_id": actor_worker_id,
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
  def actor_method_call(attr, *args, **kwargs):
    ray.worker.check_connected()
    ray.worker.check_main_thread()
    args = list(args)
    # TODO(pcm): Extend args with keyword args
    # TODO(pcm): call ray.worker._submit_task()
    print("executing actor attribute", attr)
  class NewClass(object):
    def __init__(self, *args, **kwargs):
      self.actor_id = random_actor_id()
      self.actor_methods = {k: v for (k, v) in inspect.getmembers(Class, predicate=inspect.isfunction)}
      export_actor(self.actor_id, Class, ray.worker.global_worker)
    def __getattribute__(self, attr):
      # First try to access the attribute from NewClass. This is needed so
      # we can still access self.actor_methods.
      try:
        x = super(NewClass, self).__getattribute__(attr)
      except AttributeError:
        pass
      else:
        return x
      if attr in self.actor_methods.keys():
        return lambda *args, **kwargs: actor_method_call(attr, *args, **kwargs)
    def __repr__(self):
      return "Actor(" + self.actor_id.hex() + ")"
  return NewClass

ray.worker.global_worker.fetch_and_register["Actor"] = fetch_and_register_actor
