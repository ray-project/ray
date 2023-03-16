# flake8: noqa

# fmt: off
# __actor_creator_failure_begin__
import ray
import os
import signal
ray.init()

@ray.remote(max_restarts=-1)
class Actor:
    def ping(self):
        return "hello"

@ray.remote
class Parent:
    def generate_actors(self):
        self.child = Actor.remote()
        self.detached_actor = Actor.options(name="actor", lifetime="detached").remote()
        return self.child, self.detached_actor, os.getpid()

parent = Parent.remote()
actor, detached_actor, pid = ray.get(parent.generate_actors.remote())

os.kill(pid, signal.SIGKILL)

try:
    print("actor.ping:", ray.get(actor.ping.remote()))
except ray.exceptions.RayActorError as e:
    print("Failed to submit actor call", e)
# Failed to submit actor call The actor died unexpectedly before finishing this task.
# 	class_name: Actor
# 	actor_id: 56f541b178ff78470f79c3b601000000
# 	namespace: ea8b3596-7426-4aa8-98cc-9f77161c4d5f
# The actor is dead because because all references to the actor were removed.

try:
    print("detached_actor.ping:", ray.get(detached_actor.ping.remote()))
except ray.exceptions.RayActorError as e:
    print("Failed to submit detached actor call", e)
# detached_actor.ping: hello

# __actor_creator_failure_end__
# fmt: on
