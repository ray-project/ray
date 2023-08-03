# __anti_pattern_start__
import ray

ray.init()

global_var = 3


@ray.remote
class Actor:
    def f(self):
        return global_var + 3


actor = Actor.remote()
global_var = 4
# This returns 6, not 7. It is because the value change of global_var
# inside a driver is not reflected to the actor
# because they are running in different processes.
assert ray.get(actor.f.remote()) == 6
# __anti_pattern_end__


# __better_approach_start__
@ray.remote
class GlobalVarActor:
    def __init__(self):
        self.global_var = 3

    def set_global_var(self, var):
        self.global_var = var

    def get_global_var(self):
        return self.global_var


@ray.remote
class Actor:
    def __init__(self, global_var_actor):
        self.global_var_actor = global_var_actor

    def f(self):
        return ray.get(self.global_var_actor.get_global_var.remote()) + 3


global_var_actor = GlobalVarActor.remote()
actor = Actor.remote(global_var_actor)
ray.get(global_var_actor.set_global_var.remote(4))
# This returns 7 correctly.
assert ray.get(actor.f.remote()) == 7
# __better_approach_end__
