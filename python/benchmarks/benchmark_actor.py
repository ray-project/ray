from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

NUM_WORKERS = 4


def setup():
    if not hasattr(setup, "is_initialized"):
        ray.init(num_cpus=4)
        setup.is_initialized = True


@ray.remote
class MyActor(object):
    def __init__(self):
        self.x = None

    def get_x(self):
        return self.x

    def set_x(self, x):
        self.x = x


class ActorInstantiationSuite(object):
    def instantiate_actor(self):
        actor = MyActor.remote()
        # Block to make sure actor is instantiated
        ray.get(actor.get_x.remote())

    def instantiate_many_actors(self):
        actors = [MyActor.remote() for _ in range(NUM_WORKERS + 10)]
        ray.get([actor.get_x.remote() for actor in actors])

    def time_instantiate_actor(self):
        self.instantiate_actor()

    def peakmem_instantiate_actor(self):
        self.instantiate_actor()

    def time_instantiate_many_actors(self):
        self.instantiate_many_actors()

    def peakmem_instantiate_many_actors(self):
        self.instantiate_many_actors()


class ActorMethodSuite(object):
    def setup(self):
        self.actor = MyActor.remote()
        # Block to make sure actor is instantiated
        ray.get(self.actor.get_x.remote())

    def time_call_method(self):
        ray.get(self.actor.get_x.remote())

    def peakmem_call_method(self):
        ray.get(self.actor.get_x.remote())


class ActorCheckpointSuite(object):
    def checkpoint_and_restore(self):
        actor = MyActor.remote()
        actor.__ray_checkpoint__.remote()
        assert ray.get(actor.__ray_checkpoint_restore__.remote())

    def save_checkpoint(self):
        actor = MyActor.remote()
        checkpoint = ray.get(actor.__ray_save_checkpoint__.remote())
        return checkpoint

    def time_checkpoint_and_restore(self):
        self.checkpoint_and_restore()

    def peakmem_checkpoint_and_restore(self):
        self.checkpoint_and_restore()

    def time_save_checkpoint(self):
        self.save_checkpoint()

    def mem_save_checkpoint(self):
        return self.save_checkpoint()
