# flake8: noqa
"""
This file holds code for the runtime envs documentation.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""
import ray

# yapf: disable

# __runtime_env_conda_def_start__
runtime_env = {
    "conda": {
        "dependencies":
        ["toolz", "dill", "pip", {
            "pip": ["pendulum", "ray[serve]"]
        }]
    },
    "env_vars": {"TF_WARNINGS": "none"}
}
# __runtime_env_conda_def_end__

# __ray_init_start__
# Starting a single-node local Ray cluster
ray.init(runtime_env=runtime_env)
# __ray_init_end__

@ray.remote
def f_job():
    pass

@ray.remote
class Actor_job:
    def g(self):
        pass

ray.get(f_job.remote())
a = Actor_job.remote()
ray.get(a.g.remote())

ray.shutdown()

ray.init()

@ray.remote
def f():
    pass

@ray.remote
class SomeClass:
    pass

# __per_task_per_actor_start__
# Invoke a remote task that will run in a specified runtime environment.
f.options(runtime_env=runtime_env).remote()

# Instantiate an actor that will run in a specified runtime environment.
actor = SomeClass.options(runtime_env=runtime_env).remote()

# Specify a runtime environment in the task definition.  Future invocations via
# `g.remote()` will use this runtime environment unless overridden by using
# `.options()` as above.
@ray.remote(runtime_env=runtime_env)
def g():
    pass

# Specify a runtime environment in the actor definition.  Future instantiations
# via `MyClass.remote()` will use this runtime environment unless overridden by
# using `.options()` as above.
@ray.remote(runtime_env=runtime_env)
class MyClass:
    pass
# __per_task_per_actor_end__
