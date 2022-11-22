# flake8: noqa
"""
This file holds code for the runtime envs documentation.

FIXME: We switched our code formatter from YAPF to Black. Check if we can enable code
formatting on this module and update the paragraph below. See issue #21318.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""
import ray

# fmt: off

# __runtime_env_pip_def_start__
runtime_env = {
    "pip": ["emoji"],
    "env_vars": {"TF_WARNINGS": "none"}
}
# __runtime_env_pip_def_end__

# __strong_typed_api_runtime_env_pip_def_start__
from ray.runtime_env import RuntimeEnv
runtime_env = RuntimeEnv(
    pip=["emoji"],
    env_vars={"TF_WARNINGS": "none"}
)
# __strong_typed_api_runtime_env_pip_def_end__

# __ray_init_start__
# Option 1: Starting a single-node local Ray cluster or connecting to existing local cluster
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
