# flake8: noqa
# __env_var_start__
import ray
import os

ray.init()


@ray.remote
def myfunc():
    myenv = os.environ.get("FOO")
    print(f"myenv is {myenv}")
    return 1


ray.get(myfunc.remote())
# this prints: "myenv is None"
# __env_var_end__
ray.shutdown()

# __env_var_fix_start__
ray.init(runtime_env={"env_vars": {"FOO": "bar"}})


@ray.remote
def myfunc():
    myenv = os.environ.get("FOO")
    print(f"myenv is {myenv}")
    return 1


ray.get(myfunc.remote())
# this prints: "myenv is bar"
# __env_var_fix_end__
