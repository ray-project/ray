import ray

# ray.init("anyscale://avnish_rllib/avnish-pymodules", runtime_env={"py_modules": [
#     "/Users/avnish/avnish_ray/rllib"]})

ray.init("auto", runtime_env={"py_modules": [
    "/Users/avnish/avnish_ray/rllib"]})



@ray.remote
def task():
    import sys
    print(sys.path)
    from ray import rllib
    print(rllib.__path__)
    from ray.rllib.agents.impala import ImpalaTrainer

ray.get(task.remote())