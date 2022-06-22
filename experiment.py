import ray
from ray import serve

ray.init(address="auto")
serve.start(detached=True)


@serve.deployment
class Test:
    def __init__(self, msg):
        self._msg = msg

    def __call__(self, *args):
        return self._msg


obj_ref = ray.put("Hello world!")

Test.deploy(obj_ref)




# Option 1: Serve controller is the "driver"

from ray.serve import SharedMemoryObject

Deployment1.bind(SharedMemoryObject(my_gen_function))

# Option 2: Detached objects

@ray.remote
def my_gen_function(path):
    return s3.download_model_weights(path)


Deployment1.bind(my_gen_function.bind(arg1, arg2, arg3))

# Controller does this:
ref = my_gen_function.remote(arg1, arg2, arg3)
# Controller passes it to the replicas:
Replica.remote(ref)
# In the replica, it's replaced with whatever the output was (using shmem):
def __init__(self, model_weights):
    ...


# What needs to be done:
# 1) Change DAG building/transforming code to accept bound functions as an argument to a class constructor.
# 2) Define some interface in Serve controller that accepts bound function arguments.
# 3) Update controller lifecycle code to pass in the resolved bound arguments (object refs).
# 4) Update replica code to replace the bound functions with their obj refs at runtime.