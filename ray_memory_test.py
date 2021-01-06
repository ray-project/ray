# Burner file to test basic modifications to the 'ray memory' CLI
# according to https://github.com/ray-project/ray/issues/12280
import ray
import time
ray.init()

# Local Refs
@ray.remote
def f(arg):
    return arg

a = ray.put(None)
b = f.remote(None)

# Give some time to test 'ray memory'
time.sleep(7200)


# Large Suite of Tests
# @ray.remote
# def f(arg):
#     return arg


# a = ray.put(None)
# b = f.remote(None)
# c = ray.put([1, 2, 3])
# d, e = ray.put([c]), ray.put(["hello"])
# del c