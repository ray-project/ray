# Burner file to test basic modifications to the 'ray memory' CLI according to https://github.com/ray-project/ray/issues/12280
import ray
import time
ray.init()

@ray.remote
def f(arg):
    return arg

a = ray.put(None)
b = f.remote(None)

# Give some time to test 'ray memory' 
time.sleep(60)