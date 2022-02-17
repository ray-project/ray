import ray
import csv
import argparse
import numpy as np 
import time
import multiprocessing
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=500_000_000)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata


def test_ray_pipeline():
    ray_pipeline_begin = perf_counter()

    @ray.remote(num_cpus=1)
    def consumer_one(obj_ref):
        return True

    @ray.remote(num_cpus=1)
    def consumer_two(obj_ref1, obj_ref2):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
    
    ref_one   = producer.remote()
    ref_two   = producer.remote()
    time.sleep(0.3)
    ref_three = producer.remote()

    res = []

    res.append(consumer_two.remote(ref_one, ref_three))
    res.append(consumer_two.remote(ref_two, ref_three))
    res.append(consumer_one.remote(ref_three))

    ray.get(res)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin


ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE)

ray_time = []

ray_time.append(test_ray_pipeline())
print(f"Ray Pipieline time: {sum(ray_time)}")
