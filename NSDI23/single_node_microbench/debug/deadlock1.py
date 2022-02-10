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
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        time.sleep(0.3)
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    num_workers = multiprocessing.cpu_count()

    refs = [[] for _ in range(2)]
    for _ in range(num_workers):
        refs[0].append(producer.remote())

    for i in range(num_workers):
        refs[1].append(consumer.remote(refs[0][i]))

    ray.get(refs[1])

    ray_pipeline_end = perf_counter()

    del refs

    return ray_pipeline_end - ray_pipeline_begin


ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

ray_time = []

ray_time.append(test_ray_pipeline())
print(f"Ray Pipieline time: {sum(ray_time)}")
