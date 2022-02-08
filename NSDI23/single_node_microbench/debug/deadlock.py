import ray
import csv
import argparse
import numpy as np 
import time
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=350_000_000)
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']

SLEEP_TIME = 3

def test_ray_pipeline():
    ray_pipeline_begin = perf_counter()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        print("producer return")
        return np.zeros(OBJECT_SIZE // 8)
        
    @ray.remote(num_cpus=1) 
    def first_producer(): 
        time.sleep(SLEEP_TIME)
        print("sleep finished in first producer")
        return np.zeros(OBJECT_SIZE // 8)
        
    a = first_producer.remote()
    b = producer.remote()
    c = producer.remote()

    time.sleep(SLEEP_TIME*3)
    print("calling consumers")

    a1 = consumer.remote(a)
    del a
    b1 = consumer.remote(b)
    del b
    c1 = consumer.remote(c)
    del c

    print("Waiting for consuermers to finish")
    ray.get(a1)
    ray.get(b1)
    ray.get(c1)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin


ray.init(object_store_memory=OBJECT_STORE_SIZE)

ray_time = []
for i in range(NUM_TRIAL):
    ray_time.append(test_ray_pipeline())

print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
