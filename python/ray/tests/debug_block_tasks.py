import ray
import time
import sys
import argparse
import csv
import numpy as np
from time import perf_counter
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=2)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=200_000_000)
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
NUM_STAGES = params['NUM_STAGES']

def test_ray_pipeline():
    ray_pipeline_begin = perf_counter()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        #args = ray.get(obj_ref)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        time.sleep(0.1)
        for i in range(1000000):
            pass
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = OBJECT_STORE_SIZE//OBJECT_SIZE 
    produced_objs = [producer.remote()  for _ in range(WORKING_SET_RATIO*num_fill_object_store)]
    refs = [[] for _ in range(NUM_STAGES)]

    for obj in produced_objs:
        refs[0].append(consumer.remote(obj))
    '''
    for stage in range(1, NUM_STAGES):
        for r in refs[stage-1]:
            refs[stage].append(consumer.remote(r))
    '''
    del produced_objs
    #ray.get(refs[-1])
    for r in refs[0]:
        print(r)
        ray.get(r)
    '''
    for ref in refs:
        for r in ref:
            ray.get(r)
    '''
    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

ray.init(object_store_memory=OBJECT_STORE_SIZE)
print(test_ray_pipeline())
