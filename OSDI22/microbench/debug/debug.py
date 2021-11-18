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
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']

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
    ray.get(refs[-1])
    '''
    for ref in refs:
        for r in ref:
            ray.get(r)
    '''
    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_baseline_pipeline():
    baseline_start = perf_counter()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        #args = ray.get(obj_ref)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = OBJECT_STORE_SIZE//OBJECT_SIZE
    for i in range(WORKING_SET_RATIO):
        produced_objs = [producer.remote()  for _ in range(num_fill_object_store)]

        refs = [[] for _ in range(NUM_STAGES)]
        for obj in produced_objs:
            refs[0].append(consumer.remote(obj))

        del produced_objs
        '''
        for stage in range(1, NUM_STAGES):
            for r in refs[stage-1]:
                refs[stage].append(consumer.remote(r))
        '''
        ray.get(refs[-1])

    baseline_end = perf_counter()
    return baseline_end - baseline_start

ray.init(object_store_memory=OBJECT_STORE_SIZE)

ray_time = []
for i in range(NUM_TRIAL):
    ray_time.append(test_ray_pipeline())


print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
