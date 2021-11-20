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
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=2)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=250_000_000)
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
    def last_consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    refs = [[] for _ in range(NUM_STAGES+1)]
    for _ in range(WORKING_SET_RATIO*num_fill_object_store):
        refs[0].append(producer.remote())

    for stage in range(1, NUM_STAGES):
        for r in refs[stage-1]:
            refs[stage].append(consumer.remote(r))

    for r in refs[NUM_STAGES-1]:
        ref = last_consumer.remote(r)
        print("REF", ref, "depends on", r)
        refs[NUM_STAGES].append(ref)

    for ref in refs[-1]:
        print("GET", ref)
        ray.get(ref)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin


ray.init(object_store_memory=OBJECT_STORE_SIZE)

ray_time = []
for i in range(NUM_TRIAL):
    ray_time.append(test_ray_pipeline())

print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
