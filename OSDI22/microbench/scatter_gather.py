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
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=40_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/pipeline.csv")
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=3)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']

def test_ray_scatter_gather():
    scatter_gather_start = perf_counter()

    @ray.remote
    def scatter(npartitions, object_size):
        data = np.zeros(object_size // 8)
        size = object_size//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def gather(partitions, npartitions):
        return np.zeros(OBJECT_SIZE // 8)

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    scatter_outputs = [scatter.options(num_returns=npartitions).remote(npartitions, OBJECT_STORE_SIZE) for _ in range(WORKING_SET_RATIO)]
    outputs = []
    for i in range(WORKING_SET_RATIO):
        for j in range(npartitions):
            outputs.append(gather.remote(scatter_outputs[i][j], npartitions))
    ray.get(outputs)

    scatter_gather_end = perf_counter()
    return scatter_gather_end - scatter_gather_start

def test_baseline_scatter_gather():
    scatter_gather_start = perf_counter()

    @ray.remote
    def scatter(npartitions, object_size):
        data = np.zeros(object_size // 8)
        size = object_size//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def gather(partitions, npartitions):
        return np.zeros(OBJECT_SIZE // 8)

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    for _ in range(WORKING_SET_RATIO):
        scatter_outputs = scatter.options(num_returns=npartitions).remote(npartitions, OBJECT_STORE_SIZE)
        outputs = []
        for i in range(npartitions):
            outputs.append(gather.remote(scatter_outputs[i], npartitions))
        ray.get(outputs)

    scatter_gather_end = perf_counter()
    return scatter_gather_end - scatter_gather_start

ray.init(object_store_memory=OBJECT_STORE_SIZE)

#warm up
test_baseline_scatter_gather()

ray_time = []
base_time = []
for i in range(NUM_TRIAL):
    base_time.append(test_baseline_scatter_gather())
    ray_time.append(test_ray_scatter_gather())

print(f"Baseline scatter_gather time: {sum(base_time)/NUM_TRIAL}")
print(f"Ray scatter_gather time: {sum(ray_time)/NUM_TRIAL}")
'''
#header = ['base_var','ray_var','working_set_ratio', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.var(base_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(base_time)/NUM_TRIAL, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)
'''
