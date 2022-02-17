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
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=250_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/pipeline.csv")
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=5)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
OBJECT_STORE_BUFFER_SIZE = 5_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def test_ray_scatter_gather():
    @ray.remote
    def scatter(npartitions, object_store_size):
        size = (object_store_size // 8)//npartitions
        #return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))
        return tuple(np.random.randint(1<<31, size=size) for i in range(npartitions))

    @ray.remote
    def worker(partition):
        return np.average(partition)

    @ray.remote
    def gather(*avgs):
        return np.average(avgs)

    scatter_gather_start = perf_counter()

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    scatter_outputs = [scatter.options(num_returns=npartitions).remote(npartitions, OBJECT_STORE_SIZE) for _ in range(WORKING_SET_RATIO)]
    outputs = [[] for _ in range(WORKING_SET_RATIO)]
    for i in range(WORKING_SET_RATIO):
        for j in range(npartitions):
            outputs[i].append(worker.remote(scatter_outputs[i][j]))
    del scatter_outputs
    gather_outputs = []
    for i in range(WORKING_SET_RATIO):
        gather_outputs.append(gather.remote(*[o for o in outputs[i]]))
    del outputs
    ray.get(gather_outputs)

    scatter_gather_end = perf_counter()
    del gather_outputs
    return scatter_gather_end - scatter_gather_start

def test_baseline_scatter_gather():
    @ray.remote
    def scatter(npartitions, object_store_size):
        size = (object_store_size // 8)//npartitions
        return tuple(np.random.randint(1<<31, size=size) for i in range(npartitions))

    @ray.remote
    def worker(partitions):
        return np.average(partitions)

    @ray.remote
    def gather(*avgs):
        return np.average(avgs)

    scatter_gather_start = perf_counter()

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    for _ in range(WORKING_SET_RATIO):
        scatter_outputs = scatter.options(num_returns=npartitions).remote(npartitions, OBJECT_STORE_SIZE)
        outputs = []
        for i in range(npartitions):
            outputs.append(worker.remote(scatter_outputs[i]))
        del scatter_outputs
        ray.get(gather.remote(*outputs))

    scatter_gather_end = perf_counter()
    return scatter_gather_end - scatter_gather_start

ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

#warm up
test_baseline_scatter_gather()

ray_time = []
base_time = []
for i in range(NUM_TRIAL):
    base_time.append(test_baseline_scatter_gather())
    ray_time.append(test_ray_scatter_gather())

print(f"Ray scatter_gather time: {sum(ray_time)/NUM_TRIAL}")
print(f"Baseline scatter_gather time: {sum(base_time)/NUM_TRIAL}")
#header = ['base_std','ray_std','base_std, 'ray_std', 'base_var','ray_var','working_set_ratio', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.std(base_time), np.std(ray_time), np.var(base_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(base_time)/NUM_TRIAL, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)
