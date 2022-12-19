import ray
import argparse
from ray import tune
from ray.air.config import ScalingConfig
from ray.tune import Tuner
from ray.data.preprocessors import BatchMapper
from ray.air import session
from ray.data import Dataset
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig
import time
from ray._private.utils import get_system_memory, get_used_memory

preprocessor = BatchMapper(lambda df: df * 2, batch_format="pandas")
threshold = 0.9

def pct_to_bytes(pct):
    return get_system_memory() * pct

def current_usage_pct():
    return get_used_memory() / get_system_memory()

# Utility for allocating locally (ray not involved)
def alloc_mem(bytes):
    bytes = int(bytes)
    chunks = 10
    mem = []
    bytes_per_chunk = bytes // 8 // chunks
    for _ in range(chunks):
        mem.extend([0] * bytes_per_chunk)
    return mem

@ray.remote
def alloc_remote(bytes, post_sleep=0):
    print('Leaf allocation')
    mem = alloc_mem(bytes)
    time.sleep(post_sleep)
    print('Done leaf')
    return mem

@ray.remote
def double_nested(bytes_parent, bytes_child):
    mem_parent = alloc_mem(bytes_parent)
    mem_child = alloc_remote.remote(bytes_child, post_sleep=300)
    return mem_parent, mem_child

# Creates a task->task->task workload
@ray.remote
def triple_nested(bytes_root, bytes_mid, bytes_leaf):
    print('Starting allocations...')
    mem_root = alloc_mem(bytes_root)
    mem_mid, mem_leaf = double_nested.remote(bytes_mid, bytes_leaf)
    print('Finished allocations...')
    return (mem_root, mem_mid, mem_leaf)

def train_loop_per_worker():
    print('Starting loop...')
    data_shard: Dataset = session.get_dataset_shard("train")
    # trainer_data = alloc_mem(pct_to_bytes(0.5))
    print('Done local allocation...')
    
    total = 0
    #mem = data_shard.take_all(limit=1e20)
    # batch_size = pct_to_bytes(0.3) // 8
    for batch in data_shard.iter_batches(batch_size=65536):
        total += batch[0]
    total = total.item()
    print('Starting local iteration...')
    # for i in range(1000):
    #     total += trainer_data[i * len(trainer_data) // 1000]

    print(f'memory per block: {data_shard.size_bytes() / data_shard.num_blocks()}')
    session.report({'result': total})


if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-tune-instances', type=int, required=False, default=1)
    args = parser.parse_args()
    tuners = args.num_tune_instances
    
    ray.init(
        _system_config={
            "task_oom_retries": 40,
        },
    )
    trainer = TorchTrainer(
        train_loop_per_worker = train_loop_per_worker,
        train_loop_config = {},
        scaling_config=ScalingConfig(num_workers=1),
        datasets={
            "train": ray.data.range_tensor(pct_to_bytes(0.5) // 8),
        },
    )

    tuner = Tuner(
        trainer,
        param_space={"train_loop_config": {"random_var": tune.grid_search(list(range(1, tuners + 1)))}},
        tune_config=tune.TuneConfig(metric="result", mode="min", num_samples=1),
    )
    print('Starting trials...')
    tuner.fit()
