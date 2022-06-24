import os
import random
from time import sleep
import numpy as np
import pandas as pd
import psutil
import ray
from ray.data._internal.compute import ActorPoolStrategy


if __name__ == "__main__":
    ray.init(address='auto')
    
    data = list(range(400000))
    num_chunks = 4
    chunk_size = len(data) // num_chunks
    chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
    dfs = [
        pd.DataFrame({"col1": list(chunk), "col2": list(map(str, chunk))})
        for chunk in chunks
    ]
    ds = ray.data.from_pandas(dfs)
    print(ds)
    
    concurrent_mapper = 2
    estimated_cpu = 1
    estimated_heap = 2 * 1024 * 1024 * 1024

    def udf(_):
        return pd.DataFrame({"text": np.random.random((100, 1)).tolist()})
    def batch_udf(df: pd.DataFrame) -> pd.DataFrame:
        df["col1"] =  df["col1"] / df["col1"].max()
        df = df.drop(columns=["col2"])
        return df    
    while True:
        ds.map(udf, compute=ActorPoolStrategy(concurrent_mapper, concurrent_mapper), num_cpus=estimated_cpu)
        # ds.map_batches(batch_udf, compute=ActorPoolStrategy(concurrent_mapper, concurrent_mapper), num_cpus=estimated_cpu)
        
    # counter = Counter.remote()

    # while True:
    #     obj_ref = counter.increment.remote()
    #     print(obj_ref)
    #     sleep(5)

    
    