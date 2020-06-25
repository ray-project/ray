import ray
from ray import serve
from ray.serve.api import _get_master_actor

import pandas as pd

ray.init(address="auto")
serve.init()

master = _get_master_actor()
traces = ray.get(master._collect_trace.remote())

print("received {} trace events".format(len(traces)))

df = pd.DataFrame(traces)
df.to_parquet("result.pq")
