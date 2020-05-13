from ray import serve
from ray.serve.constants import DEFAULT_HTTP_ADDRESS
import requests
import time
import pandas as pd
from tqdm import tqdm

serve.init(blocking=True)


def noop(_):
    return ""


serve.create_endpoint("noop", "/noop")
serve.create_backend("noop", noop)
serve.set_traffic("noop", {"noop": 1.0})

url = "{}/noop".format(DEFAULT_HTTP_ADDRESS)
while requests.get(url).status_code == 404:
    time.sleep(1)
    print("Waiting for noop route to showup.")

latency = []
for _ in tqdm(range(5200)):
    start = time.perf_counter()
    resp = requests.get(url)
    end = time.perf_counter()
    latency.append(end - start)

# Remove initial samples
latency = latency[200:]

series = pd.Series(latency) * 1000
print("Latency for single noop backend (ms)")
print(series.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))
