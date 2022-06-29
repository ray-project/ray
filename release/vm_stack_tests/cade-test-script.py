import ray
import requests
import time
import os
import json

ray.init()

@ray.remote
class Counter:
    def __init__(self):
        self.counter = 0

    def inc(self):
        self.counter += 1

    def get_counter(self):
        return self.counter

def main():
    counter = Counter.remote()
    
    for _ in range(5):
        ray.get(counter.inc.remote())
        print(ray.get(counter.get_counter.remote()))
    print(requests.__version__)

if __name__ == '__main__':
    start_time = time.time()
    rval = main()
    duration = time.time() - start_time

    if "TEST_OUTPUT_JSON" in os.environ:
        with open(os.environ["TEST_OUTPUT_JSON"], "w") as out_file:
            results = {
                "time": duration,
                "success": "1" if rval is None else "0",
                "perf_metrics": [],
            }
            json.dump(results, out_file)
