import os
import time
import pickle
import ray


@ray.remote
class Counter(object):
    def __init__(self):
        self.value = 0

    def increment(self, i):
        self.value += i


if __name__ == "__main__":
    ray.init()

    counter = Counter.remote()
    client = ray.worker.global_worker.raylet_client

    while True:
        start = time.time()
        M = 100
        N = 1000
        for _ in range(M):
            ids = []
            for _ in range(N):
                ids.append(client.fast_submit_task(
                    counter, b"increment", pickle.dumps([1])))
            client.fast_get_results(ids)
        elapsed = time.time() - start
        print("Tasks per second", M * N / elapsed)
