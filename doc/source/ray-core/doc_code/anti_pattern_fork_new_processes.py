import os

os.environ["RAY_DEDUP_LOGS"] = "0"

import ray
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import numpy as np


@ray.remote
def generate_response(request):
    print(request)
    array = np.ones(100000)
    return array


def process_response(response, idx):
    print(f"Processing response {idx}")
    return response


def main():
    ray.init()
    responses = ray.get([generate_response.remote(f"request {i}") for i in range(4)])

    # Better approach: Set the start method to "spawn"
    multiprocessing.set_start_method("spawn", force=True)

    with ProcessPoolExecutor(max_workers=4) as executor:
        future_to_task = {}
        for idx, response in enumerate(responses):
            future_to_task[executor.submit(process_response, response, idx)] = idx

        for future in as_completed(future_to_task):
            idx = future_to_task[future]
            response_entry = future.result()
            print(f"Response {idx} processed: {response_entry}")

    ray.shutdown()


if __name__ == "__main__":
    main()
