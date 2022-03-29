import ray
import time
import asyncio

from ray import serve


@serve.deployment(num_replicas=2)
def model_one(input_data):
    print("Model 1 predict")
    time.sleep(4)
    return 1


@serve.deployment(num_replicas=2)
def model_two(input_data):
    print("Model 2 predict")
    time.sleep(4)
    return 2


@serve.deployment(max_concurrent_queries=10, route_prefix="/composed")
class EnsembleModel:
    def __init__(self):
        self.model_one = model_one.get_handle()
        self.model_two = model_two.get_handle()

    async def __call__(self, input_data):
        print("Call models concurrently, wait for both to finish")
        tasks = [self.model_one.remote(input_data), self.model_two.remote(input_data)]
        print("collect models predictions (non-blocking)")
        predictions = await asyncio.gather(*tasks)
        return predictions


def send_concurrent_model_requests(num_single_model_replicas=2):
    ensemble_model = EnsembleModel.get_handle()
    all_data = [
        ensemble_model.remote(input_data)
        for input_data in range(num_single_model_replicas)
    ]
    all_predictions = ray.get(all_data)
    print(all_predictions)


if __name__ == "__main__":
    # start local cluster and ray serve processes
    # Start ray with 8 processes.
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=8)
    serve.start()

    # deploy all actors / models
    model_one.deploy()
    model_two.deploy()
    EnsembleModel.deploy()

    # Send 2 concurrent requests to the Ensemble Model for predictions.
    # This runs 4 seconds in total calling 2 times the ensemble model
    # concurrently,
    # which calls 2 models in parallel for each call. In total 4 models run
    # parallel.
    st = time.time()
    send_concurrent_model_requests()
    print("duration", time.time() - st)

    # Output
    # [[1, 2], [1, 2], [1, 2], [1, 2], [1, 2]]
    # duration 4.015406847000122
