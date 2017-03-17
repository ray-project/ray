import os
import ray

class Env(object):
    def __init__(self, default_num_workers=4):
        if "RAY_BENCHMARK_ENVIRONMENT" in os.environ:
            benchmark_environment = os.environ["RAY_BENCHMARK_ENVIRONMENT"]
            if benchmark_environment == "stress":
                # require RAY_NUM_WORKERS and RAY_REDIS_ADDRESS
                if not ("RAY_REDIS_ADDRESS" in os.environ and "RAY_NUM_WORKERS" in os.environ):
                    raise RuntimeError("RAY_BENCHMARK_ENVIRONMENT=stress")
            else:
                raise RuntimeError("Unknown benchmark environment: {}".format(benchmark_environment))

        if "RAY_NUM_WORKERS" in os.environ:
            self.num_workers = int(os.environ["RAY_NUM_WORKERS"])
        else:
            self.num_workers = default_num_workers

        if 'RAY_REDIS_ADDRESS' in os.environ:
            self.redis_address = os.environ['RAY_REDIS_ADDRESS']
        else:
            self.redis_address = None

        if "RAY_BENCHMARK_ITERATION" in os.environ:
            self.benchmark_iteration = int(os.environ["RAY_BENCHMARK_ITERATION"])
        else:
            self.benchmark_iteration = 0


    def ray_init(self):
        if self.redis_address:
            self.address_info = ray.init(redis_address=self.redis_address)
        else:
            self.address_info = ray.init(num_workers=self.num_workers)
        return self.address_info
