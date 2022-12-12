import ray
from ray.cluster_utils import AutoscalingCluster

cluster = AutoscalingCluster(
    head_resources={"CPU": 8},
    worker_node_types={
        "cpu_node": {
            "resources": {
                "CPU": 1,
            },
            "node_config": {},
            "min_workers": 1,
            "max_workers": 1,
        },
        "gpu_node": {
            "resources": {
                "CPU": 2,
                "GPU": 1,
            },
            "node_config": {},
            "min_workers": 0,
            "max_workers": 0,
        },
    },
    idle_timeout_minutes=30,
)

cluster.start()
ray.init()

@ray.remote(num_cpus=1)
class A:
    pass

a = [A.remote() for _ in range(3)]
import time
time.sleep(3000)