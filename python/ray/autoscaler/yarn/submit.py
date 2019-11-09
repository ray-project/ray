from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import skein

# ray_start_script = "source /venv/bin/activate && ray start --head --block --redis-port=6379 --object-manager-port=8076 --autoscaling-config=/ray_bootstrap_config.yaml"
ray_start_script = "/home/rayonyarn/miniconda3/bin/ray stop && /home/rayonyarn/miniconda3/bin/ray start --head --block --redis-port=6379 --object-manager-port=8076 --object-store-memory=104857600 --autoscaling-config=/ray_bootstrap_config.yaml"

def main():
    client = skein.Client(address=None)

    # script should be the ray start command
    ray_master = skein.Master(
        resources=skein.Resources(memory="500MiB", vcores=1),
        script=ray_start_script,
        files={"ray_bootstrap_config.yaml": "/home/rayonyarn/ray/python/ray/autoscaler/yarn/example-full.yaml"})
    worker_service = skein.Service(
        resources=skein.Resources(memory="1GiB", vcores=1),
        script="sleep infinity")
    spec = skein.ApplicationSpec(
        services={"ray-worker-service": worker_service},
        master=ray_master,
        name="ray-cluster")
    client.submit(spec)


if __name__ == "__main__":
    main()
