from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import skein

ray_start_script = "ray start --head --redis-port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml"


def main():
    client = skein.Client(address=None)

    # script should be the ray start command
    ray_master = skein.Master(
        resources=None,
        script=ray_start_script,
        files={"ray_bootstrap_config.yaml": "example-full.yaml"})
    worker_service = skein.Service(
        resources=skein.Resources(memory="1GiB", vcores=1),
        script="sleep infinity")
    spec = skein.ApplicationSpec(
        services={"ray-worker": worker_service},
        master=ray_master,
        name="ray-cluster")
    client.submit(spec)


if __name__ == "__main__":
    main()
