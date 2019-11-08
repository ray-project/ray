from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import skein


# ray_start_script = "ulimit -n 65536; ray start --head --num-cpus=$MY_CPU_REQUEST --redis-port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml"
ray_start_script = "ls"


def main():
    client = skein.Client(address=None)

    # script should be the ray start command
    ray_master = skein.Master(resources=None, script=ray_start_script)
    worker_service = skein.Service(
        resources=skein.Resources(memory="1GiB", vcores=1),
        script="sleep 1000")
    spec = skein.ApplicationSpec(
        services={"ray_worker": worker_service},
        master=ray_master,
        name="ray_cluster")
    client.submit(spec)


if __name__ == '__main__':
    main()
