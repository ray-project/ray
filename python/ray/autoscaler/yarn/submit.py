from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import skein

ray_start_script = "sleep infinity"


def main():
    client = skein.Client(address=None)

    # script should be the ray start command
    ray_master = skein.Master(resources=None, script=ray_start_script)
    worker_service = skein.Service(
        resources=skein.Resources(memory="1GiB", vcores=1),
        script="sleep 1000")
    spec = skein.ApplicationSpec(
        services={"ray-worker": worker_service},
        master=ray_master,
        name="ray-cluster")
    client.submit(spec)


if __name__ == "__main__":
    main()
