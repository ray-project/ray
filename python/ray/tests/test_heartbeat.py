import time

import ray
from ray.monitor import Monitor


def verify_load_metrics(monitor, expected_resource_usage=None, timeout=10):
    while True:
        monitor.process_messages()
        resource_usage = monitor.load_metrics.get_resource_usage()

        if expected_resource_usage is None:
            if all(x for x in resource_usage[1:]):
                break
        elif all(x == y
                 for x, y in zip(resource_usage, expected_resource_usage)):
            break
        else:
            timeout -= 1
            time.sleep(2)

        if timeout <= 0:
            raise ValueError("Timeout. {} != {}".format(
                resource_usage, expected_resource_usage))

    return resource_usage


def test_heartbeats():
    """Unit test for `Cluster.wait_for_nodes`.

    Test proper metrics.
    """
    redis_address = ray.init(num_cpus=1)["redis_address"]
    monitor = Monitor(redis_address, None)
    monitor.subscribe(ray.gcs_utils.XRAY_HEARTBEAT_BATCH_CHANNEL)
    monitor.subscribe(ray.gcs_utils.XRAY_JOB_CHANNEL)
    monitor.update_raylet_map()
    monitor._maybe_flush_gcs()

    verify_load_metrics(monitor, (0.0, {"CPU": 0.0}, {"CPU": 1.0}))
    work_handles = []
    timeout = 20

    @ray.remote
    def work(timeout=10):
        time.sleep(timeout)
        return True

    work_handles += [work.remote(timeout=timeout * 2)]
    verify_load_metrics(
        monitor, (1.0, {
            "CPU": 1.0
        }, {
            "CPU": 1.0
        }), timeout=timeout)
    ray.get(work_handles)

    work_handles = []

    @ray.remote
    class Actor(object):
        def work(self, timeout=10):
            time.sleep(timeout)
            return True

    test_actors = [Actor.remote()]
    work_handles += [test_actors[0].work.remote(timeout=timeout * 2)]

    verify_load_metrics(monitor, (1.0, {"CPU": 1.0}, {"CPU": 1.0}))

    ray.get(work_handles)


if __name__ == "__main__":
    test_heartbeats()
