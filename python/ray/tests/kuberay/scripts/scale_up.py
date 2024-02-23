import ray
from ray._private.test_utils import wait_for_condition


def main():
    """Submits CPU request.
    Wait 15 sec for autoscaler scale-up event to get emitted to stdout.

    The autoscaler update interval is 5 sec, so it should be enough to wait 5 seconds.
    An extra ten seconds are added to the timeout as a generous buffer against
    flakiness.
    """
    ray.autoscaler.sdk.request_resources(num_cpus=2)

    def verify():
        cluster_resources = ray.cluster_resources()
        assert cluster_resources.get("CPU", 0) == 2

        return True

    wait_for_condition(verify, timeout=15, retry_interval_ms=1000)
    


if __name__ == "__main__":
    ray.init("auto")
    main()
