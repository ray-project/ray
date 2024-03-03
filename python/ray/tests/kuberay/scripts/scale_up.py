import ray
from ray._private.test_utils import wait_for_condition


def main():
    """Submits CPU request"""
    ray.autoscaler.sdk.request_resources(num_cpus=2)

    def verify():
        cluster_resources = ray.cluster_resources()
        assert cluster_resources.get("CPU", 0) == 2, cluster_resources

        return True

    wait_for_condition(verify, timeout=60, retry_interval_ms=2000)


if __name__ == "__main__":
    ray.init("auto")
    main()
