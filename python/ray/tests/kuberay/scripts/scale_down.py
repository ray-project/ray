import ray
from ray._private import test_utils


def main():
    """Removes CPU request, removes GPU actor.
    Waits for autoscaler scale-down events to get emitted to stdout.

    The worker idle timeout is set to 10 seconds and the autoscaler's update interval is
    5 seconds, so it should be enough to wait 15 seconds.
    """

    # Before scale-down.
    cluster_resources = ray.cluster_resources()
    assert cluster_resources.get("CPU", 0) > 0, cluster_resources
    assert cluster_resources.get("GPU", 0) > 0, cluster_resources

    # Remove resource demands
    ray.autoscaler.sdk.request_resources(num_cpus=0)
    gpu_actor = ray.get_actor("gpu_actor")
    ray.kill(gpu_actor)

    # Wait for scale-down to happen.
    def verify():
        cluster_resources = ray.cluster_resources()
        # From head node
        assert cluster_resources.get("CPU", 0) == 1, cluster_resources
        assert cluster_resources.get("GPU", 0) == 0, cluster_resources

        return True

    test_utils.wait_for_condition(verify, timeout=60, retry_interval_ms=2000)


if __name__ == "__main__":
    ray.init("auto", namespace="gpu-test")
    main()
