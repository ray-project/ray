import ray
from ray._common.test_utils import wait_for_condition


def main():
    """Submits CPU request"""
    ray.autoscaler.sdk.request_resources(num_cpus=2)
    from ray.autoscaler.v2.sdk import get_cluster_status
    from ray.autoscaler.v2.utils import ClusterStatusFormatter

    gcs_address = ray.get_runtime_context().gcs_address

    def verify():
        cluster_resources = ray.cluster_resources()

        cluster_status = get_cluster_status(gcs_address)
        print(ClusterStatusFormatter.format(cluster_status, verbose=True))
        assert cluster_resources.get("CPU", 0) == 2, cluster_resources

        return True

    wait_for_condition(verify, timeout=60, retry_interval_ms=2000)


if __name__ == "__main__":
    ray.init("auto")
    main()
