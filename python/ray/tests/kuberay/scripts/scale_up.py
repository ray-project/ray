import ray
from ray._private import test_utils


@test_utils.wait_for_stdout(
    strings_to_match=["Adding 1 node(s) of type small-group."], timeout_s=15
)
def main():
    """Submits CPU request.
    Wait 15 sec for autoscaler scale-up event to get emitted to stdout.

    The autoscaler update interval is 5 sec, so it should be enough to wait 5 seconds.
    An extra ten seconds are added to the timeout as a generous buffer against
    flakiness.
    """
    ray.autoscaler.sdk.request_resources(num_cpus=2)

    # get gcs address
    ctx = ray.get_runtime_context()

    for _ in range(15):
        import time

        print(ray.autoscaler.v2.sdk.get_cluster_status(ctx.gcs_address))

        time.sleep(1)


if __name__ == "__main__":
    ray.init("auto")
    main()
