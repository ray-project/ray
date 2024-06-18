import ray
from ray._private import test_utils

SCALE_DOWN_ACC = "Removing 1 nodes of type fake-acc-group (idle)."


@test_utils.wait_for_stdout(strings_to_match=[SCALE_DOWN_ACC], timeout_s=25)
def main():
    """Removes CPU request, removes ACC actor.
    Waits for autoscaler scale-down events to get emitted to stdout.

    The worker idle timeout is set to 10 seconds and the autoscaler's update interval is
    5 seconds, so it should be enough to wait 15 seconds.
    An extra ten seconds are added to the timeout as a generous buffer against
    flakiness.
    """
    # Remove resource demands
    ray.autoscaler.sdk.request_resources(num_cpus=0)
    acc_actor = ray.get_actor("acc_actor")
    ray.kill(acc_actor)


if __name__ == "__main__":
    ray.init("auto", namespace="acc-test")
    main()
