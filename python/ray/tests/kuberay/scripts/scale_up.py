import ray
import time


def main():
    """Submits CPU request."""
    ray.autoscaler.sdk.request_resources(num_cpus=2)

    # Hold the driver for a bit to allow the message
    # "Adding 1 nodes of type small-group" to reach the driver's stdout.
    time.sleep(15)

    # Unexplained "time.sleep" is not good style, so here are some details:

    # This script is executed on the Ray head via kubectl in
    # tests/kuberay/test_autoscaling_e2e.py.
    # The test receives the stdout of this script and checks for presence of
    # the string "Adding 1 nodes of type small-group" in the output.
    # This validates piping of autoscaling events to driver stdout.

    # Since it takes some time for the autoscaler event to appear in stdout, and since
    # there isn't a convenient way to inspect stdout from within this script, we wait
    # long enough for the autoscaling event to be piped to stdout.

    # 15 seconds is overkill meant to avoid test flakiness.
    # It should take at most 5 seconds (the autoscaler update interval)
    # for the autoscaling event to be emitted to the driver.
    # (If the autoscaler event isn't emitted in that time frame,
    # it's a legitimate test failure.)


if __name__ == "__main__":
    ray.init("auto")
    main()
