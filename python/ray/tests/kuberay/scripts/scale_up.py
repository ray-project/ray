import ray
import time


def main():
    """Submits CPU request."""
    ray.autoscaler.sdk.request_resources(num_cpus=2)
    # Hold the driver for a bit to allow the message
    # "Adding 1 nodes of type small-group" to reach the driver.
    time.sleep(15)
    # 15 seconds is overkill meant to avoid test flakiness.
    # It should take at most 5 seconds (the autoscaler update interval)
    # for the autoscaling event to be emitted to the driver.


if __name__ == "__main__":
    ray.init("auto")
    main()
