import ray
from ray._private import test_utils
import io
import sys

SCALE_MESSAGE = "Adding 1 nodes of type small-group."


def main():
    """Submits CPU request.
    Waits for autoscaler scale-up event to get emitted to stdout."""
    # Redirect stdout to a StringIO object.
    out_stream = io.StringIO()
    sys.stdout = out_stream

    # Make the scale request
    ray.autoscaler.sdk.request_resources(num_cpus=2)

    def scale_event_logged() -> bool:
        """Return True if the scale up event message is in the out_stream.
        Otherwise, raise an Exception.
        """
        stdout_so_far = out_stream.getvalue()
        if SCALE_MESSAGE in out_stream.getvalue():
            return True
        else:
            # This error will be logged to stderr once if SCALE_MESSAGE doesn't
            # appear in STDOUT in time.
            raise RuntimeError(
                "Expected autoscaler event not  detected. Driver stdout follows:\n"
                + stdout_so_far
            )

    # Wait for the expected autoscaler event message to appear.
    # It should take at most (5 + epsilon) seconds for this to happen.
    # To prevent flakiness, use a large timeout of 15 seconds.
    test_utils.wait_for_condition(
        condition_predictor=scale_event_logged, timeout=15, retry_interval_ms=1500
    )

    # Reset stdout and print driver logs.
    sys.stdout = sys.__stdout__
    print("Expected autoscaler event detected. Driver stdout follows:")
    print(out_stream.getvalue())

    out_stream.close()


if __name__ == "__main__":
    ray.init("auto")
    main()
