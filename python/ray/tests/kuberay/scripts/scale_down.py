import ray
from ray._private import test_utils
import io
import sys

SCALE_DOWN_CPU = "Removing 1 nodes of type fake-gpu-group (idle)."
SCALE_DOWN_GPU = "Removing 2 nodes of type small-group (idle)."


def main():
    """Removes CPU request, removes GPU actor.
    Waits for autoscaler scale-down events to get emitted to stdout."""
    # Redirect stdout to a StringIO object.
    out_stream = io.StringIO()
    sys.stdout = out_stream

    # Remove resource demands
    ray.autoscaler.sdk.request_resources(num_cpus=0)
    gpu_actor = ray.get_actor("gpu_actor")
    ray.kill(gpu_actor)

    def scale_events_logged() -> bool:
        """Return True if the scale down event messages are in the out_stream.
        Otherwise, raise an Exception.
        """
        stdout_so_far = out_stream.getvalue()
        if (SCALE_DOWN_CPU in stdout_so_far) and (SCALE_DOWN_GPU in stdout_so_far):
            return True
        else:
            # This error will be logged to stderr once if SCALE_MESSAGE doesn't
            # appear in STDOUT in time.
            raise RuntimeError(
                "Expected autoscaler events not  detected. Driver stdout follows:\n"
                + stdout_so_far
            )

    # Wait for the expected autoscaler event message to appear.
    # It should take at most (15 + epsilon) seconds for this to happen.
    # [10 seconds idle timeout + 5 sec autoscaler poll interval + processing time.]
    # To prevent flakiness, use a large timeout of 25 seconds.
    test_utils.wait_for_condition(
        condition_predictor=scale_events_logged, timeout=25, retry_interval_ms=1500
    )

    # Reset stdout and print driver logs.
    sys.stdout = sys.__stdout__
    print("Expected autoscaler event detected. Driver stdout follows:")
    print(out_stream.getvalue())

    out_stream.close()


if __name__ == "__main__":
    ray.init("auto", namespace="gpu-test")
    main()
