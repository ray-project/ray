import os
import subprocess
import sys
import time

here = os.path.dirname(os.path.abspath(__file__))
run_autoscaler_script = os.path.join(here, "run_autoscaler.py")

BACKOFF_S = 5

if __name__ == "__main__":
    """Keep trying to start the autoscaler until it runs.
    We need to retry until the Ray head is running.

    This script also has the effect of restarting the autoscaler if it fails.

    Autoscaler-starting attempts are run in subprocesses out of fear that a
    failed Monitor.start() attempt could leave dangling half-initialized global
    Python state.
    """
    while True:
        try:
            # We are forwarding all the command line arguments of
            # run_autoscaler_with_retries.py to run_autoscaler.py.
            subprocess.run(["python", f"{run_autoscaler_script}"] +
                           sys.argv[1:])  # noqa: B1
        except subprocess.SubprocessError:
            print(f"Restarting autoscaler in {BACKOFF_S} seconds.")
            time.sleep(BACKOFF_S)
