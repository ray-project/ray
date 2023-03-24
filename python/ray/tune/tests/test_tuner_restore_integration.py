"""
This test is meant to be an integration stress test for experiment restoration.

Test setup:
- 8 trials, with a max of 2 running concurrently (--> 4 rounds of trials)
- Each iteration takes 0.5 seconds
- Each trial runs for 8 iterations --> 4 seconds
- Each round of 2 trials should take 4 seconds
- Without any interrupts/restoration:
    - Minimum runtime: 4 rounds * 4 seconds / round = 16 seconds
    - Actually running it w/o interrupts = ~24 seconds
- The test will stop the script with a SIGINT at a random time between
  4-8 iterations after restoring.

Requirements:
- With interrupts, the experiment should finish within 1.5 * 24 = 36 seconds.
    - 1.5x is the passing threshold.
"""
import numpy as np
from pathlib import Path
import pytest
import time
import signal
import subprocess
import sys

import ray

from ray.tune.result_grid import ResultGrid
from ray.tune.analysis import ExperimentAnalysis


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def kill_process(process, timeout_s=10):
    kill_timeout = time.monotonic() + timeout_s
    while process.poll() is None and time.monotonic() < kill_timeout:
        time.sleep(1)
    if process.poll() is None:
        process.terminate()


def print_message(message):
    print("\n")
    print("=" * 50)
    print(message)
    print("=" * 50)
    print("\n")


def test_tuner_restore_integration(ray_start_4_cpus, tmp_path):
    np.random.seed(2023)

    script_path = Path(__file__).parent / "_test_tuner_restore.py"

    # Args to pass into the script as environment variables
    exp_name = "tuner_restore_integration_test"
    storage_path = tmp_path / "ray_results"
    iters_per_trial = 8
    num_trials = 8
    total_iters = iters_per_trial * num_trials
    max_concurrent = 2

    # Pass criteria
    no_interrupts_runtime = 24.0
    passing_factor = 1.5
    passing_runtime = no_interrupts_runtime * passing_factor
    print(f"Experiment should finish with a total runtime <= {passing_runtime}.")

    # Variables used in the loop
    return_code = None
    total_runtime = 0
    run_iter = 0
    progress_history = []

    while total_runtime < passing_runtime:
        run_started_marker = tmp_path / "run_started_marker"
        run_started_marker.write_text("", encoding="utf-8")

        time_per_iter_s = 0.5
        env = {
            "STORAGE_PATH": str(storage_path),
            "EXP_NAME": exp_name,
            "TIME_PER_ITER_S": str(time_per_iter_s),
            "CALLBACK_DUMP_DIR": str(tmp_path / "callback_dump_dir"),
            "RUN_STARTED_MARKER": str(run_started_marker),
        }
        run = subprocess.Popen(
            [sys.executable, script_path], env=env  # , stderr=subprocess.PIPE
        )
        run_iter += 1

        print_message(f"Started run #{run_iter} w/ PID = {run.pid}")

        # Start the timer after the first trial has entered its training loop.
        while run.poll() is None and run_started_marker.exists():
            time.sleep(0.05)

        # If the run already finished, then exit immediately.
        if run.poll() is not None:
            return_code = run.poll()
            break

        timeout = min(
            np.random.uniform(4 * time_per_iter_s, 8 * time_per_iter_s),
            passing_runtime - total_runtime,
        )

        print_message(
            "Training has started...\n"
            f"Interrupting after {timeout:.2f} s\n"
            f"Currently at {total_runtime:.2f}/{passing_runtime} seconds"
        )

        time.sleep(timeout)
        total_runtime += timeout

        if run.poll() is None:
            # Send "SIGINT" to stop the run
            print_message(f"Sending SIGUSR1 to run #{run_iter} w/ PID = {run.pid}")
            run.send_signal(signal.SIGUSR1)

            # Make sure the process is stopped forcefully after a timeout.
            kill_process(run)
        else:
            print_message("Run has already terminated!")
            return_code = run.poll()
            assert return_code
            break

        # Check up on the results.
        results = ResultGrid(ExperimentAnalysis(str(storage_path / exp_name)))
        iters = [result.metrics.get("training_iteration", 0) for result in results]
        progress = sum(iters) / total_iters
        progress_history.append(progress)
        print_message(
            f"Number of trials = {len(results)}\n"
            f"% completion = {progress} ({sum(iters)} iters / {total_iters})\n"
            f"Currently at {total_runtime:.2f}/{passing_runtime} seconds"
        )

    print("Total number of restorations:", run_iter)
    print("Total runtime:", total_runtime)
    print("Return code:", return_code)

    assert total_runtime < passing_runtime
    assert return_code == 0
    # Check that progress increases monotonically (we never go backwards/start from 0)
    assert np.all(np.diff(progress_history))


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
