"""
This test is meant to be an integration stress test for experiment restoration.

Test setup:
- 8 trials, with a max of 2 running concurrently (--> 4 rounds of trials)
- Each iteration takes 0.5 seconds
- Each trial runs for 8 iterations --> 4 seconds
- Each round of 2 trials should take 4 seconds
- Without any interrupts/restoration:
    - Minimum runtime: 4 rounds * 4 seconds / round = 16 seconds
    - Actually running it without any interrupts = ~24 seconds
- The test will stop the script with a SIGINT at a random time between
  4-8 iterations after restoring.

Requirements:
- Req 1: Reasonable runtime
    - The experiment should finish within 1.5 * 24 = 36 seconds.
    - 1.5x is the passing threshold.
- Req 2: Training progress persisted
    - The experiment should progress monotonically.
    - The experiment shouldn't "go backward" at any point.
    - Trials shouldn't start from scratch.
- Req 3: Searcher state saved/restored correctly
- Req 4: Callback state saved/restored correctly
"""
import json
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


_RUN_SCRIPT_FILENAME = "_test_experiment_restore_run.py"


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def kill_process_if_needed(process, timeout_s=10):
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


# TODO(ml-team): "trainer" doesn't work
@pytest.mark.parametrize("runner_type", ["tuner", "trainer"])
def test_air_experiment_restore(tmp_path, runner_type):
    np.random.seed(2023)

    script_path = Path(__file__).parent / _RUN_SCRIPT_FILENAME

    # Args to pass into the script as environment variables
    exp_name = f"{runner_type}_restore_integration_test"
    callback_dump_file = tmp_path / f"{runner_type}-callback_dump_file.json"
    storage_path = tmp_path / "ray_results"
    if storage_path.exists():
        import shutil

        shutil.rmtree(storage_path)

    run_started_marker = tmp_path / "run_started_marker"

    time_per_iter_s = 0.5
    max_concurrent = 2

    if runner_type == "tuner":
        iters_per_trial = 8
        num_trials = 8
    elif runner_type == "trainer":
        iters_per_trial = 64
        num_trials = 1

    total_iters = iters_per_trial * num_trials

    env = {
        "RUNNER_TYPE": runner_type,
        "STORAGE_PATH": str(storage_path),
        "EXP_NAME": exp_name,
        "CALLBACK_DUMP_FILE": str(callback_dump_file),
        "RUN_STARTED_MARKER": str(run_started_marker),
        "TIME_PER_ITER_S": str(time_per_iter_s),
        "ITERATIONS_PER_TRIAL": str(iters_per_trial),
        "NUM_TRIALS": str(num_trials),
        "MAX_CONCURRENT_TRIALS": str(max_concurrent),
    }

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
        run_started_marker.write_text("", encoding="utf-8")

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
            f"Interrupting after {timeout:.2f} seconds\n"
            f"Currently at {total_runtime:.2f}/{passing_runtime} seconds"
        )

        # Sleep for a random amount of time, then stop the run.
        time.sleep(timeout)
        total_runtime += timeout

        if run.poll() is None:
            # Send "SIGINT" to stop the run
            print_message(f"Sending SIGUSR1 to run #{run_iter} w/ PID = {run.pid}")
            run.send_signal(signal.SIGUSR1)

            # Make sure the process is stopped forcefully after a timeout.
            kill_process_if_needed(run)
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

    print_message(
        f"Total number of restorations = {run_iter}\n"
        f"Total runtime = {total_runtime}\n"
        f"Return code = {return_code}"
    )

    # The script shouldn't have errored. (It should have finished by this point.)
    assert return_code == 0, (
        f"The script errored with return code: {return_code}.\n"
        f"Check the `{_RUN_SCRIPT_FILENAME}` script for any issues."
    )

    # Req 1: runtime
    assert (
        total_runtime <= passing_runtime
    ), f"Expected runtime to be <= {passing_runtime}, but ran for: {total_runtime}"

    # Req 2: training progress persisted
    # Check that progress increases monotonically (we never go backwards/start from 0)
    assert np.all(np.diff(progress_history) >= 0), (
        "Expected progress to increase monotonically. Instead, got:\n"
        "{progress_history}"
    )

    # Req 3: searcher state
    results = ResultGrid(ExperimentAnalysis(str(storage_path / exp_name)))
    # Check that all trials have unique ids assigned by the searcher
    ids = [result.config["id"] for result in results]
    assert sorted(ids) == list(range(1, num_trials + 1)), (
        "Expected the searcher to assign increasing id for each trial, but got:"
        f"{ids}"
    )

    # Req 4: callback state
    with open(callback_dump_file, "r") as f:
        callback_state = json.load(f)

    trial_iters = callback_state["trial_iters"]
    for iters in trial_iters.values():
        # Check that the callback has data for each trial, for all iters
        # NOTE: There may be some duplicate data, due to the fact that
        # the callback will be updated on every `on_trial_result` hook,
        # but the trial may crash before the corresponding checkpoint gets processed.
        assert sorted(set(iters)) == list(
            range(1, iters_per_trial + 1)
        ), f"Expected data from all iterations, but got: {iters}"

    print_message("Success!")


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
