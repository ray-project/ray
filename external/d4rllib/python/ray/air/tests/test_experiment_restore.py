import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ray.tune.analysis import ExperimentAnalysis
from ray.tune.result_grid import ResultGrid

_RUN_SCRIPT_FILENAME = "_test_experiment_restore_run.py"


def _kill_process_if_needed(
    process: subprocess.Popen, timeout_s: float = 10, poll_interval_s: float = 1.0
):
    """Kills a process if it hasn't finished in `timeout_s` seconds.
    Polls every `poll_interval_s` seconds to check if the process is still running."""
    kill_timeout = time.monotonic() + timeout_s
    while process.poll() is None and time.monotonic() < kill_timeout:
        time.sleep(poll_interval_s)
    if process.poll() is None:
        process.terminate()


def _print_message(message):
    sep = "=" * 50
    print(f"\n{sep}\n{message}\n{sep}\n")


@pytest.mark.parametrize("runner_type", ["tuner", "trainer"])
def test_experiment_restore(tmp_path, runner_type):
    """
    This is an integration stress test for experiment restoration.


    Test setup:

    - For Tuner.restore:
        - 8 trials, with a max of 2 running concurrently (--> 4 rounds of trials)
        - Each iteration takes 0.5 seconds
        - Each trial runs for 8 iterations --> 4 seconds
        - Each round of 2 trials should take 4 seconds
        - Without any interrupts/restoration:
            - Minimum runtime: 4 rounds * 4 seconds / round = 16 seconds
        - The test will stop the script with a SIGINT at a random time between
        6-10 iterations each restore.

    - For Trainer.restore:
        - 1 trial with 4 workers
        - Each iteration takes 0.5 seconds
        - Runs for 32 iterations --> Minimum runtime = 16 seconds
        - The test will stop the script with a SIGINT at a random time between
        6-10 iterations after each restore.

    Requirements:
    - Req 1: Training progress persisted
        - The experiment should progress monotonically.
        (The training iteration shouldn't go backward at any point)
        - Trials shouldn't start from scratch.
    - Req 2: Searcher state saved/restored correctly
    - Req 3: Callback state saved/restored correctly
    """

    np.random.seed(2023)

    script_path = Path(__file__).parent / _RUN_SCRIPT_FILENAME

    # Args to pass into the script as environment variables
    exp_name = f"{runner_type}_restore_integration_test"
    callback_dump_file = tmp_path / f"{runner_type}-callback_dump_file.json"
    storage_path = tmp_path / "ray_results"
    if storage_path.exists():
        shutil.rmtree(storage_path)

    csv_file = str(tmp_path / "dummy_data.csv")
    dummy_df = pd.DataFrame({"x": np.arange(128), "y": 2 * np.arange(128)})
    dummy_df.to_csv(csv_file)

    run_started_marker = tmp_path / "run_started_marker"

    time_per_iter_s = 0.5
    max_concurrent = 2

    if runner_type == "tuner":
        iters_per_trial = 8
        num_trials = 8
    elif runner_type == "trainer":
        iters_per_trial = 32
        num_trials = 1

    total_iters = iters_per_trial * num_trials

    env = os.environ.copy()
    env.update(
        {
            "RUNNER_TYPE": runner_type,
            "STORAGE_PATH": str(storage_path),
            "EXP_NAME": exp_name,
            "CALLBACK_DUMP_FILE": str(callback_dump_file),
            "RUN_STARTED_MARKER": str(run_started_marker),
            "TIME_PER_ITER_S": str(time_per_iter_s),
            "ITERATIONS_PER_TRIAL": str(iters_per_trial),
            "NUM_TRIALS": str(num_trials),
            "MAX_CONCURRENT_TRIALS": str(max_concurrent),
            "CSV_DATA_FILE": csv_file,
        }
    )

    # Variables used in the loop
    return_code = None
    total_runtime = 0
    run_iter = 0
    progress = 0
    progress_history = []

    poll_interval_s = 0.1
    test_start_time = time.monotonic()

    while True:
        run_started_marker.write_text("", encoding="utf-8")

        run = subprocess.Popen([sys.executable, script_path], env=env)
        run_iter += 1

        _print_message(f"Started run #{run_iter} w/ PID = {run.pid}")

        # Start the timer after the first trial has entered its training loop.
        while run.poll() is None and run_started_marker.exists():
            time.sleep(poll_interval_s)

        # If the run already finished, then exit immediately.
        if run.poll() is not None:
            return_code = run.poll()
            break

        timeout_s = np.random.uniform(6 * time_per_iter_s, 10 * time_per_iter_s)

        _print_message(
            "Training has started...\n"
            f"Interrupting after {timeout_s:.2f} seconds\n"
            f"Currently at {total_runtime:.2f} seconds"
        )

        # Sleep for a random amount of time, then stop the run.
        start_time = time.monotonic()
        time.sleep(timeout_s)
        total_runtime += time.monotonic() - start_time

        return_code = run.poll()
        if return_code is None:
            # Send "SIGINT" to stop the run
            _print_message(f"Sending SIGUSR1 to run #{run_iter} w/ PID = {run.pid}")
            run.send_signal(signal.SIGUSR1)

            # Make sure the process is stopped forcefully after a timeout.
            _kill_process_if_needed(run)
        else:
            _print_message("Run has already terminated!")
            break

        # Check up on the results.
        results = ResultGrid(ExperimentAnalysis(str(storage_path / exp_name)))
        iters = [result.metrics.get("training_iteration", 0) for result in results]
        progress = sum(iters) / total_iters
        progress_history.append(progress)
        _print_message(
            f"Number of trials = {len(results)}\n"
            f"% completion = {progress} ({sum(iters)} iters / {total_iters})\n"
            f"Currently at {total_runtime:.2f} seconds"
        )

    _print_message(
        f"Total number of restorations = {run_iter}\n"
        f"Total runtime = {total_runtime:.2f}\n"
        f"Return code = {return_code}"
    )
    test_end_time = time.monotonic()

    assert progress == 1.0

    # The script shouldn't have errored. (It should have finished by this point.)
    assert return_code == 0, (
        f"The script errored with return code: {return_code}.\n"
        f"Check the `{_RUN_SCRIPT_FILENAME}` script for any issues. "
    )

    # Req 1: training progress persisted
    # Check that progress increases monotonically (we never go backwards/start from 0)
    assert np.all(np.diff(progress_history) >= 0), (
        "Expected progress to increase monotonically. Instead, got:\n"
        "{progress_history}"
    )

    # Req 2: searcher state
    results = ResultGrid(ExperimentAnalysis(str(storage_path / exp_name)))
    # Check that all trials have unique ids assigned by the searcher (if applicable)
    ids = [result.config.get("id", -1) for result in results]
    ids = [id for id in ids if id >= 0]
    if ids:
        assert sorted(ids) == list(range(1, num_trials + 1)), (
            "Expected the searcher to assign increasing id for each trial, but got:"
            f"{ids}"
        )

    # Req 3: callback state
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

    _print_message(f"Success! Test took {test_end_time - test_start_time:.2f} seconds.")


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
