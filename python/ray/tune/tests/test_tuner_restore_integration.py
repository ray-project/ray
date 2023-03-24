import numpy as np
from pathlib import Path
import pytest
import time
import signal
import subprocess
import sys

import ray


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def kill_process(process, timeout=10):
    kill_timeout = time.monotonic() + 10
    while process.poll() is None and time.monotonic() < kill_timeout:
        time.sleep(1)
    if process.poll() is None:
        process.terminate()


def test_tuner_restore_integration(ray_start_4_cpus, tmp_path):
    script_path = Path(__file__).parent / "_test_tuner_restore.py"

    # 6 trials, each running one at a time
    # Each trial runs for 8 iterations
    # Each iteration takes 0.5 seconds
    # 4 seconds per trial --> 24 seconds in total

    errors_to_test = [""] * 10

    theoretical_runtime = 24.0
    passing_factor = 1.5
    passing_runtime = theoretical_runtime * passing_factor
    print(f"Experiment should with a total runtime of less than {passing_runtime}.")

    return_code = None
    runtime_so_far = 0
    run_iter = 0
    while runtime_so_far < passing_runtime:
        run_started_marker = tmp_path / "run_started_marker"
        # run_ended_marker = tmp_path / "run_started_marker"

        run_started_marker.write_text("", encoding="utf-8")
        # run_ended_marker.write_text("", encoding="utf-8")

        time_per_iter_s = 0.5
        env = {
            "STORAGE_PATH": str(tmp_path / "ray_results"),
            "TIME_PER_ITER_S": str(time_per_iter_s),
            "CALLBACK_DUMP_DIR": str(tmp_path / "callback_dump_dir"),
            "RUN_STARTED_MARKER": str(run_started_marker),
        }
        run = subprocess.Popen(
            [sys.executable, script_path], env=env  # , stderr=subprocess.PIPE
        )
        run_iter += 1
        print(f"Started run #{run_iter}:", run.pid)

        # Start the timer after the first trial has entered its training loop.
        while run.poll() is None and run_started_marker.exists():
            time.sleep(0.05)

        # If the run finished before any trials started, then exit immediately.
        if run.poll() is not None:
            return_code = run.poll()
            break

        timeout = min(
            np.random.uniform(3 * time_per_iter_s, 6 * time_per_iter_s),
            passing_runtime - runtime_so_far,
        )

        print("\n")
        print("=" * 40)
        print("Training has started...")
        print(f"Interrupting after {timeout:.2f} s")
        print(f"Currently at {runtime_so_far:.2f}/{passing_runtime}")
        print("=" * 40)
        print("\n")

        time.sleep(timeout)
        runtime_so_far += timeout

        if run.poll() is None:
            # Send "SIGINT" to stop the run
            print("Sending SIGUSR1 to process", run.pid)
            run.send_signal(signal.SIGUSR1)

            # Make sure the process is stopped forcefully after a timeout.
            kill_process(run)
        else:
            print("Run already terminated!")
            return_code = run.poll()
            assert return_code
            break

    print("\nTotal number of runs:", run_iter)
    print("Total runtime:", runtime_so_far)
    print("Return code:", return_code)

    # errors_to_test = [
    #     # All (non-trivial) Tune loop hooks
    #     # Crash on 4
    #     "on_step_begin",
    #     # Resume from 4, trial failure, restore, crash on 8
    #     "on_step_end",
    #     # Resume from 8
    #     "on_trial_start",
    #     "on_trial_restore",
    #     "on_trial_save",
    #     "on_trial_result",
    #     "on_trial_complete",
    #     "on_trial_error",
    #     # Test w/ SIGINT randomly between other error locations.
    #     "",
    #     "",
    # ]

    # for error_on in errors_to_test:
    #     print("\n\n")
    #     print("=" * 40)
    #     print("Testing error on: ", error_on)
    #     print("=" * 40)
    #     print("\n\n")

    #     run_started_marker = tmp_path / "run_started_marker"
    #     run_ended_marker = tmp_path / "run_started_marker"

    #     run_started_marker.write_text("", encoding="utf-8")
    #     run_ended_marker.write_text("", encoding="utf-8")

    #     env = {
    #         "FAIL_ON": error_on,
    #         "STORAGE_PATH": str(tmp_path / "ray_results"),
    #         # "EXP_NAME": "",
    #         "TIME_PER_ITER_S": str(0.5),
    #         "CALLBACK_DUMP_DIR": str(tmp_path / "callback_dump_dir"),
    #         "RUN_STARTED_MARKER": str(run_started_marker),
    #         "RUN_ENDED_MARKER": str(run_ended_marker),
    #     }
    #     run = subprocess.Popen(
    #         [sys.executable, script_path], env=env  # , stderr=subprocess.PIPE
    #     )
    #     print("Started run:", run.pid)

    #     while run_started_marker.exists():
    #         time.sleep(0.05)

    #     timeout = 4 * 0.5
    #     if error_on != "":
    #         timeout *= 2

    #     start = time.time()
    #     time.sleep(timeout)
    #     while run.poll() is None and time.time() - start <= timeout:
    #         time.sleep(0.1)

    #     elapsed = time.time() - start
    #     print(f"Ran for {time.time() - start} seconds!\n")
    #     total_runtime += elapsed

    #     if run.poll() is None:
    #         # Send "SIGINT" to stop the run
    #         print("Sending SIGUSR1 to process", run.pid)
    #         run.send_signal(signal.SIGUSR1)

    #         # Make sure the process is stopped forcefully after a timeout.
    #         kill_timeout = time.monotonic() + 10
    #         while run.poll() is None and time.monotonic() < kill_timeout:
    #             time.sleep(1)
    #         if run.poll() is None:
    #             run.terminate()
    #     else:
    #         print("Run already terminated!")

    # print("\nTotal runtime:", total_runtime)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
