import os
import shutil
import signal
import subprocess
import time
from typing import Callable, Optional

TUNE_SCRIPT = os.path.join(os.path.dirname(__file__), "_tune_script.py")


def delete_file_if_exists(filename: str):
    if os.path.exists(filename):
        os.remove(filename)


def cleanup_driver_experiment_dir(experiment_name: str):
    experiment_dir = os.path.join(
        os.path.expanduser("~/ray_results"), experiment_name)
    if os.path.exists(experiment_dir):
        print("Removing existing experiment dir:", experiment_dir)
        shutil.rmtree(experiment_dir)


def start_run(
        sync_to_driver: bool,
        upload_dir: Optional[str] = None,
        durable: bool = False,
        experiment_name: str = "cloud_test",
        indicator_file: str = "/tmp/tune_cloud_indicator",
) -> subprocess.Popen:
    args = []
    if sync_to_driver:
        args.append("--sync-to-driver")

    if upload_dir:
        args.extend(["--upload-dir", upload_dir])

    if durable:
        args.append("--durable")

    if experiment_name:
        args.extend(["--experiment-name", experiment_name])

    if indicator_file:
        args.extend(["--indicator-file", indicator_file])

    process = subprocess.Popen(["python", TUNE_SCRIPT] + args)

    return process


def wait_for_run_or_raise(process: subprocess.Popen,
                          indicator_file: str,
                          timeout: int = 30):
    print(f"Waiting up to {timeout} seconds until trials have been started "
          f"(indicated by existence of `{indicator_file}`)")

    timeout = time.monotonic() + timeout
    while (process.poll() is None and time.monotonic() < timeout
           and not os.path.exists(indicator_file)):
        time.sleep(1)

    if not os.path.exists(indicator_file):
        process.terminate()

        raise RuntimeError(
            f"Indicator file `{indicator_file}` still doesn't exist, "
            f"indicating that trials have not been started. "
            f"Please check the process output.")

    print("Process started, trials are running")


def send_signal_after_wait(process: subprocess.Popen,
                           signal: int,
                           wait: int = 30):
    print(f"Waiting {wait} seconds until sending signal {signal} "
          f"to process ")

    time.sleep(wait)

    if process.poll() is not None:
        raise RuntimeError("Process already terminated.")

    print(f"Sending signal {signal} to process")
    process.send_signal(signal)


def wait_until_process_terminated(process: subprocess.Popen,
                                  timeout: int = 60):
    print(f"Waiting up to {timeout} seconds until process terminated")

    timeout = time.monotonic() + timeout
    while process.poll() is None and time.monotonic() < timeout:
        time.sleep(1)

    if process.poll() is None:
        process.terminate()

        raise RuntimeError(
            "Process did not terminate within timeout, terminating "
            "forcefully instead.")

    print("Process terminated.")


def run_tune_script_for_time(
        run_time: int,
        experiment_name: str,
        indicator_file: str,
):
    # Start run
    process = start_run(
        sync_to_driver=False,
        upload_dir=None,
        durable=False,
        experiment_name=experiment_name,
        indicator_file=indicator_file,
    )
    try:
        # Wait until indicator file exists
        wait_for_run_or_raise(
            process, indicator_file=indicator_file, timeout=30)
        # Stop experiment (with checkpoint) after some time
        send_signal_after_wait(process, signal=signal.SIGINT, wait=run_time)
        # Wait until process gracefully terminated
        wait_until_process_terminated(process, timeout=30)
    finally:
        process.terminate()


def run_resume_flow(
        experiment_name: str,
        indicator_file: str,
        before_experiments_callback: Optional[Callable[[], None]] = None,
        between_experiments_callback: Optional[Callable[[], None]] = None,
        after_experiments_callback: Optional[Callable[[], None]] = None):
    # Cleanup ~/ray_results/<experiment_name> folder
    cleanup_driver_experiment_dir(experiment_name)

    # Run before experiment callbacks
    if before_experiments_callback:
        before_experiments_callback()

    # Delete indicator file
    delete_file_if_exists(indicator_file)

    # Run tune script for 30 seconds
    run_tune_script_for_time(
        run_time=30,
        experiment_name=experiment_name,
        indicator_file=indicator_file)

    # Before we restart, run a couple of checks
    # Run before experiment callbacks
    if between_experiments_callback:
        between_experiments_callback()

    # Restart. First, clean up indicator file
    delete_file_if_exists(indicator_file)

    # Start run again, run for another 30 seconds
    run_tune_script_for_time(
        run_time=30,
        experiment_name=experiment_name,
        indicator_file=indicator_file)

    if after_experiments_callback:
        after_experiments_callback()


def test_no_sync():
    experiment_name = "cloud_no_sync"
    indicator_file = "/tmp/cloud_no_sync_indicator"

    def between_experiments():
        print("CHECK CHECK CHECK")
        time.sleep(5)

    run_resume_flow(
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        between_experiments_callback=between_experiments)


if __name__ == "__main__":
    test_no_sync()
