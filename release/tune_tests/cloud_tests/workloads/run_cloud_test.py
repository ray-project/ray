import csv
import json
import os
import platform
import shutil
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from ray.tune.trial import Location
from ray.tune.trial_runner import _find_newest_ckpt
from ray.tune.utils.serialization import TuneFunctionDecoder

TUNE_SCRIPT = os.path.join(os.path.dirname(__file__), "_tune_script.py")


class TrialStub:
    def __init__(self, trainable_name: str, trial_id: str, status: str,
                 config: Dict[str, Any], local_dir: str, experiment_tag: str,
                 _last_result: Dict[str, Any], logdir: str,
                 *args, **kwargs,):
        self.trainable_name = trainable_name
        self.trial_id = trial_id
        self.status = status
        self.config = config
        self.local_dir = local_dir
        self.experiment_tag = experiment_tag
        self.last_result = _last_result
        self.logdir = logdir

        self.local_experiment_dir = None

        # Ignore remaining arguments

    @property
    def was_on_driver_node(self):
        return self.last_result["hostname"] == platform.node()

    def __hash__(self):
        return hash(self.trial_id)


@dataclass
class TrialCheckpointData:
    params: Dict[str, Any]
    results: List[Dict[str, Any]]
    progress: List[Dict[str, Any]]
    checkpoints: List[Tuple[str, Dict[Any, Any]]]


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


def assert_experiment_dir_exists(experiment_name: str) -> str:
    experiment_dir = os.path.join(
        os.path.expanduser("~/ray_results"), experiment_name)

    if not os.path.exists(experiment_dir):
        raise RuntimeError(
            f"Check failed: Experiment dir {experiment_dir} does not exist.")

    return experiment_dir


def load_experiment_checkpoint(
        experiment_dir: str) -> Tuple[Dict[str, Any], List[TrialStub]]:
    newest_ckpt_path = _find_newest_ckpt(experiment_dir)
    with open(newest_ckpt_path, "r") as f:
        runner_state = json.load(f, cls=TuneFunctionDecoder)

    trials = []
    for trial_cp_str in runner_state["checkpoints"]:
        parsed = json.loads(trial_cp_str, cls=TuneFunctionDecoder)
        trials.append(TrialStub(**parsed))

    runner_data = runner_state["runner_data"]

    return runner_data, trials


def assert_experiment_checkpoint_validity(
        experiment_dir: str,
        total_time_bounds: Optional[Tuple[float, float]] = None):
    runner_data, trials = load_experiment_checkpoint(experiment_dir)

    assert len(trials) == 4, "Not all trials have been created."

    assert all(trial.status == "RUNNING" for trial in trials), \
        "Not all trials are RUNNING"

    if total_time_bounds:
        assert (total_time_bounds[0]
                <= runner_data["_total_time"]
                <= total_time_bounds[1]), \
            f"Total time {runner_data['_total_time']} not within bounds " \
            f"({total_time_bounds[0]} <= {runner_data['_total_time']} <= " \
            f"{total_time_bounds[1]})"

    return runner_data, trials


def fetch_trials_to_tmp_dir(trials: List[TrialStub]) -> Dict[TrialStub, str]:
    dirmap = {}

    for trial in trials:
        tmpdir = tempfile.mkdtemp(prefix="tune_cloud_test")

        if trial.was_on_driver_node:
            # Trial was run on driver
            shutil.rmtree(tmpdir)
            shutil.copytree(trial.local_dir, tmpdir)
        else:
            # Trial was run on remote node
            raise RuntimeError(f"Cannot test right now: {trial.location.hostname}")

        dirmap[trial] = tmpdir

    return dirmap


def load_data_from_trial_checkpoints(trial_to_dir: Dict[TrialStub, str]) -> Dict[TrialStub, TrialCheckpointData]:
    trial_to_checkpoint_data = {}
    for trial, tmpdir in trial_to_dir.items():
        with open(os.path.join(tmpdir, "params.json"), "rt") as f:
            params = json.load(f)

        results = []
        with open(os.path.join(tmpdir, "result.json"), "rt") as f:
            for line in f.readlines():
                results.append(json.loads(line))

        with open(os.path.join(tmpdir, "progress.csv"), "rt") as f:
            reader = csv.DictReader(f)
            progress = list(reader)

        checkpoints = []
        for cp_dir in sorted(os.listdir(tmpdir)):
            if not cp_dir.startswith("checkpoint_"):
                continue
            with open(os.path.join(tmpdir, cp_dir, "checkpoint.json"), "rt") as f:
                checkpoint_data = json.load(f)
            checkpoints.append((cp_dir, checkpoint_data))

        trial_to_checkpoint_data[trial] = TrialCheckpointData(params=params, results=results, progress=progress, checkpoints=checkpoints)

    return trial_to_checkpoint_data


def test_no_sync():
    experiment_name = "cloud_no_sync"
    indicator_file = "/tmp/cloud_no_sync_indicator"

    def between_experiments():
        print("CHECK CHECK CHECK")
        experiment_dir = assert_experiment_dir_exists(
            experiment_name=experiment_name)

        _, trials = assert_experiment_checkpoint_validity(
            experiment_dir=experiment_dir, total_time_bounds=(120, 239))

        trial_to_tmpdir = fetch_trials_to_tmp_dir(trials)
        trial_checkpoint_data = load_data_from_trial_checkpoints(trial_to_tmpdir)

        print(trial_to_tmpdir)
        print(trial_checkpoint_data)

    between_experiments()

    # run_resume_flow(
    #     experiment_name=experiment_name,
    #     indicator_file=indicator_file,
    #     between_experiments_callback=between_experiments)


if __name__ == "__main__":
    test_no_sync()
