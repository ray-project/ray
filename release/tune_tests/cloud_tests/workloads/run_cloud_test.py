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
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import ray
from ray.tune.syncer import NodeSyncer, detect_sync_to_driver, get_node_syncer
from ray.tune.trial_runner import _find_newest_ckpt
from ray.tune.utils.serialization import TuneFunctionDecoder

TUNE_SCRIPT = os.path.join(os.path.dirname(__file__), "_tune_script.py")

# Classes to hold data from experiment checkpoints


class ExperimentStateCheckpoint:
    def __init__(self, runner_data: Dict[str, Any], trials: List["TrialStub"]):
        self.runner_data = runner_data
        self.trials = trials


class ExperimentDirCheckpoint:
    def __init__(self, trial_to_cps: Dict["TrialStub", "TrialCheckpointData"]):
        self.trial_to_cps = trial_to_cps


class TrialStub:
    def __init__(
            self,
            trainable_name: str,
            trial_id: str,
            status: str,
            config: Dict[str, Any],
            local_dir: str,
            experiment_tag: str,
            _last_result: Dict[str, Any],
            logdir: str,
            *args,
            **kwargs,
    ):
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
    def hostname(self):
        return self.last_result["hostname"]

    @property
    def node_ip(self):
        return self.last_result["node_ip"]

    @property
    def dirname(self):
        return os.path.basename(self.logdir)

    @property
    def was_on_driver_node(self):
        return self.hostname == platform.node()

    def __hash__(self):
        return hash(self.trial_id)

    def __repr__(self):
        return f"<TrialStub trial_id={self.trial_id}>"


@dataclass
class TrialCheckpointData:
    params: Dict[str, Any]
    results: List[Dict[str, Any]]
    progress: List[Dict[str, Any]]
    checkpoints: List[Tuple[str, Dict[Any, Any]]]


# Utility functions


def delete_file_if_exists(filename: str):
    if os.path.exists(filename):
        os.remove(filename)


def cleanup_driver_experiment_dir(experiment_name: str):
    experiment_dir = os.path.join(
        os.path.expanduser("~/ray_results"), experiment_name)
    if os.path.exists(experiment_dir):
        print("Removing existing experiment dir:", experiment_dir)
        shutil.rmtree(experiment_dir)


def cleanup_remote_node_experiment_dir(experiment_name: str):
    experiment_dir = os.path.join(
        os.path.expanduser("~/ray_results"), experiment_name)

    command = ["rm", "-rf", f"{experiment_dir}"]

    @ray.remote
    def _run_on_remote_node(cmd: str):
        return subprocess.check_call(cmd)

    futures = []
    for node in ray.nodes():
        if not node["Alive"]:
            continue

        hostname = node["NodeManagerHostname"]
        ip = node["NodeManagerAddress"]

        if hostname == platform.node():
            # Skip on driver
            continue

        rfn = _run_on_remote_node.options(resources={f"node:{ip}": 0.01})
        futures.append(rfn.remote(command))
    ray.get(futures)


# Run tune script in different process


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
        sync_to_driver: bool,
        upload_dir: Optional[str],
        durable: bool,
):
    # Start run
    process = start_run(
        sync_to_driver=sync_to_driver,
        upload_dir=upload_dir,
        durable=durable,
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


# Run full flow


def run_resume_flow(
        experiment_name: str,
        indicator_file: str,
        sync_to_driver: bool,
        upload_dir: Optional[str],
        durable: bool,
        before_experiments_callback: Optional[Callable[[], None]] = None,
        between_experiments_callback: Optional[Callable[[], None]] = None,
        after_experiments_callback: Optional[Callable[[], None]] = None):
    """Run full flow, i.e.

    - Clean up existing experiment dir
    - Call before experiment callback
    - Run tune script for 30 seconds
    - Call between experiment callback
    - Run tune script for another 30 seconds
    - Call after experiment callback
    """
    # Cleanup ~/ray_results/<experiment_name> folder
    cleanup_driver_experiment_dir(experiment_name)

    # Cleanup experiment folder on remote nodes
    cleanup_remote_node_experiment_dir(experiment_name)

    # Run before experiment callbacks
    if before_experiments_callback:
        before_experiments_callback()

    # Delete indicator file
    delete_file_if_exists(indicator_file)

    # Run tune script for 30 seconds
    run_tune_script_for_time(
        run_time=30,
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        sync_to_driver=sync_to_driver,
        upload_dir=upload_dir,
        durable=durable,
    )

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
        indicator_file=indicator_file,
        sync_to_driver=sync_to_driver,
        upload_dir=upload_dir,
        durable=durable,
    )

    if after_experiments_callback:
        after_experiments_callback()


# Download data from remote nodes

_trial_node_syncers = {}


def get_trial_node_syncer(trial: TrialStub, tmpdir: str) -> NodeSyncer:
    global _trial_node_syncers

    if trial not in _trial_node_syncers:
        optional_cloud_syncer = detect_sync_to_driver(sync_to_driver=True)
        node_syncer = get_node_syncer(
            tmpdir,
            remote_dir=trial.local_dir,
            sync_function=optional_cloud_syncer)
        _trial_node_syncers[trial] = node_syncer

    return _trial_node_syncers[trial]


def fetch_trial_node_dirs_to_tmp_dir(
        trials: List[TrialStub]) -> Dict[TrialStub, str]:
    dirmap = {}

    for trial in trials:
        tmpdir = tempfile.mkdtemp(prefix="tune_cloud_test")

        if trial.was_on_driver_node:
            # Trial was run on driver
            shutil.rmtree(tmpdir)
            shutil.copytree(trial.local_dir, tmpdir)
            print("COPIED LOCAL NODE", trial.local_dir, "TO", tmpdir)

        else:
            # Trial was run on remote node
            node_syncer = get_trial_node_syncer(trial, tmpdir)
            node_syncer.set_worker_ip(trial.node_ip)
            if not node_syncer.sync_down():
                raise RuntimeError(
                    f"Could not sync remote experiment dir for trial "
                    f"{trial} from {trial.hostname} ({trial.node_ip}) "
                    f"to {tmpdir}.")
            print("SYNCED DOWN REMOTE NODE", trial.hostname, "TO", tmpdir)

        dirmap[trial] = tmpdir

    return dirmap


# Load data from local dirs into objects


def load_experiment_checkpoint_from_state_file(
        experiment_dir: str) -> ExperimentStateCheckpoint:
    newest_ckpt_path = _find_newest_ckpt(experiment_dir)
    with open(newest_ckpt_path, "r") as f:
        runner_state = json.load(f, cls=TuneFunctionDecoder)

    trials = []
    for trial_cp_str in runner_state["checkpoints"]:
        parsed = json.loads(trial_cp_str, cls=TuneFunctionDecoder)
        trials.append(TrialStub(**parsed))

    runner_data = runner_state["runner_data"]

    return ExperimentStateCheckpoint(runner_data, trials)


def load_experiment_checkpoint_from_dir(
        trials: Iterable[TrialStub],
        experiment_dir: str) -> ExperimentDirCheckpoint:
    trial_to_cps = {}
    for f in sorted(os.listdir(experiment_dir)):
        full_path = os.path.join(experiment_dir, f)
        if os.path.isdir(full_path):
            trial_checkpoint_data = load_trial_checkpoint_data(full_path)

            # Map to TrialStub object
            trial_stub = None
            for trial in trials:
                if trial.dirname == f:
                    trial_stub = trial
                    break

            if not trial_stub:
                raise RuntimeError(f"Trial with dirname {f} not found.")

            trial_to_cps[trial_stub] = trial_checkpoint_data

    return ExperimentDirCheckpoint(trial_to_cps)


def load_trial_checkpoint_data(trial_dir: str) -> TrialCheckpointData:
    params_file = os.path.join(trial_dir, "params.json")
    if os.path.exists(params_file):
        with open(params_file, "rt") as f:
            params = json.load(f)
    else:
        params = {}

    result_file = os.path.join(trial_dir, "result.json")
    if os.path.exists(result_file):
        results = []
        with open(result_file, "rt") as f:
            for line in f.readlines():
                results.append(json.loads(line))
    else:
        results = []

    progress_file = os.path.join(trial_dir, "progress.csv")
    if os.path.exists(progress_file):
        with open(progress_file, "rt") as f:
            reader = csv.DictReader(f)
            progress = list(reader)
    else:
        progress = []

    checkpoints = []
    for cp_dir in sorted(os.listdir(trial_dir)):
        if not cp_dir.startswith("checkpoint_"):
            continue
        with open(os.path.join(trial_dir, cp_dir, "checkpoint.json"),
                  "rt") as f:
            checkpoint_data = json.load(f)
        checkpoints.append((cp_dir, checkpoint_data))

    return TrialCheckpointData(
        params=params,
        results=results,
        progress=progress,
        checkpoints=checkpoints)


def load_data_from_trial_exp_checkpoints(
        trial_to_exp_dir: Dict[TrialStub, str]
) -> Dict[TrialStub, ExperimentDirCheckpoint]:
    trial_to_checkpoint_data = {}
    for trial, dirname in trial_to_exp_dir.items():
        trial_to_checkpoint_data[trial] = load_experiment_checkpoint_from_dir(
            trial_to_exp_dir.keys(), dirname)

    return trial_to_checkpoint_data


# Load all relevant data


def get_experiment_and_trial_data(
        experiment_name: str
) -> Tuple[ExperimentStateCheckpoint, ExperimentDirCheckpoint, Dict[
        TrialStub, ExperimentDirCheckpoint]]:
    experiment_dir = assert_experiment_dir_exists(
        experiment_name=experiment_name)

    experiment_state = assert_experiment_checkpoint_validity(
        experiment_dir=experiment_dir, total_time_bounds=(120, 239))

    driver_dir_cp = load_experiment_checkpoint_from_dir(
        experiment_state.trials, experiment_dir)

    # Fetch experiment dirs from remote nodes to a local temp dir
    trial_to_exp_dir = fetch_trial_node_dirs_to_tmp_dir(
        experiment_state.trials)

    # Load data stored in these experiment dirs
    trial_exp_checkpoint_data = load_data_from_trial_exp_checkpoints(
        trial_to_exp_dir)

    return experiment_state, driver_dir_cp, trial_exp_checkpoint_data


# Assertions


def assert_experiment_dir_exists(experiment_name: str) -> str:
    experiment_dir = os.path.join(
        os.path.expanduser("~/ray_results"), experiment_name)

    if not os.path.exists(experiment_dir):
        raise RuntimeError(
            f"Check failed: Experiment dir {experiment_dir} does not exist.")

    return experiment_dir


def assert_experiment_checkpoint_validity(
        experiment_dir: str,
        total_time_bounds: Optional[Tuple[float, float]] = None
) -> ExperimentStateCheckpoint:
    experiment_state = load_experiment_checkpoint_from_state_file(
        experiment_dir)

    assert len(
        experiment_state.trials) == 4, "Not all trials have been created."

    assert all(
        trial.status == "RUNNING"
        for trial in experiment_state.trials), "Not all trials are RUNNING"

    runner_data = experiment_state.runner_data
    if total_time_bounds:
        assert (total_time_bounds[0]
                <= runner_data["_total_time"]
                <= total_time_bounds[1]), \
            f"Total time {runner_data['_total_time']} not within bounds " \
            f"({total_time_bounds[0]} <= {runner_data['_total_time']} <= " \
            f"{total_time_bounds[1]})"

    return experiment_state


def assert_min_num_trials(trials: Iterable[TrialStub], on_driver: int,
                          on_worker: int) -> Tuple[int, int]:
    num_trials_on_driver = len(
        [trial for trial in trials if trial.was_on_driver_node])

    num_trials_not_on_driver = len(trials) - num_trials_on_driver

    assert num_trials_on_driver >= on_driver, \
        f"Not enough trials were scheduled on the driver node " \
        f"({num_trials_on_driver} < {on_driver})."

    assert num_trials_not_on_driver >= on_worker, \
        f"Not enough trials were scheduled on remote nodes." \
        f"({num_trials_on_driver} < {on_worker})."

    return num_trials_on_driver, len(trials) - num_trials_on_driver


def assert_checkpoint_count(experiment_dir_cp: ExperimentDirCheckpoint,
                            for_driver_trial: int, for_worker_trial: int):
    for trial, trial_cp in experiment_dir_cp.trial_to_cps.items():
        cps = len(trial_cp.checkpoints)
        if trial.was_on_driver_node:
            assert cps == for_driver_trial, (
                f"Trial {trial.trial_id} was on driver, "
                f"but did not observe the expected amount of checkpoints "
                f"({cps} != {for_driver_trial}).")
        else:
            assert cps == for_worker_trial, (
                f"Trial {trial.trial_id} was not on the driver, "
                f"but did not observe the expected amount of checkpoints "
                f"({cps} != {for_worker_trial}).")


def test_no_sync():
    """
    No syncing, so:

        sync_to_driver=False
        upload_dir=None
        no durable_trainable

    Expected results after first checkpoint:

        - 4 trials are running
        - At least one trial ran on the head node
        - At least one trial ran remotely
        - Driver has trial checkpoints from head node trial
        - Driver has no trial checkpoints from remote node trials
        - Remote trial dirs only have data for one trial
        - Remote trial dirs have checkpoints for node-local trials

    """
    experiment_name = "cloud_no_sync"
    indicator_file = "/tmp/cloud_no_sync_indicator"

    def between_experiments():
        # Includes Req: 4 trials are running
        (experiment_state, driver_dir_cp, trial_exp_checkpoint_data
         ) = get_experiment_and_trial_data(experiment_name=experiment_name)

        # Req: At least one trial ran on driver
        # Req: At least one trial ran remotely
        assert_min_num_trials(
            driver_dir_cp.trial_to_cps.keys(), on_driver=1, on_worker=1)

        # Req: Driver has trial checkpoints from head node trial
        # Req: Driver has no trial checkpoints from remote node trials
        assert_checkpoint_count(
            driver_dir_cp, for_driver_trial=2, for_worker_trial=0)

        for trial, exp_dir_cp in trial_exp_checkpoint_data.items():
            # Req: Remote trial dirs only have data for one trial

            seen = len(exp_dir_cp.trial_to_cps)

            if trial.was_on_driver_node:
                assert seen == 4, (f"Trial {trial.trial_id} was on driver, "
                                   f"but observed too few trials ({seen}) "
                                   f"in experiment dir.")
            else:
                assert seen == 1, (
                    f"Trial {trial.trial_id} was not on driver, "
                    f"but observed not exactly 1 trials ({seen}) "
                    f"in experiment dir.")

                assert_checkpoint_count(
                    exp_dir_cp, for_driver_trial=0, for_worker_trial=2)

    run_resume_flow(
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        sync_to_driver=False,
        upload_dir=None,
        durable=False,
        between_experiments_callback=between_experiments)


if __name__ == "__main__":
    ray.init()
    test_no_sync()
