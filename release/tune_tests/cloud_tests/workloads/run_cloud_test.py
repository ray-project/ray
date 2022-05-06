"""Run cloud checkpointing tests.

This script provides utilities and end to end tests for cloud checkpointing.

We are considering several scenarios depending on the combination of the
following Tune properties:

syncer ("auto" or None)
upload_dir

Generally the flow is as follows:

A Tune run is started in a separate process. It is terminated after some
time. It is then restarted for another period of time.

Depending on the combination of the run properties above, we expect different
results between the two runs and after the second run.

For instance, we sometimes expect all checkpoints to be synced to the driver
(syncer="auto" and no upload dir), and sometimes not (syncer=None).

We also ensure that checkpoints are properly deleted.

The Tune run is kicked off in _tune_script.py. Trials write a checkpoint
every 2 iterations, and take 5 seconds per iteration.

More details on the expected results can be found in the scenario descriptions.
"""

import argparse
import csv
import io
import tarfile
from dataclasses import dataclass
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import tempfile
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import ray
import ray.cloudpickle as pickle
from ray.tune.trial_runner import find_newest_experiment_checkpoint
from ray.tune.utils.serialization import TuneFunctionDecoder

TUNE_SCRIPT = os.path.join(os.path.dirname(__file__), "_tune_script.py")

# Classes to hold data from experiment checkpoints


class ExperimentStateCheckpoint:
    def __init__(
        self, dir: str, runner_data: Dict[str, Any], trials: List["TrialStub"]
    ):
        self.dir = dir
        self.runner_data = runner_data
        self.trials = trials


class ExperimentDirCheckpoint:
    def __init__(
        self, dir: str, trial_to_cps: Dict["TrialStub", "TrialCheckpointData"]
    ):
        self.dir = dir
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
    num_skipped: int


# Utility functions


def delete_file_if_exists(filename: str):
    if os.path.exists(filename):
        os.remove(filename)


def cleanup_driver_experiment_dir(experiment_name: str):
    experiment_dir = os.path.join(os.path.expanduser("~/ray_results"), experiment_name)
    if os.path.exists(experiment_dir):
        print("Removing existing experiment dir:", experiment_dir)
        shutil.rmtree(experiment_dir)


def cleanup_remote_node_experiment_dir(experiment_name: str):
    experiment_dir = os.path.join(os.path.expanduser("~/ray_results"), experiment_name)

    @ray.remote
    def _remove_on_remove_node(path: str):
        return shutil.rmtree(path, ignore_errors=True)

    futures = []
    for node in ray.nodes():
        if not node["Alive"]:
            continue

        hostname = node["NodeManagerHostname"]
        ip = node["NodeManagerAddress"]

        if hostname == platform.node():
            # Skip on driver
            continue

        rfn = _remove_on_remove_node.options(resources={f"node:{ip}": 0.01})
        futures.append(rfn.remote(experiment_dir))
    ray.get(futures)


# Cluster utility
def wait_for_nodes(
    num_nodes: int, timeout: float = 300.0, feedback_interval: float = 10.0
):
    start = time.time()

    max_time = start + timeout
    next_feedback = start + feedback_interval

    curr_nodes = len(ray.nodes())
    while curr_nodes < num_nodes:
        now = time.time()

        if now >= max_time:
            raise RuntimeError(
                f"Maximum wait time reached, but only "
                f"{curr_nodes}/{num_nodes} nodes came up. Aborting."
            )

        if now >= next_feedback:
            passed = now - start
            print(
                f"Waiting for more nodes to come up: "
                f"{curr_nodes}/{num_nodes} "
                f"({passed:.0f} seconds passed)"
            )
            next_feedback = now + feedback_interval

        time.sleep(5)
        curr_nodes = len(ray.nodes())


# Run tune script in different process


def start_run(
    no_syncer: bool,
    upload_dir: Optional[str] = None,
    experiment_name: str = "cloud_test",
    indicator_file: str = "/tmp/tune_cloud_indicator",
) -> subprocess.Popen:
    args = []
    if no_syncer:
        args.append("--no-syncer")

    if upload_dir:
        args.extend(["--upload-dir", upload_dir])

    if experiment_name:
        args.extend(["--experiment-name", experiment_name])

    if indicator_file:
        args.extend(["--indicator-file", indicator_file])

    env = os.environ.copy()
    env["TUNE_RESULT_BUFFER_LENGTH"] = "1"
    env["TUNE_GLOBAL_CHECKPOINT_S"] = "10"

    tune_script = os.environ.get("OVERWRITE_TUNE_SCRIPT", TUNE_SCRIPT)

    full_command = ["python", tune_script] + args

    print(f"Running command: {' '.join(full_command)}")
    process = subprocess.Popen(full_command, env=env)

    return process


def wait_for_run_or_raise(
    process: subprocess.Popen, indicator_file: str, timeout: int = 30
):
    print(
        f"Waiting up to {timeout} seconds until trials have been started "
        f"(indicated by existence of `{indicator_file}`)"
    )

    timeout = time.monotonic() + timeout
    while (
        process.poll() is None
        and time.monotonic() < timeout
        and not os.path.exists(indicator_file)
    ):
        time.sleep(1)

    if not os.path.exists(indicator_file):
        process.terminate()

        raise RuntimeError(
            f"Indicator file `{indicator_file}` still doesn't exist, "
            f"indicating that trials have not been started. "
            f"Please check the process output."
        )

    print("Process started, trials are running")


def send_signal_after_wait(process: subprocess.Popen, signal: int, wait: int = 30):
    print(
        f"Waiting {wait} seconds until sending signal {signal} "
        f"to process {process.pid}"
    )

    time.sleep(wait)

    if process.poll() is not None:
        raise RuntimeError(
            f"Process {process.pid} already terminated. This usually means "
            f"that some of the trials ERRORed (e.g. because they couldn't be "
            f"restored. Try re-running this test to see if this fixes the "
            f"issue."
        )

    print(f"Sending signal {signal} to process {process.pid}")
    process.send_signal(signal)


def wait_until_process_terminated(process: subprocess.Popen, timeout: int = 60):
    print(f"Waiting up to {timeout} seconds until process " f"{process.pid} terminates")

    timeout = time.monotonic() + timeout
    while process.poll() is None and time.monotonic() < timeout:
        time.sleep(1)

    if process.poll() is None:
        process.terminate()

        print(
            f"Warning: Process {process.pid} did not terminate within "
            f"timeout, terminating forcefully instead."
        )
    else:
        print(f"Process {process.pid} terminated gracefully.")


def run_tune_script_for_time(
    run_time: int,
    experiment_name: str,
    indicator_file: str,
    no_syncer: bool,
    upload_dir: Optional[str],
):
    # Start run
    process = start_run(
        no_syncer=no_syncer,
        upload_dir=upload_dir,
        experiment_name=experiment_name,
        indicator_file=indicator_file,
    )
    try:
        # Wait until indicator file exists
        wait_for_run_or_raise(process, indicator_file=indicator_file, timeout=30)
        # Stop experiment (with checkpoint) after some time
        send_signal_after_wait(process, signal=signal.SIGUSR1, wait=run_time)
        # Wait until process gracefully terminated
        wait_until_process_terminated(process, timeout=45)
    finally:
        process.terminate()


# Run full flow


def run_resume_flow(
    experiment_name: str,
    indicator_file: str,
    no_syncer: bool,
    upload_dir: Optional[str],
    first_run_time: int = 33,
    second_run_time: int = 33,
    before_experiments_callback: Optional[Callable[[], None]] = None,
    between_experiments_callback: Optional[Callable[[], None]] = None,
    after_experiments_callback: Optional[Callable[[], None]] = None,
):
    """Run full flow, i.e.

    - Clean up existing experiment dir
    - Call before experiment callback
    - Run tune script for `first_run_time` seconds
    - Call between experiment callback
    - Run tune script for another `second_run_time` seconds
    - Call after experiment callback
    """
    # Cleanup ~/ray_results/<experiment_name> folder
    cleanup_driver_experiment_dir(experiment_name)

    # Cleanup experiment folder on remote nodes
    cleanup_remote_node_experiment_dir(experiment_name)

    # Run before experiment callbacks
    if before_experiments_callback:
        print("Before experiments: Invoking callback")
        before_experiments_callback()
        print("Before experiments: Callback completed")

    # Delete indicator file
    delete_file_if_exists(indicator_file)

    # Run tune script for `first_run_time` seconds
    run_tune_script_for_time(
        run_time=first_run_time,
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        no_syncer=no_syncer,
        upload_dir=upload_dir,
    )

    # Before we restart, run a couple of checks
    # Run before experiment callbacks
    if between_experiments_callback:
        print("Between experiments: Invoking callback")
        between_experiments_callback()
        print("Between experiments: Callback completed")

    # Restart. First, clean up indicator file
    delete_file_if_exists(indicator_file)

    # Start run again, run for another `second_run_time` seconds
    run_tune_script_for_time(
        run_time=second_run_time,
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        no_syncer=no_syncer,
        upload_dir=upload_dir,
    )

    if after_experiments_callback:
        print("After experiments: Invoking callback")
        after_experiments_callback()
        print("After experiments: Callback completed")


# Download data from remote nodes


def fetch_remote_directory_content(
    node_ip: str,
    remote_dir: str,
    local_dir: str,
):
    def _pack(dir: str):
        stream = io.BytesIO()
        with tarfile.open(
            fileobj=stream, mode="w:gz", format=tarfile.PAX_FORMAT
        ) as tar:
            tar.add(dir, arcname="")

        return stream.getvalue()

    def _unpack(stream: str, dir: str):
        with tarfile.open(fileobj=io.BytesIO(stream)) as tar:
            tar.extractall(dir)

    try:
        packed = ray.get(
            ray.remote(resources={f"node:{node_ip}": 0.01})(_pack).remote(remote_dir)
        )
        _unpack(packed, local_dir)
    except Exception as e:
        print(
            f"Warning: Could not fetch remote directory contents. Message: " f"{str(e)}"
        )


def send_local_file_to_remote_file(local_path: str, remote_path: str, ip: str):
    def _write(stream: bytes, path: str):
        with open(path, "wb") as f:
            f.write(stream)

    with open(local_path, "rb") as f:
        stream = f.read()

    _remote_write = ray.remote(resources={f"node:{ip}": 0.01})(_write)
    return ray.get(_remote_write.remote(stream, remote_path))


def fetch_remote_file_to_local_file(remote_path: str, ip: str, local_path: str):
    def _read(path: str):
        with open(path, "rb") as f:
            return f.read()

    _remote_read = ray.remote(resources={f"node:{ip}": 0.01})(_read)
    stream = ray.get(_remote_read.remote(remote_path))

    with open(local_path, "wb") as f:
        f.write(stream)


def fetch_trial_node_dirs_to_tmp_dir(trials: List[TrialStub]) -> Dict[TrialStub, str]:
    dirmap = {}

    for trial in trials:
        tmpdir = tempfile.mkdtemp(prefix="tune_cloud_test")

        if trial.was_on_driver_node:
            # Trial was run on driver
            shutil.rmtree(tmpdir)
            shutil.copytree(trial.local_dir, tmpdir)
            print(
                "Copied local node experiment dir",
                trial.local_dir,
                "to",
                tmpdir,
                "for trial",
                trial.trial_id,
            )

        else:
            # Trial was run on remote node
            fetch_remote_directory_content(
                trial.node_ip, remote_dir=trial.local_dir, local_dir=tmpdir
            )

        dirmap[trial] = tmpdir

    return dirmap


# Bucket interaction


def clear_bucket_contents(bucket: str):
    if bucket.startswith("s3://"):
        print("Clearing bucket contents:", bucket)
        subprocess.check_call(["aws", "s3", "rm", "--recursive", "--quiet", bucket])
    elif bucket.startswith("gs://"):
        print("Clearing bucket contents:", bucket)
        try:
            subprocess.check_call(["gsutil", "-m", "rm", "-f", "-r", bucket])
        except subprocess.CalledProcessError:
            # If empty, ignore error
            pass
    else:
        raise ValueError(f"Invalid bucket URL: {bucket}")


def fetch_bucket_contents_to_tmp_dir(bucket: str) -> str:
    tmpdir = tempfile.mkdtemp(prefix="tune_cloud_test")
    subfolder = None

    if bucket.startswith("s3://"):
        subprocess.check_call(
            ["aws", "s3", "cp", "--recursive", "--quiet", bucket, tmpdir]
        )
    elif bucket.startswith("gs://"):
        try:
            subprocess.check_call(["gsutil", "-m", "cp", "-r", bucket, tmpdir])
        except subprocess.CalledProcessError as e:
            # Sometimes single files cannot be processed
            if len(os.listdir(tmpdir)) == 0:
                raise RuntimeError(
                    f"Local dir {tmpdir} empty after trying to fetch bucket data."
                ) from e
        pattern = re.compile("gs://[^/]+/(.+)")
        subfolder = re.match(pattern, bucket).group(1).split("/")[-1]
    else:
        raise ValueError(f"Invalid bucket URL: {bucket}")

    if subfolder:
        tmpdir = os.path.join(tmpdir, subfolder)

    print("Copied bucket data from", bucket, "to", tmpdir)

    return tmpdir


# Load data from local dirs into objects


def load_experiment_checkpoint_from_state_file(
    experiment_dir: str,
) -> ExperimentStateCheckpoint:
    newest_ckpt_path = find_newest_experiment_checkpoint(experiment_dir)
    with open(newest_ckpt_path, "r") as f:
        runner_state = json.load(f, cls=TuneFunctionDecoder)

    trials = []
    for trial_cp_str in runner_state["checkpoints"]:
        parsed = json.loads(trial_cp_str, cls=TuneFunctionDecoder)
        trial = TrialStub(**parsed)
        trials.append(trial)

    runner_data = runner_state["runner_data"]

    return ExperimentStateCheckpoint(experiment_dir, runner_data, trials)


def load_experiment_checkpoint_from_dir(
    trials: Iterable[TrialStub], experiment_dir: str
) -> ExperimentDirCheckpoint:
    trial_to_cps = {}
    for f in sorted(os.listdir(experiment_dir)):
        full_path = os.path.join(experiment_dir, f)
        if os.path.isdir(full_path):
            # Map to TrialStub object
            trial_stub = None
            for trial in trials:
                if trial.dirname == f:
                    trial_stub = trial
                    break

            if not trial_stub:
                raise RuntimeError(f"Trial with dirname {f} not found.")

            trial_checkpoint_data = load_trial_checkpoint_data(
                full_path, node_trial=trial_stub
            )

            trial_to_cps[trial_stub] = trial_checkpoint_data

    return ExperimentDirCheckpoint(experiment_dir, trial_to_cps)


def load_trial_checkpoint_data(
    trial_dir: str, node_trial: TrialStub
) -> TrialCheckpointData:
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
    num_skipped = 0
    for cp_dir in sorted(os.listdir(trial_dir)):
        if not cp_dir.startswith("checkpoint_"):
            continue

        cp_full_dir = os.path.join(trial_dir, cp_dir)

        try:
            checkpoint_num = int(cp_dir.lstrip("checkpoint_"))
            if checkpoint_num > node_trial.last_result["internal_iter"]:
                # Checkpoint has not been observed by tune, yet, so we can't
                # account for it. This is a race condition where training
                # already created a checkpoint, but the result was not yet
                # processed by Ray Tune. So, we just pretend it isn't there
                # for the sake of the test.
                print(
                    f"Skipping unobserved checkpoint: {cp_full_dir} as "
                    f"{checkpoint_num} > "
                    f"{node_trial.last_result['internal_iter']}"
                )
                num_skipped += 1
                continue
        except ValueError:
            # temporary checkpoint
            continue

        json_path = os.path.join(cp_full_dir, "checkpoint.json")
        if os.path.exists(json_path):
            with open(json_path, "rt") as f:
                checkpoint_data = json.load(f)
        else:
            meta_path = os.path.join(
                cp_full_dir, f"checkpoint-{checkpoint_num}.tune_metadata"
            )
            with open(meta_path, "rb") as f:
                checkpoint_meta = pickle.load(f)
                checkpoint_data = {"internal_iter": checkpoint_meta["iteration"]}
        checkpoints.append((cp_dir, checkpoint_data))

    return TrialCheckpointData(
        params=params,
        results=results,
        progress=progress,
        checkpoints=checkpoints,
        num_skipped=num_skipped,
    )


def load_data_from_trial_exp_checkpoints(
    trial_to_exp_dir: Dict[TrialStub, str]
) -> Dict[TrialStub, ExperimentDirCheckpoint]:
    trial_to_checkpoint_data = {}
    for trial, dirname in trial_to_exp_dir.items():
        trial_to_checkpoint_data[trial] = load_experiment_checkpoint_from_dir(
            trial_to_exp_dir.keys(), dirname
        )

    return trial_to_checkpoint_data


# Load all relevant data


def get_experiment_and_trial_data(
    experiment_name: str,
) -> Tuple[
    ExperimentStateCheckpoint,
    ExperimentDirCheckpoint,
    Dict[TrialStub, ExperimentDirCheckpoint],
]:
    experiment_dir = assert_experiment_dir_exists(experiment_name=experiment_name)

    experiment_state = load_experiment_checkpoint_from_state_file(
        experiment_dir=experiment_dir
    )

    assert_experiment_checkpoint_validity(experiment_state)

    driver_dir_cp = load_experiment_checkpoint_from_dir(
        experiment_state.trials, experiment_dir
    )

    # Fetch experiment dirs from remote nodes to a local temp dir
    trial_to_exp_dir = fetch_trial_node_dirs_to_tmp_dir(experiment_state.trials)

    # Load data stored in these experiment dirs
    trial_exp_checkpoint_data = load_data_from_trial_exp_checkpoints(trial_to_exp_dir)

    return experiment_state, driver_dir_cp, trial_exp_checkpoint_data


def get_bucket_data(
    bucket: str,
    experiment_name: str,
) -> Tuple[ExperimentStateCheckpoint, ExperimentDirCheckpoint]:
    local_bucket_dir = fetch_bucket_contents_to_tmp_dir(bucket)
    local_experiment_dir = os.path.join(local_bucket_dir, experiment_name)

    bucket_state_cp = load_experiment_checkpoint_from_state_file(local_experiment_dir)

    bucket_dir_cp = load_experiment_checkpoint_from_dir(
        bucket_state_cp.trials, local_experiment_dir
    )

    return bucket_state_cp, bucket_dir_cp


# Assertions


def assert_experiment_dir_exists(experiment_name: str) -> str:
    experiment_dir = os.path.join(os.path.expanduser("~/ray_results"), experiment_name)

    if not os.path.exists(experiment_dir):
        raise RuntimeError(
            f"Check failed: Experiment dir {experiment_dir} does not exist."
        )

    return experiment_dir


def assert_experiment_checkpoint_validity(experiment_state: ExperimentStateCheckpoint):
    assert len(experiment_state.trials) == 4, "Not all trials have been created."


def assert_min_num_trials(
    trials: Iterable[TrialStub], on_driver: int, on_worker: int
) -> Tuple[int, int]:
    num_trials_on_driver = len([trial for trial in trials if trial.was_on_driver_node])

    num_trials_not_on_driver = len(trials) - num_trials_on_driver

    assert num_trials_on_driver >= on_driver, (
        f"Not enough trials were scheduled on the driver node "
        f"({num_trials_on_driver} < {on_driver})."
    )

    assert num_trials_not_on_driver >= on_worker, (
        f"Not enough trials were scheduled on remote nodes."
        f"({num_trials_on_driver} < {on_worker})."
    )

    return num_trials_on_driver, len(trials) - num_trials_on_driver


def assert_checkpoint_count(
    experiment_dir_cp: ExperimentDirCheckpoint,
    for_driver_trial: int,
    for_worker_trial: int,
    max_additional: int = 0,
):
    for trial, trial_cp in experiment_dir_cp.trial_to_cps.items():
        cps = len(trial_cp.checkpoints)
        num_skipped = trial_cp.num_skipped
        if trial.was_on_driver_node:
            assert (
                cps >= for_driver_trial and cps <= for_driver_trial + max_additional
            ), (
                f"Trial {trial.trial_id} was on driver, "
                f"but did not observe the expected amount of checkpoints "
                f"({cps} != {for_driver_trial}, "
                f"skipped={num_skipped}, max_additional={max_additional}). "
                f"Directory: {experiment_dir_cp.dir}"
            )
        else:
            assert (
                cps >= for_worker_trial and cps <= for_worker_trial + max_additional
            ), (
                f"Trial {trial.trial_id} was not on the driver, "
                f"but did not observe the expected amount of checkpoints "
                f"({cps} != {for_worker_trial}, "
                f"skipped={num_skipped}, max_additional={max_additional}). "
                f"Directory: {experiment_dir_cp.dir}"
            )


def assert_trial_progressed_training(trial: TrialStub):
    assert (
        trial.last_result["training_iteration"]
        > trial.last_result["iterations_since_restore"]
    ), (
        f"Trial {trial.trial_id} had a checkpoint but did not continue "
        f"on resume (training iteration: "
        f"{trial.last_result['training_iteration']} <="
        f"{trial.last_result['iterations_since_restore']}). "
        f"This probably means the checkpoint has not been synced "
        f"to the node correctly."
    )


def test_no_sync_down():
    """
    No down syncing, so:

        syncer=None
        upload_dir=None

    Expected results after first checkpoint:

        - 4 trials are running
        - At least one trial ran on the head node
        - At least one trial ran remotely
        - Driver has trial checkpoints from head node trial
        - Driver has no trial checkpoints from remote node trials
        - Remote trial dirs only have data for one trial
        - Remote trial dirs have checkpoints for node-local trials

    Then, remote checkpoint directories are cleaned up. This means only
    one trial can continue training (the one trained on the head node)
    - even if it is subsequently scheduled on a different node (as
    sync_on_checkpoint is still True).

    Expected results after second checkpoint:

        - 1 trial is running, 3 errored
        - The running trial progressed with training

    """
    experiment_name = "cloud_no_sync_down"
    indicator_file = f"/tmp/{experiment_name}_indicator"

    def between_experiments():
        (
            experiment_state,
            driver_dir_cp,
            trial_exp_checkpoint_data,
        ) = get_experiment_and_trial_data(experiment_name=experiment_name)

        # Req: 4 trials are running
        assert all(
            trial.status == "RUNNING" for trial in experiment_state.trials
        ), "Not all trials are RUNNING"

        # Req: At least one trial ran on driver
        # Req: At least one trial ran remotely
        assert_min_num_trials(
            driver_dir_cp.trial_to_cps.keys(), on_driver=1, on_worker=1
        )

        # Req: Driver has trial checkpoints from head node trial
        # Req: Driver has no trial checkpoints from remote node trials
        assert_checkpoint_count(
            driver_dir_cp, for_driver_trial=2, for_worker_trial=0, max_additional=1
        )

        for trial, exp_dir_cp in trial_exp_checkpoint_data.items():
            # Req: Remote trial dirs only have data for one trial

            seen = len(exp_dir_cp.trial_to_cps)

            if trial.was_on_driver_node:
                assert seen == 4, (
                    f"Trial {trial.trial_id} was on driver, "
                    f"but observed too few trials ({seen}) "
                    f"in experiment dir."
                )
            else:
                assert seen == 1, (
                    f"Trial {trial.trial_id} was not on driver, "
                    f"but observed not exactly 1 trials ({seen}) "
                    f"in experiment dir."
                )

                assert_checkpoint_count(
                    exp_dir_cp, for_driver_trial=0, for_worker_trial=2, max_additional=1
                )

        # Delete remote checkpoints before resume
        print("Deleting remote checkpoints before resume")
        cleanup_remote_node_experiment_dir(experiment_name)

    def after_experiments():
        (
            experiment_state,
            driver_dir_cp,
            trial_exp_checkpoint_data,
        ) = get_experiment_and_trial_data(experiment_name=experiment_name)

        num_errored = 0
        for trial in experiment_state.trials:
            if trial.status == "ERROR":
                num_errored += 1
            else:
                # Req: The running trial progressed with training
                assert_trial_progressed_training(trial)

        # Req: 1 trial is running, 3 errored
        assert num_errored == 3, (
            f"Only one trial should have had a valid checkpoint, but "
            f"{num_errored} trials errored (expected 3). If more trials have "
            f"errored, there is something wrong with restoration. If less, "
            f"maybe cleanup has not worked, or syncing to driver took place."
        )

    run_time = int(os.getenv("TUNE_RUN_TIME", "180")) or 180

    run_resume_flow(
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        no_syncer=True,
        upload_dir=None,
        first_run_time=run_time,
        second_run_time=run_time,
        between_experiments_callback=between_experiments,
        after_experiments_callback=after_experiments,
    )


def test_ssh_sync():
    """
    SSH syncing, so:

        syncer="auto"
        upload_dir=None

    Expected results after first checkpoint:

        - 4 trials are running
        - At least one trial ran on the head node
        - At least one trial ran remotely
        - Driver has trial checkpoints from head node trial
        - Driver has trial checkpoints from remote node trials
        - Remote trial dirs only have data for one trial
        - Remote trial dirs have checkpoints for node-local trials

    Then, remote checkpoint directories are cleaned up.

    Expected results after second checkpoint:

        - 4 trials are running
        - All trials progressed with training

    """
    experiment_name = "cloud_ssh_sync"
    indicator_file = f"/tmp/{experiment_name}_indicator"

    def between_experiments():
        (
            experiment_state,
            driver_dir_cp,
            trial_exp_checkpoint_data,
        ) = get_experiment_and_trial_data(experiment_name=experiment_name)

        # Req: 4 trials are running
        assert all(
            trial.status == "RUNNING" for trial in experiment_state.trials
        ), "Not all trials are RUNNING"

        # Req: At least one trial ran on driver
        # Req: At least one trial ran remotely
        assert_min_num_trials(
            driver_dir_cp.trial_to_cps.keys(), on_driver=1, on_worker=1
        )

        # Req: Driver has trial checkpoints from head node trial
        # Req: Driver has trial checkpoints from remote node trials
        assert_checkpoint_count(
            driver_dir_cp, for_driver_trial=2, for_worker_trial=2, max_additional=1
        )

        for trial, exp_dir_cp in trial_exp_checkpoint_data.items():
            # Req: Remote trial dirs only have data for one trial

            seen = len(exp_dir_cp.trial_to_cps)

            if trial.was_on_driver_node:
                assert seen == 4, (
                    f"Trial {trial.trial_id} was on driver, "
                    f"but observed too few trials ({seen}) "
                    f"in experiment dir."
                )
            else:
                assert seen == 1, (
                    f"Trial {trial.trial_id} was not on driver, "
                    f"but observed not exactly 1 trials ({seen}) "
                    f"in experiment dir."
                )

                assert_checkpoint_count(
                    exp_dir_cp, for_driver_trial=0, for_worker_trial=2, max_additional=1
                )

        # Delete remote checkpoints before resume
        print("Deleting remote checkpoints before resume")
        cleanup_remote_node_experiment_dir(experiment_name)

    def after_experiments():
        (
            experiment_state,
            driver_dir_cp,
            trial_exp_checkpoint_data,
        ) = get_experiment_and_trial_data(experiment_name=experiment_name)

        # Req: 4 trials are running
        assert all(
            trial.status == "RUNNING" for trial in experiment_state.trials
        ), "Not all trials are RUNNING"

        for trial in experiment_state.trials:
            assert_trial_progressed_training(trial)

    run_time = int(os.getenv("TUNE_RUN_TIME", "180")) or 180

    run_resume_flow(
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        no_syncer=False,
        upload_dir=None,
        first_run_time=run_time + 10,  # More time because of SSH syncing
        second_run_time=run_time + 10,
        between_experiments_callback=between_experiments,
        after_experiments_callback=after_experiments,
    )


def test_durable_upload(bucket: str):
    """
    Sync trial and experiment checkpoints to cloud, so:

        syncer="auto"
        upload_dir="s3://"

    Expected results after first checkpoint:

        - 4 trials are running
        - At least one trial ran on the head node
        - At least one trial ran remotely
        - Driver has trial checkpoints from head node trial
        - Driver has no trial checkpoints from remote node trials
        - Remote trial dirs only have data for one trial
        - Remote trial dirs have checkpoints for node-local trials
        - Cloud checkpoint is valid
        - Cloud checkpoint has checkpoints from all trials

    Then, remote checkpoint directories are cleaned up.

    Expected results after second checkpoint:

        - 4 trials are running
        - All trials progressed with training
        - Cloud checkpoint is valid
        - Cloud checkpoint has checkpoints from all trials

    """
    if not bucket:
        raise ValueError(
            "The `durable_upload` test requires a `--bucket` argument to be set."
        )

    experiment_name = "cloud_durable_upload"
    indicator_file = f"/tmp/{experiment_name}_indicator"

    def before_experiments():
        clear_bucket_contents(bucket)

    def between_experiments():
        (
            experiment_state,
            driver_dir_cp,
            trial_exp_checkpoint_data,
        ) = get_experiment_and_trial_data(experiment_name=experiment_name)

        # Req: 4 trials are running
        assert all(
            trial.status == "RUNNING" for trial in experiment_state.trials
        ), "Not all trials are RUNNING"

        # Req: At least one trial ran on driver
        # Req: At least one trial ran remotely
        assert_min_num_trials(
            driver_dir_cp.trial_to_cps.keys(), on_driver=1, on_worker=1
        )

        # Req: Driver has trial checkpoints from head node trial
        # Req: Driver has no trial checkpoints from remote node trials
        assert_checkpoint_count(
            driver_dir_cp, for_driver_trial=2, for_worker_trial=0, max_additional=1
        )

        for trial, exp_dir_cp in trial_exp_checkpoint_data.items():
            # Req: Remote trial dirs only have data for one trial

            seen = len(exp_dir_cp.trial_to_cps)

            if trial.was_on_driver_node:
                assert seen == 4, (
                    f"Trial {trial.trial_id} was on driver, "
                    f"but observed too few trials ({seen}) "
                    f"in experiment dir."
                )
            else:
                assert seen == 1, (
                    f"Trial {trial.trial_id} was not on driver, "
                    f"but observed not exactly 1 trials ({seen}) "
                    f"in experiment dir."
                )

                assert_checkpoint_count(
                    exp_dir_cp, for_driver_trial=0, for_worker_trial=2, max_additional=1
                )

        bucket_state_cp, bucket_dir_cp = get_bucket_data(bucket, experiment_name)

        # Req: Cloud checkpoint is valid
        assert_experiment_checkpoint_validity(bucket_state_cp)

        # Req: Cloud checkpoint has checkpoints from all trials
        assert_checkpoint_count(
            bucket_dir_cp, for_driver_trial=2, for_worker_trial=2, max_additional=2
        )

        # Delete remote checkpoints before resume
        print("Deleting remote checkpoints before resume")
        cleanup_remote_node_experiment_dir(experiment_name)

    def after_experiments():
        (
            experiment_state,
            driver_dir_cp,
            trial_exp_checkpoint_data,
        ) = get_experiment_and_trial_data(experiment_name=experiment_name)

        # Req: 4 trials are running
        assert all(
            trial.status == "RUNNING" for trial in experiment_state.trials
        ), "Not all trials are RUNNING"

        for trial in experiment_state.trials:
            assert_trial_progressed_training(trial)

        bucket_state_cp, bucket_dir_cp = get_bucket_data(bucket, experiment_name)

        # Req: Cloud checkpoint is valid
        assert_experiment_checkpoint_validity(bucket_state_cp)

        # Req: Cloud checkpoint has checkpoints from all trials
        assert_checkpoint_count(
            bucket_dir_cp, for_driver_trial=2, for_worker_trial=2, max_additional=2
        )

        # clear_bucket_contents(bucket)

    run_time = int(os.getenv("TUNE_RUN_TIME", "180")) or 180

    run_resume_flow(
        experiment_name=experiment_name,
        indicator_file=indicator_file,
        no_syncer=False,
        upload_dir=bucket,
        first_run_time=run_time,
        second_run_time=run_time,
        before_experiments_callback=before_experiments,
        between_experiments_callback=between_experiments,
        after_experiments_callback=after_experiments,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "variant", choices=["no_sync_down", "ssh_sync", "durable_upload"]
    )
    parser.add_argument("--trainable", type=str, default="function")
    parser.add_argument("--bucket", type=str, default=None)
    parser.add_argument("--cpus-per-trial", required=False, default=2, type=int)

    args = parser.parse_args()

    # Check if test should be run using Ray client
    addr = os.environ.get("RAY_ADDRESS", "")
    job_name = os.environ.get("RAY_JOB_NAME", "client_cloud_test")
    if addr.startswith("anyscale://"):
        uses_ray_client = True
        ray.init(
            address=addr,
            job_name=job_name,
            runtime_env={"working_dir": os.path.abspath(os.path.dirname(__file__))},
        )
    else:
        uses_ray_client = False
        ray.init(address="auto")

    print(f"Running cloud test variant: {args.variant}")

    release_test_out = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")

    def _run_test(
        variant: str,
        trainable: str = "function",
        run_time: int = 180,
        bucket: str = "",
        cpus_per_trial: int = 2,
        overwrite_tune_script: Optional[str] = None,
    ) -> Dict:
        start_time = time.monotonic()
        print(
            f"Running test variant `{variant}` on "
            f"node {ray.util.get_node_ip_address()} with "
            f"{cpus_per_trial} CPUs per trial."
        )

        os.environ["TUNE_TRAINABLE"] = str(trainable)
        os.environ["TUNE_RUN_TIME"] = str(run_time)
        os.environ["TUNE_NUM_CPUS_PER_TRIAL"] = str(cpus_per_trial)

        if overwrite_tune_script:
            os.environ["OVERWRITE_TUNE_SCRIPT"] = overwrite_tune_script
            print(
                f"The test script has been overwritten with " f"{overwrite_tune_script}"
            )

        if variant == "no_sync_down":
            test_no_sync_down()
        elif variant == "ssh_sync":
            test_ssh_sync()
        elif variant == "durable_upload":
            test_durable_upload(bucket)

        time_taken = time.monotonic() - start_time

        result = {"time_taken": time_taken, "last_update": time.time()}
        return result

    run_time = 180 if "rllib" in args.trainable else 90

    bucket = None
    if args.bucket:
        bucket = os.path.join(args.bucket, f"test_{int(time.time())}")

    err = None
    try:
        if not uses_ray_client:
            print("This test will *not* use Ray client.")
            result = _run_test(
                args.variant, args.trainable, run_time, bucket, args.cpus_per_trial
            )
        else:
            print("This test will run using Ray client.")

            wait_for_nodes(num_nodes=4, timeout=300.0)

            # This will usually run on the head node
            @ray.remote
            def _get_head_ip():
                return ray.util.get_node_ip_address()

            ip = ray.get(_get_head_ip.remote())

            remote_tune_script = "/tmp/_tune_script.py"

            print(f"Sending tune script to remote node {ip} ({remote_tune_script})")
            send_local_file_to_remote_file(TUNE_SCRIPT, remote_tune_script, ip)
            print("Starting remote cloud test using Ray client")

            _run_test_remote = ray.remote(resources={f"node:{ip}": 0.01}, num_cpus=0)(
                _run_test
            )
            result = ray.get(
                _run_test_remote.remote(
                    args.variant,
                    args.trainable,
                    run_time,
                    bucket,
                    args.cpus_per_trial,
                    remote_tune_script,
                )
            )
    except Exception as e:
        err = e
        result = {}

    if bucket:
        try:
            # clear_bucket_contents(bucket)
            pass
        except Exception as be:
            print(f"Error during cleanup of bucket: {be}")

    with open(release_test_out, "wt") as f:
        json.dump(result, f)

    if err:
        raise err

    print(f"Test for variant {args.variant} SUCCEEDED")
