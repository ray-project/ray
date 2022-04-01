import os
import time
from typing import Optional, List

from ray_release.alerts.handle import handle_result
from ray_release.anyscale_util import get_cluster_name
from ray_release.buildkite.output import buildkite_group, buildkite_open_last
from ray_release.cluster_manager.full import FullClusterManager
from ray_release.command_runner.client_runner import ClientRunner
from ray_release.command_runner.job_runner import JobRunner
from ray_release.command_runner.sdk_runner import SDKRunner
from ray_release.config import (
    Test,
    load_test_cluster_env,
    load_test_cluster_compute,
    DEFAULT_BUILD_TIMEOUT,
    DEFAULT_CLUSTER_TIMEOUT,
    DEFAULT_COMMAND_TIMEOUT,
    RELEASE_PACKAGE_DIR,
    DEFAULT_AUTOSUSPEND_MINS,
    validate_test,
)
from ray_release.exception import (
    ReleaseTestConfigError,
    ReleaseTestSetupError,
    CommandError,
    PrepareCommandError,
    CommandTimeout,
    PrepareCommandTimeout,
    TestCommandError,
    TestCommandTimeout,
    LocalEnvSetupError,
    ClusterEnvCreateError,
)
from ray_release.file_manager.job_file_manager import JobFileManager
from ray_release.file_manager.remote_task import RemoteTaskFileManager
from ray_release.file_manager.session_controller import SessionControllerFileManager
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result, handle_exception
from ray_release.util import run_bash_script

type_str_to_command_runner = {
    "command": SDKRunner,
    "sdk_command": SDKRunner,
    "job": JobRunner,
    "client": ClientRunner,
}

command_runner_to_cluster_manager = {
    SDKRunner: FullClusterManager,
    ClientRunner: FullClusterManager,
    JobRunner: FullClusterManager,
}

file_manager_str_to_file_manager = {
    "sdk": SessionControllerFileManager,
    "client": RemoteTaskFileManager,
    "job": JobFileManager,
}

command_runner_to_file_manager = {
    SDKRunner: SessionControllerFileManager,
    ClientRunner: RemoteTaskFileManager,
    JobFileManager: JobFileManager,
}

uploader_str_to_uploader = {"client": None, "s3": None, "command_runner": None}


def run_release_test(
    test: Test,
    anyscale_project: str,
    result: Result,
    ray_wheels_url: str,
    reporters: Optional[List[Reporter]] = None,
    smoke_test: bool = False,
    cluster_id: Optional[str] = None,
    cluster_env_id: Optional[str] = None,
    no_terminate: bool = False,
) -> Result:
    buildkite_group(":spiral_note_pad: Loading test configuration")

    validate_test(test)

    result.wheels_url = ray_wheels_url
    result.stable = test.get("stable", True)

    buildkite_url = os.getenv("BUILDKITE_BUILD_URL", "")
    if buildkite_url:
        buildkite_url += "#" + os.getenv("BUILDKITE_JOB_ID", "")
    result.buildkite_url = buildkite_url

    working_dir = test["working_dir"]

    old_wd = os.getcwd()
    new_wd = os.path.join(RELEASE_PACKAGE_DIR, working_dir)
    os.chdir(new_wd)

    start_time = time.monotonic()

    run_type = test["run"].get("type", "sdk_command")

    command_runner_cls = type_str_to_command_runner.get(run_type)
    if not command_runner_cls:
        raise ReleaseTestConfigError(
            f"Unknown command runner type: {run_type}. Must be one of "
            f"{list(type_str_to_command_runner.keys())}"
        )

    cluster_manager_cls = command_runner_to_cluster_manager[command_runner_cls]

    file_manager_str = test["run"].get("file_manager", None)
    if file_manager_str:
        if file_manager_str not in file_manager_str_to_file_manager:
            raise ReleaseTestConfigError(
                f"Unknown file manager: {file_manager_str}. Must be one of "
                f"{list(file_manager_str_to_file_manager.keys())}"
            )
        file_manager_cls = file_manager_str_to_file_manager[file_manager_str]
    else:
        file_manager_cls = command_runner_to_file_manager[command_runner_cls]

    # Instantiate managers and command runner
    try:
        cluster_manager = cluster_manager_cls(test["name"], anyscale_project)
        file_manager = file_manager_cls(cluster_manager=cluster_manager)
        command_runner = command_runner_cls(cluster_manager, file_manager, working_dir)
    except Exception as e:
        raise ReleaseTestSetupError(f"Error setting up release test: {e}") from e

    pipeline_exception = None
    try:
        # Load configs
        cluster_env = load_test_cluster_env(test, ray_wheels_url=ray_wheels_url)
        cluster_compute = load_test_cluster_compute(test)

        if cluster_env_id:
            try:
                cluster_manager.cluster_env_id = cluster_env_id
                cluster_manager.build_cluster_env()
                cluster_manager.fetch_build_info()
                logger.info(
                    "Using overridden cluster environment with ID "
                    f"{cluster_env_id} and build ID "
                    f"{cluster_manager.cluster_env_build_id}"
                )
            except Exception as e:
                raise ClusterEnvCreateError(
                    f"Could not get existing overridden cluster environment "
                    f"{cluster_env_id}: {e}"
                ) from e
        else:
            cluster_manager.set_cluster_env(cluster_env)

        cluster_manager.set_cluster_compute(cluster_compute)

        buildkite_group(":nut_and_bolt: Setting up local environment")
        driver_setup_script = test.get("driver_setup", None)
        if driver_setup_script:
            try:
                run_bash_script(driver_setup_script)
            except Exception as e:
                raise LocalEnvSetupError(f"Driver setup script failed: {e}") from e

        # Install local dependencies
        command_runner.prepare_local_env(ray_wheels_url)
        command_timeout = test["run"].get("timeout", DEFAULT_COMMAND_TIMEOUT)

        # Start cluster
        if cluster_id:
            buildkite_group(":rocket: Using existing cluster")
            # Re-use existing cluster ID for development
            cluster_manager.cluster_id = cluster_id
            cluster_manager.cluster_name = get_cluster_name(cluster_id)
        else:
            buildkite_group(":gear: Building cluster environment")
            build_timeout = test["run"].get("build_timeout", DEFAULT_BUILD_TIMEOUT)

            if cluster_env_id:
                cluster_manager.cluster_env_id = cluster_env_id

            cluster_manager.build_configs(timeout=build_timeout)

            cluster_timeout = test["run"].get(
                "session_timeout", DEFAULT_CLUSTER_TIMEOUT
            )

            autosuspend_mins = test["cluster"].get("autosuspend_mins", None)
            if autosuspend_mins:
                cluster_manager.autosuspend_minutes = autosuspend_mins
            else:
                cluster_manager.autosuspend_minutes = min(
                    DEFAULT_AUTOSUSPEND_MINS, int(command_timeout / 60) + 10
                )

            buildkite_group(":rocket: Starting up cluster")
            cluster_manager.start_cluster(timeout=cluster_timeout)

        result.cluster_url = cluster_manager.get_cluster_url()

        # Upload files
        buildkite_group(":wrench: Preparing remote environment")
        command_runner.prepare_remote_env()

        wait_for_nodes = test["run"].get("wait_for_nodes", None)
        if wait_for_nodes:
            buildkite_group(":stopwatch: Waiting for nodes to come up")
            num_nodes = test["run"]["wait_for_nodes"]["num_nodes"]
            wait_timeout = test["run"]["wait_for_nodes"]["timeout"]
            command_runner.wait_for_nodes(num_nodes, wait_timeout)

        prepare_cmd = test["run"].get("prepare", None)
        if prepare_cmd:
            prepare_timeout = test["run"].get("prepare_timeout", command_timeout)
            try:
                command_runner.run_prepare_command(prepare_cmd, timeout=prepare_timeout)
            except CommandError as e:
                raise PrepareCommandError(e)
            except CommandTimeout as e:
                raise PrepareCommandTimeout(e)

        buildkite_group(":runner: Running test script")
        command = test["run"]["script"]
        command_env = {}

        if smoke_test:
            command = f"{command} --smoke-test"
            command_env["IS_SMOKE_TEST"] = "1"

        is_long_running = test["run"].get("long_running", False)

        try:
            command_runner.run_command(
                command, env=command_env, timeout=command_timeout
            )
        except CommandError as e:
            raise TestCommandError(e)
        except CommandTimeout as e:
            if not is_long_running:
                # Only raise error if command is not long running
                raise TestCommandTimeout(e)

        buildkite_group(":floppy_disk: Fetching results")
        try:
            command_results = command_runner.fetch_results()
        except Exception as e:
            logger.error("Could not fetch results for test command")
            logger.exception(e)
            command_results = {}

        # Postprocess result:
        if "last_update" in command_results:
            command_results["last_update_diff"] = time.time() - command_results.get(
                "last_update", 0.0
            )
        if smoke_test:
            command_results["smoke_test"] = True

        result.results = command_results
        result.status = "finished"

    except Exception as e:
        logger.exception(e)
        buildkite_open_last()
        pipeline_exception = e

    try:
        last_logs = command_runner.get_last_logs()
    except Exception as e:
        logger.error(f"Error fetching logs: {e}")
        last_logs = "No logs could be retrieved."

    result.last_logs = last_logs

    if not no_terminate:
        buildkite_group(":earth_africa: Terminating cluster")
        try:
            cluster_manager.terminate_cluster(wait=False)
        except Exception as e:
            logger.error(f"Could not terminate cluster: {e}")

    time_taken = time.monotonic() - start_time
    result.runtime = time_taken

    os.chdir(old_wd)

    if not pipeline_exception:
        buildkite_group(":mag: Interpreting results")
        # Only handle results if we didn't run into issues earlier
        try:
            handle_result(test, result)
        except Exception as e:
            pipeline_exception = e

    if pipeline_exception:
        buildkite_group(":rotating_light: Handling errors")
        exit_code, error_type, runtime = handle_exception(pipeline_exception)

        result.return_code = exit_code.value
        result.status = error_type
        if runtime is not None:
            result.runtime = runtime

    buildkite_group(":memo: Reporting results", open=True)
    reporters = reporters or []
    for reporter in reporters:
        try:
            reporter.report_result(test, result)
        except Exception as e:
            logger.error(f"Error reporting results via {type(reporter)}: {e}")

    if pipeline_exception:
        raise pipeline_exception

    return result
