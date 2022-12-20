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
    DEFAULT_BUILD_TIMEOUT,
    DEFAULT_CLUSTER_TIMEOUT,
    DEFAULT_COMMAND_TIMEOUT,
    DEFAULT_WAIT_FOR_NODES_TIMEOUT,
    RELEASE_PACKAGE_DIR,
    DEFAULT_AUTOSUSPEND_MINS,
    validate_test,
)
from ray_release.template import load_test_cluster_env, load_test_cluster_compute
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
from ray_release.util import (
    run_bash_script,
    get_pip_packages,
    reinstall_anyscale_dependencies,
)

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


def _get_extra_tags() -> dict:
    env_vars = (
        "BUILDKITE_JOB_ID",
        "BUILDKITE_PULL_REQUEST",
        "BUILDKITE_PIPELINE_SLUG",
        "BUILDKITE_RETRY_COUNT",
        "BUILDKITE_SOURCE",
        "RELEASE_FREQUENCY",
    )
    return {key.lower(): os.getenv(key, "") for key in env_vars}


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
    result.smoke_test = smoke_test

    buildkite_url = os.getenv("BUILDKITE_BUILD_URL", "")
    buildkite_job_id = os.getenv("BUILDKITE_JOB_ID", "")

    if buildkite_url:
        buildkite_url += "#" + buildkite_job_id

    result.buildkite_url = buildkite_url
    result.buildkite_job_id = buildkite_job_id

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

    # Extra tags to be set on resources on cloud provider's side
    extra_tags = _get_extra_tags()
    result.extra_tags = extra_tags

    # Instantiate managers and command runner
    try:
        cluster_manager = cluster_manager_cls(
            test["name"],
            anyscale_project,
            smoke_test=smoke_test,
        )
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

        # Load some timeouts
        build_timeout = int(test["run"].get("build_timeout", DEFAULT_BUILD_TIMEOUT))
        command_timeout = int(test["run"].get("timeout", DEFAULT_COMMAND_TIMEOUT))
        cluster_timeout = int(
            test["run"].get("session_timeout", DEFAULT_CLUSTER_TIMEOUT)
        )
        # Use default timeout = 0 here if wait_for_nodes is empty. This is to make
        # sure we don't inflate the maximum_uptime_minutes too much if we don't wait
        # for nodes at all.
        # The actual default will be otherwise loaded further down.
        wait_timeout = int(test["run"].get("wait_for_nodes", {}).get("timeout", 0))

        autosuspend_mins = test["cluster"].get("autosuspend_mins", None)
        if autosuspend_mins:
            cluster_manager.autosuspend_minutes = autosuspend_mins
            autosuspend_base = autosuspend_mins
        else:
            cluster_manager.autosuspend_minutes = min(
                DEFAULT_AUTOSUSPEND_MINS, int(command_timeout / 60) + 10
            )
            # Maximum uptime should be based on the command timeout, not the
            # DEFAULT_AUTOSUSPEND_MINS
            autosuspend_base = int(command_timeout / 60) + 10

        maximum_uptime_minutes = test["cluster"].get("maximum_uptime_minutes", None)
        if maximum_uptime_minutes:
            cluster_manager.maximum_uptime_minutes = maximum_uptime_minutes
        else:
            cluster_manager.maximum_uptime_minutes = (
                autosuspend_base + wait_timeout + 10
            )

        # Set cluster compute here. Note that this may use timeouts provided
        # above.
        cluster_manager.set_cluster_compute(
            cluster_compute,
            extra_tags=extra_tags,
        )

        buildkite_group(":nut_and_bolt: Setting up local environment")
        driver_setup_script = test.get("driver_setup", None)
        if driver_setup_script:
            try:
                run_bash_script(driver_setup_script)
            except Exception as e:
                raise LocalEnvSetupError(f"Driver setup script failed: {e}") from e

        # Install local dependencies
        command_runner.prepare_local_env(ray_wheels_url)

        # Re-install anyscale package as local dependencies might have changed
        # from local env setup
        reinstall_anyscale_dependencies()

        # Print installed pip packages
        buildkite_group(":bulb: Local environment information")
        pip_packages = get_pip_packages()
        pip_package_string = "\n".join(pip_packages)
        logger.info(f"Installed python packages:\n{pip_package_string}")

        # Start cluster
        if cluster_id:
            buildkite_group(":rocket: Using existing cluster")
            # Re-use existing cluster ID for development
            cluster_manager.cluster_id = cluster_id
            cluster_manager.cluster_name = get_cluster_name(cluster_id)
        else:
            buildkite_group(":gear: Building cluster environment")

            if cluster_env_id:
                cluster_manager.cluster_env_id = cluster_env_id

            cluster_manager.build_configs(timeout=build_timeout)

            buildkite_group(":rocket: Starting up cluster")
            cluster_manager.start_cluster(timeout=cluster_timeout)

        result.cluster_url = cluster_manager.get_cluster_url()
        result.cluster_id = cluster_manager.cluster_id

        # Upload files
        buildkite_group(":wrench: Preparing remote environment")
        command_runner.prepare_remote_env()

        wait_for_nodes = test["run"].get("wait_for_nodes", None)
        if wait_for_nodes:
            buildkite_group(":stopwatch: Waiting for nodes to come up")
            # Overwrite wait_timeout from above to account for better default
            wait_timeout = int(
                wait_for_nodes.get("timeout", DEFAULT_WAIT_FOR_NODES_TIMEOUT)
            )
            num_nodes = test["run"]["wait_for_nodes"]["num_nodes"]
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

        start_time_unix = time.time()

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

        try:
            command_runner.save_metrics(start_time_unix)
            metrics = command_runner.fetch_metrics()
        except Exception as e:
            logger.exception(f"Could not fetch metrics for test command: {e}")
            metrics = {}

        if smoke_test:
            command_results["smoke_test"] = True

        result.results = command_results
        result.status = "finished"

    except Exception as e:
        logger.exception(e)
        buildkite_open_last()
        pipeline_exception = e
        metrics = {}

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
    result.prometheus_metrics = metrics

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
