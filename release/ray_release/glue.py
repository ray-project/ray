import os
import time
from typing import Optional, List

from ray_release.alerts.handle import handle_result
from ray_release.anyscale_util import get_cluster_name
from ray_release.cluster_manager.full import FullClusterManager
from ray_release.cluster_manager.minimal import MinimalClusterManager
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
)
from ray_release.file_manager.remote_task import RemoteTaskFileManager
from ray_release.file_manager.session_controller import SessionControllerFileManager
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result, handle_exception

type_str_to_command_runner = {
    "command": SDKRunner,
    "sdk_command": SDKRunner,
    "job": JobRunner,
    "client": ClientRunner,
}

command_runner_to_cluster_manager = {
    SDKRunner: FullClusterManager,
    ClientRunner: FullClusterManager,
    JobRunner: MinimalClusterManager,
}

command_runner_to_file_manager = {
    SDKRunner: SessionControllerFileManager,
    ClientRunner: RemoteTaskFileManager,
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
    no_terminate: bool = False,
) -> Result:
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
        raise ReleaseTestConfigError(f"Unknown command runner type: {run_type}")

    cluster_manager_cls = command_runner_to_cluster_manager[command_runner_cls]
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

        cluster_manager.set_cluster_env(cluster_env)
        cluster_manager.set_cluster_compute(cluster_compute)

        # Run driver_setup command, install local dependencies
        command_runner.prepare_local_env(ray_wheels_url)

        # Start session
        if cluster_id:
            # Re-use existing cluster ID for development
            cluster_manager.cluster_id = cluster_id
            cluster_manager.cluster_name = get_cluster_name(cluster_id)
        else:
            build_timeout = test["run"].get("build_timeout", DEFAULT_BUILD_TIMEOUT)
            cluster_manager.build_configs(timeout=build_timeout)

            cluster_timeout = test["run"].get(
                "session_timeout", DEFAULT_CLUSTER_TIMEOUT
            )

            autosuspend_mins = test["run"].get("autosuspend_mins", None)
            if autosuspend_mins:
                cluster_manager.autosuspend_minutes = autosuspend_mins

            cluster_manager.start_cluster(timeout=cluster_timeout)

        result.cluster_url = cluster_manager.get_cluster_url()

        # Upload files
        command_runner.prepare_remote_env()

        command_timeout = test["run"].get("timeout", DEFAULT_COMMAND_TIMEOUT)

        prepare_cmd = test["run"].get("prepare", None)
        if prepare_cmd:
            prepare_timeout = test["run"].get("prepare_timeout", command_timeout)
            try:
                command_runner.run_prepare_command(prepare_cmd, timeout=prepare_timeout)
            except CommandError as e:
                raise PrepareCommandError(e)
            except CommandTimeout as e:
                raise PrepareCommandTimeout(e)

        command = test["run"]["script"]
        command_env = {}

        if smoke_test:
            command = f"{command} --smoke-test"
            command_env["IS_SMOKE_TEST"] = "1"

        try:
            command_runner.run_command(
                command, env=command_env, timeout=command_timeout
            )
        except CommandError as e:
            raise TestCommandError(e)
        except CommandTimeout as e:
            raise TestCommandTimeout(e)

        try:
            command_results = command_runner.fetch_results()
        except Exception as e:
            logger.error(f"Could not fetch results for test command: {e}")
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
        pipeline_exception = e

    try:
        last_logs = command_runner.get_last_logs()
    except Exception as e:
        logger.error(f"Error fetching logs: {e}")
        last_logs = "No logs could be retrieved."

    result.last_logs = last_logs

    if not no_terminate:
        try:
            cluster_manager.terminate_cluster(wait=False)
        except Exception as e:
            logger.error(f"Could not terminate cluster: {e}")

    time_taken = time.monotonic() - start_time
    result.runtime = time_taken

    os.chdir(old_wd)

    if not pipeline_exception:
        # Only handle results if we didn't run into issues earlier
        try:
            handle_result(test, result)
        except Exception as e:
            pipeline_exception = e

    if pipeline_exception:
        exit_code, error_type, runtime = handle_exception(pipeline_exception)

        result.return_code = exit_code.value
        result.status = error_type
        if runtime is not None:
            result.runtime = runtime

    reporters = reporters or []
    for reporter in reporters:
        try:
            reporter.report_result(test, result)
        except Exception as e:
            logger.error(f"Error reporting results via {type(reporter)}: {e}")

    if pipeline_exception:
        raise pipeline_exception

    return result
