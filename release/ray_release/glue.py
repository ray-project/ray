import os
import time
import traceback
from typing import Optional, List, Tuple

from ray_release.alerts.handle import handle_result, require_result
from ray_release.anyscale_util import get_cluster_name
from ray_release.buildkite.output import buildkite_group, buildkite_open_last
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.cluster_manager.full import FullClusterManager
from ray_release.cluster_manager.minimal import MinimalClusterManager
from ray_release.command_runner.job_runner import JobRunner
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.command_runner.anyscale_job_runner import AnyscaleJobRunner
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
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result, handle_exception
from ray_release.signal_handling import (
    setup_signal_handling,
    reset_signal_handling,
    register_handler,
)
from ray_release.util import (
    run_bash_script,
    get_pip_packages,
    reinstall_anyscale_dependencies,
)

type_str_to_command_runner = {
    "job": JobRunner,
    "anyscale_job": AnyscaleJobRunner,
}

command_runner_to_cluster_manager = {
    JobRunner: FullClusterManager,
    AnyscaleJobRunner: MinimalClusterManager,
}

DEFAULT_RUN_TYPE = "anyscale_job"
TIMEOUT_BUFFER_MINUTES = 15


def _get_extra_tags_from_env() -> dict:
    env_vars = (
        "BUILDKITE_JOB_ID",
        "BUILDKITE_PULL_REQUEST",
        "BUILDKITE_PIPELINE_SLUG",
        "BUILDKITE_SOURCE",
        "RELEASE_FREQUENCY",
    )
    return {key.lower(): os.getenv(key, "") for key in env_vars}


def _load_test_configuration(
    test: Test,
    anyscale_project: str,
    result: Result,
    ray_wheels_url: str,
    smoke_test: bool = False,
    no_terminate: bool = False,
) -> Tuple[ClusterManager, CommandRunner, str]:
    validate_test(test)
    logger.info(f"Test config: {test}")

    # Populate result paramaters
    result.wheels_url = ray_wheels_url
    result.stable = test.get("stable", True)
    result.smoke_test = smoke_test
    buildkite_url = os.getenv("BUILDKITE_BUILD_URL", "")
    buildkite_job_id = os.getenv("BUILDKITE_JOB_ID", "")
    if buildkite_url:
        buildkite_url += "#" + buildkite_job_id
    result.buildkite_url = buildkite_url
    result.buildkite_job_id = buildkite_job_id

    # Setting up working directory
    working_dir = test["working_dir"]
    new_wd = os.path.join(RELEASE_PACKAGE_DIR, working_dir)
    os.chdir(new_wd)

    run_type = test["run"].get("type", DEFAULT_RUN_TYPE)

    # Workaround while Anyscale Jobs don't support leaving cluster alive
    # after the job has finished.
    # TODO: Remove once we have support in Anyscale
    if no_terminate and run_type == "anyscale_job":
        logger.warning(
            "anyscale_job run type does not support --no-terminate. "
            "Switching to job (Ray Job) run type."
        )
        run_type = "job"

    command_runner_cls = type_str_to_command_runner.get(run_type)
    if not command_runner_cls:
        raise ReleaseTestConfigError(
            f"Unknown command runner type: {run_type}. Must be one of "
            f"{list(type_str_to_command_runner.keys())}"
        )

    cluster_manager_cls = command_runner_to_cluster_manager[command_runner_cls]
    logger.info(f"Got command runner cls: {command_runner_cls}")
    # Extra tags to be set on resources on cloud provider's side
    extra_tags = _get_extra_tags_from_env()
    # We don't need other attributes as they can be derived from the name
    extra_tags["test_name"] = str(test["name"])
    extra_tags["test_smoke_test"] = str(result.smoke_test)
    result.extra_tags = extra_tags

    artifact_path = test["run"].get("artifact_path", None)

    # Instantiate managers and command runner
    try:
        cluster_manager = cluster_manager_cls(
            test["name"],
            anyscale_project,
            smoke_test=smoke_test,
        )
        command_runner = command_runner_cls(
            cluster_manager,
            JobFileManager(cluster_manager=cluster_manager),
            working_dir,
            artifact_path=artifact_path,
        )
    except Exception as e:
        raise ReleaseTestSetupError(f"Error setting up release test: {e}") from e

    return cluster_manager, command_runner, artifact_path


def _setup_cluster_environment(
    test: Test,
    result: Result,
    cluster_manager: ClusterManager,
    ray_wheels_url: str,
    cluster_env_id: Optional[str],
) -> Tuple[str, int, int, int, int]:
    setup_signal_handling()
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
    cluster_timeout = int(test["run"].get("session_timeout", DEFAULT_CLUSTER_TIMEOUT))

    # Get prepare command timeout, if any
    prepare_cmd = test["run"].get("prepare", None)
    if prepare_cmd:
        prepare_timeout = test["run"].get("prepare_timeout", command_timeout)
    else:
        prepare_timeout = 0

    # Base maximum uptime on the combined command and prepare timeouts
    command_and_prepare_timeout = command_timeout + prepare_timeout

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
            DEFAULT_AUTOSUSPEND_MINS,
            int(command_and_prepare_timeout / 60) + TIMEOUT_BUFFER_MINUTES,
        )
        # Maximum uptime should be based on the command timeout, not the
        # DEFAULT_AUTOSUSPEND_MINS
        autosuspend_base = (
            int(command_and_prepare_timeout / 60) + TIMEOUT_BUFFER_MINUTES
        )

    maximum_uptime_minutes = test["cluster"].get("maximum_uptime_minutes", None)
    if maximum_uptime_minutes:
        cluster_manager.maximum_uptime_minutes = maximum_uptime_minutes
    else:
        cluster_manager.maximum_uptime_minutes = (
            autosuspend_base + wait_timeout + TIMEOUT_BUFFER_MINUTES
        )

    # Set cluster compute here. Note that this may use timeouts provided
    # above.
    cluster_manager.set_cluster_compute(
        cluster_compute,
        extra_tags=result.extra_tags,
    )

    return prepare_cmd, prepare_timeout, build_timeout, cluster_timeout, command_timeout


def _setup_local_environment(
    test: Test,
    command_runner: CommandRunner,
    ray_wheels_url: str,
) -> None:
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


def _local_environment_information(
    result: Result,
    cluster_manager: ClusterManager,
    command_runner: CommandRunner,
    build_timeout: int,
    cluster_timeout: int,
    no_terminate: bool,
    cluster_id: Optional[str],
    cluster_env_id: Optional[str],
) -> None:
    pip_packages = get_pip_packages()
    pip_package_string = "\n".join(pip_packages)
    logger.info(f"Installed python packages:\n{pip_package_string}")

    if isinstance(cluster_manager, FullClusterManager):
        if not no_terminate:
            register_handler(
                lambda sig, frame: cluster_manager.terminate_cluster(wait=True)
            )

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

        if isinstance(cluster_manager, FullClusterManager):
            buildkite_group(":rocket: Starting up cluster")
            cluster_manager.start_cluster(timeout=cluster_timeout)
        elif isinstance(command_runner, AnyscaleJobRunner):
            command_runner.job_manager.cluster_startup_timeout = cluster_timeout

    result.cluster_url = cluster_manager.get_cluster_url()
    result.cluster_id = cluster_manager.cluster_id


def _prepare_remote_environment(
    test: Test,
    command_runner: CommandRunner,
    prepare_cmd: bool,
    prepare_timeout: int,
) -> None:
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

    if prepare_cmd:
        try:
            command_runner.run_prepare_command(prepare_cmd, timeout=prepare_timeout)
        except CommandError as e:
            raise PrepareCommandError(e)
        except CommandTimeout as e:
            raise PrepareCommandTimeout(e)


def _running_test_script(
    test: Test,
    smoke_test: bool,
    command_runner: CommandRunner,
    command_timeout: int,
) -> None:
    command = test["run"]["script"]
    command_env = {}

    if smoke_test:
        command = f"{command} --smoke-test"
        command_env["IS_SMOKE_TEST"] = "1"

    is_long_running = test["run"].get("long_running", False)

    try:
        command_runner.run_command(
            command,
            env=command_env,
            timeout=command_timeout,
            raise_on_timeout=not is_long_running,
        )
    except (
        TestCommandError,
        PrepareCommandError,
        TestCommandTimeout,
        PrepareCommandTimeout,
    ) as e:
        raise e
    except CommandError as e:
        raise TestCommandError(e)
    except CommandTimeout as e:
        if not is_long_running:
            # Only raise error if command is not long running
            raise TestCommandTimeout(e)


def _fetching_results(
    result: Result,
    command_runner: CommandRunner,
    artifact_path: Optional[str],
    smoke_test: bool,
    start_time_unix: int,
) -> Tuple[dict, Exception]:
    fetch_result_exception = None
    try:
        command_results = command_runner.fetch_results()
    except Exception as e:
        logger.exception(f"Could not fetch results for test command: {e}")
        command_results = {}
        fetch_result_exception = e

    if artifact_path:
        try:
            command_runner.fetch_artifact()
        except Exception as e:
            logger.error("Could not fetch artifact for test command")
            logger.exception(e)

    # Postprocess result:
    if "last_update" in command_results:
        command_results["last_update_diff"] = time.time() - command_results.get(
            "last_update", 0.0
        )

    try:
        # Logic duplicated in ray_release/command_runner/_anyscale_job_wrapper.py
        # Timeout is the time the test took divided by 200
        # (~7 minutes for a 24h test) but no less than 30s
        # and no more than 900s
        metrics_timeout = max(30, min((time.time() - start_time_unix) / 200, 900))
        command_runner.save_metrics(start_time_unix, timeout=metrics_timeout)
        metrics = command_runner.fetch_metrics()
    except Exception as e:
        logger.exception(f"Could not fetch metrics for test command: {e}")
        metrics = {}

    if smoke_test:
        command_results["smoke_test"] = True

    result.results = command_results
    result.status = "finished"

    return metrics, fetch_result_exception


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
    old_wd = os.getcwd()
    start_time = time.monotonic()
    command_runner = None
    cluster_manager = None
    pipeline_exception = None
    # non critical for some tests. So separate it from the general one.
    fetch_result_exception = None
    try:
        buildkite_group(":spiral_note_pad: Loading test configuration")
        cluster_manager, command_runner, artifact_path = _load_test_configuration(
            test,
            anyscale_project,
            result,
            ray_wheels_url,
            smoke_test,
            no_terminate,
        )
        buildkite_group(":nut_and_bolt: Setting up cluster environment")
        (
            prepare_cmd,
            prepare_timeout,
            build_timeout,
            cluster_timeout,
            command_timeout,
        ) = _setup_cluster_environment(
            test,
            result,
            cluster_manager,
            ray_wheels_url,
            cluster_env_id,
        )

        buildkite_group(":nut_and_bolt: Setting up local environment")
        _setup_local_environment(test, command_runner, ray_wheels_url)

        # Print installed pip packages
        buildkite_group(":bulb: Local environment information")
        _local_environment_information(
            result,
            cluster_manager,
            command_runner,
            build_timeout,
            cluster_timeout,
            no_terminate,
            cluster_id,
            cluster_env_id,
        )

        # Upload files
        buildkite_group(":wrench: Preparing remote environment")
        _prepare_remote_environment(
            test,
            command_runner,
            prepare_cmd,
            prepare_timeout,
        )

        buildkite_group(":runner: Running test script")
        start_time_unix = time.time()
        _running_test_script(
            test,
            smoke_test,
            command_runner,
            command_timeout,
        )

        buildkite_group(":floppy_disk: Fetching results")
        metrics, fetch_result_exception = _fetching_results(
            result,
            command_runner,
            artifact_path,
            smoke_test,
            start_time_unix,
        )
    except Exception as e:
        logger.exception(e)
        buildkite_open_last()
        pipeline_exception = e
        metrics = {}

    # Obtain the cluster URL again as it is set after the
    # command was run in case of anyscale jobs
    if isinstance(command_runner, AnyscaleJobRunner):
        result.cluster_url = cluster_manager.get_cluster_url()
        result.cluster_id = cluster_manager.cluster_id
        result.job_url = command_runner.job_manager.job_url
        result.job_id = command_runner.job_manager.job_id

    result.last_logs = command_runner.get_last_logs() if command_runner else None

    if not no_terminate and cluster_manager:
        buildkite_group(":earth_africa: Terminating cluster")
        cluster_manager.terminate_cluster(wait=False)

    if hasattr(command_runner, "cleanup"):
        command_runner.cleanup()

    reset_signal_handling()

    time_taken = time.monotonic() - start_time
    result.runtime = time_taken
    result.prometheus_metrics = metrics

    os.chdir(old_wd)

    if not pipeline_exception:
        if require_result(test) and fetch_result_exception:
            pipeline_exception = fetch_result_exception
        else:
            buildkite_group(":mag: Interpreting results")
            # Only handle results if we didn't run into issues earlier
            try:
                handle_result(test, result)
            except Exception as e:
                pipeline_exception = e

    if pipeline_exception:
        buildkite_group(":rotating_light: Handling errors")
        exit_code, result_status, runtime = handle_exception(
            pipeline_exception,
            result.runtime,
        )

        result.return_code = exit_code.value
        result.status = result_status.value
        if runtime is not None:
            result.runtime = runtime
        try:
            raise pipeline_exception
        except Exception:
            if not result.last_logs:
                result.last_logs = traceback.format_exc()

    buildkite_group(":memo: Reporting results", open=True)
    for reporter in reporters or []:
        reporter.report_result(test, result)

    if pipeline_exception:
        raise pipeline_exception

    return result
