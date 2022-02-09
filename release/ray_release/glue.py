import os
import time
from typing import Dict, Optional

from ray_release.anyscale_util import get_cluster_name
from ray_release.cluster_manager.full import FullClusterManager
from ray_release.cluster_manager.minimal import MinimalClusterManager
from ray_release.command_runner.client_runner import ClientRunner
from ray_release.command_runner.job_runner import JobRunner
from ray_release.command_runner.sdk_runner import SDKRunner
from ray_release.config import (Test, load_test_cluster_env,
                                load_test_cluster_compute,
                                DEFAULT_BUILD_TIMEOUT, DEFAULT_SESSION_TIMEOUT,
                                DEFAULT_COMMAND_TIMEOUT, RELEASE_PACKAGE_DIR)
from ray_release.exception import ReleaseTestConfigError
from ray_release.file_manager.session_controller import \
    SessionControllerFileManager
from ray_release.util import deep_update

type_str_to_command_runner = {
    "command": SDKRunner,
    "sdk_command": SDKRunner,
    "job": JobRunner,
    "client": ClientRunner
}

command_runner_to_cluster_manager = {
    SDKRunner: FullClusterManager,
    ClientRunner: FullClusterManager,
    JobRunner: MinimalClusterManager
}

command_runner_to_file_manager = {
    SDKRunner: SessionControllerFileManager,
}

uploader_str_to_uploader = {"client": None, "s3": None, "command_runner": None}


def run_release_test(test: Test,
                     anyscale_project: str,
                     results: Dict,
                     ray_wheels_url: str,
                     cluster_id: Optional[str] = None,
                     no_terminate: bool = False) -> Dict:
    results["_runtime"] = None
    results["_session_url"] = None
    results["_commit_url"] = ray_wheels_url
    results["_stable"] = test.get("stable", True)

    working_dir = test["working_dir"]

    old_wd = os.getcwd()
    new_wd = os.path.join(RELEASE_PACKAGE_DIR, working_dir)
    os.chdir(new_wd)

    start_time = time.monotonic()

    run_type = test["run"].get("type", "sdk_command")

    command_runner_cls = type_str_to_command_runner.get(run_type)
    if not command_runner_cls:
        raise ReleaseTestConfigError(
            f"Unknown command runner type: {run_type}")

    cluster_manager_cls = command_runner_to_cluster_manager[command_runner_cls]
    file_manager_cls = command_runner_to_file_manager[command_runner_cls]

    # Instantiate managers and command runner
    cluster_manager = cluster_manager_cls(test["name"], anyscale_project)
    file_manager = file_manager_cls(cluster_manager=cluster_manager)
    command_runner = command_runner_cls(cluster_manager, file_manager,
                                        working_dir)

    exc = None
    try:
        # Load configs
        cluster_env = load_test_cluster_env(
            test, ray_wheels_url=ray_wheels_url)
        cluster_compute = load_test_cluster_compute(test)

        # Run driver_setup command, install local dependencies
        command_runner.prepare_local_env()

        # Start session
        if cluster_id:
            # Re-use existing cluster ID for development
            cluster_manager.cluster_id = cluster_id
            cluster_manager.cluster_name = get_cluster_name(cluster_id)
        else:
            build_timeout = test["run"].get("build_timeout",
                                            DEFAULT_BUILD_TIMEOUT)
            cluster_manager.set_cluster_env(cluster_env)
            cluster_manager.set_cluster_compute(cluster_compute)
            cluster_manager.build_configs(timeout=build_timeout)

            session_timeout = test["run"].get("session_timeout",
                                              DEFAULT_SESSION_TIMEOUT)

            autosuspend_mins = test["run"].get("autosuspend_mins", None)
            if autosuspend_mins:
                cluster_manager.autosuspend_minutes = autosuspend_mins

            cluster_manager.start_cluster(timeout=session_timeout)

        # session_url only for legacy support
        results["_session_url"] = cluster_manager.get_cluster_url()
        results["_cluster_url"] = cluster_manager.get_cluster_url()

        # Upload files
        command_runner.prepare_remote_env()

        command_timeout = test["run"].get("timeout", DEFAULT_COMMAND_TIMEOUT)

        prepare_cmd = test["run"].get("prepare", None)
        if prepare_cmd:
            prepare_timeout = test["run"].get("prepare_timeout",
                                              command_timeout)
            command_runner.run_command(prepare_cmd, timeout=prepare_timeout)

        command = test["run"]["script"]
        command_runner.run_command(command, timeout=command_timeout)

        command_results = command_runner.fetch_results()
        deep_update(results, command_results)
    except Exception as e:
        exc = e
    finally:
        if not no_terminate:
            cluster_manager.terminate_cluster(wait=False)

    time_taken = time.monotonic() - start_time
    results["_runtime"] = time_taken

    os.chdir(old_wd)

    if exc:
        raise exc

    print(results)

    return results
