"""
This script provides extra functionality for Anyscale Jobs tests.
It will be ran on the cluster.

We need to reimplement some utility functions here as it will not
have access to the ray_release package.
"""

import argparse
import time
import os
from pathlib import Path
import subprocess
import multiprocessing
import json
import sys
import logging
from urllib.parse import urlparse
from typing import Optional, List, Tuple

OUTPUT_JSON_FILENAME = "output.json"
AWS_CP_TIMEOUT = 300
TIMEOUT_RETURN_CODE = 124  # same as bash timeout

installed_pips = []
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stderr)
formatter = logging.Formatter(
    fmt="[%(levelname)s %(asctime)s] %(filename)s: %(lineno)d  %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def exponential_backoff_retry(
    f, retry_exceptions, initial_retry_delay_s, max_retries
) -> None:
    retry_cnt = 0
    retry_delay_s = initial_retry_delay_s
    while True:
        try:
            return f()
        except retry_exceptions as e:
            retry_cnt += 1
            if retry_cnt > max_retries:
                raise
            logger.warning(
                f"Retry function call failed due to {e} "
                f"in {retry_delay_s} seconds..."
            )
            time.sleep(retry_delay_s)
            retry_delay_s *= 2


def install_pip(pip: str):
    if pip in installed_pips:
        return
    subprocess.run(["pip", "install", "-q", pip], check=True)
    installed_pips.append(pip)


def run_storage_cp(source: str, target: str):
    if not source or not target:
        return False

    if not Path(source).exists():
        logger.warning(f"Couldn't upload to cloud storage: '{source}' does not exist.")
        return False

    storage_service = urlparse(target).scheme
    cp_cmd_args = []
    if storage_service == "s3":
        cp_cmd_args = [
            "aws",
            "s3",
            "cp",
            source,
            target,
            "--acl",
            "bucket-owner-full-control",
        ]
    elif storage_service == "gs":
        install_pip("gsutil")
        cp_cmd_args = [
            "gsutil",
            "cp",
            source,
            target,
        ]
    else:
        raise Exception(f"Not supporting storage service: {storage_service}")

    try:
        exponential_backoff_retry(
            lambda: subprocess.run(
                cp_cmd_args,
                timeout=AWS_CP_TIMEOUT,
                check=True,
            ),
            subprocess.SubprocessError,
            initial_retry_delay_s=10,
            max_retries=3,
        )
        return True
    except subprocess.SubprocessError:
        logger.exception("Couldn't upload to cloud storage.")
        return False


def collect_metrics(time_taken: float) -> bool:
    if "METRICS_OUTPUT_JSON" not in os.environ:
        return False

    # Timeout is the time the test took divided by 200
    # (~7 minutes for a 24h test) but no less than 90s
    # and no more than 900s
    metrics_timeout = max(90, min((time.time() - time_taken) / 200, 900))
    try:
        subprocess.run(
            [
                "python",
                "prometheus_metrics.py",
                str(time_taken),
                "--path",
                os.environ["METRICS_OUTPUT_JSON"],
            ],
            timeout=metrics_timeout,
            check=True,
        )
        return True
    except subprocess.SubprocessError:
        logger.exception("Couldn't collect metrics.")
        return False


# Has to be here so it can be pickled
def _run_bash_command_subprocess(command: str, timeout: float):
    """Ran in a multiprocessing process."""
    try:
        subprocess.run(command, check=True, timeout=timeout)
        return_code = 0
    except subprocess.TimeoutExpired:
        return_code = TIMEOUT_RETURN_CODE
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
    print(f"Subprocess return code: {return_code}", file=sys.stderr)
    # Exit so the return code is propagated to the outer process
    sys.exit(return_code)


def run_bash_command(workload: str, timeout: float):
    timeout = timeout if timeout > 0 else None
    cwd = Path.cwd()
    workload_path = cwd / "workload.sh"
    workload_path = workload_path.resolve()
    with open(workload_path, "w") as fp:
        fp.write(workload)

    command = ["bash", "-x", str(workload_path)]
    logger.info(f"Running command {workload}")

    # Pop job's runtime env to allow workload's runtime env to take precedence
    # TODO: Confirm this is safe
    os.environ.pop("RAY_JOB_CONFIG_JSON_ENV_VAR", None)

    # We use multiprocessing with 'spawn' context to avoid
    # forking (as happens when using subprocess directly).
    # Forking messes up Ray interactions and causes deadlocks.
    return_code = None
    try:
        ctx = multiprocessing.get_context("spawn")
        p = ctx.Process(target=_run_bash_command_subprocess, args=(command, timeout))
        p.start()
        logger.info(f"Starting process {p.pid}.")
        # Add a little extra to the timeout as _run_bash_command_subprocess
        # also has a timeout internally and it's cleaner to use that
        p.join(timeout=timeout + 10)
    except multiprocessing.TimeoutError:
        return_code = TIMEOUT_RETURN_CODE
    except multiprocessing.ProcessError:
        pass
    finally:
        if p.is_alive():
            logger.warning(f"Terminating process {p.pid} forcefully.")
            p.terminate()
        if return_code is None:
            return_code = p.exitcode
        os.remove(str(workload_path))
    logger.info(f"Process {p.pid} exited with return code {return_code}.")
    assert return_code is not None
    return return_code


def run_prepare_commands(
    prepare_commands: List[str], prepare_commands_timeouts: List[float]
) -> Tuple[bool, List[int], float]:
    """Run prepare commands. All commands must pass. Fails fast."""
    prepare_return_codes = []
    prepare_passed = True
    prepare_time_taken = None

    if not prepare_commands:
        return prepare_passed, prepare_return_codes, prepare_time_taken

    logger.info("### Starting prepare commands ###")
    for prepare_command, timeout in zip(prepare_commands, prepare_commands_timeouts):
        command_start_time = time.monotonic()
        prepare_return_codes.append(run_bash_command(prepare_command, timeout))
        prepare_time_taken = time.monotonic() - command_start_time
        return_code = prepare_return_codes[-1]

        if return_code == 0:
            continue

        timed_out = return_code == TIMEOUT_RETURN_CODE
        if timed_out:
            logger.error(
                "Prepare command timed out. " f"Time taken: {prepare_time_taken}"
            )
        else:
            logger.info(
                f"Prepare command finished with return code {return_code}. "
                f"Time taken: {prepare_time_taken}"
            )
        logger.error("Prepare command failed.")
        prepare_passed = False
        break

    return prepare_passed, prepare_return_codes, prepare_time_taken


def main(
    test_workload: str,
    test_workload_timeout: float,
    test_no_raise_on_timeout: bool,
    results_cloud_storage_uri: Optional[str],
    metrics_cloud_storage_uri: Optional[str],
    output_cloud_storage_uri: Optional[str],
    upload_cloud_storage_uri: Optional[str],
    artifact_path: Optional[str],
    prepare_commands: List[str],
    prepare_commands_timeouts: List[str],
):
    """
    This function provides extra functionality for an Anyscale Job.

    1. Runs prepare commands and handles their timeouts
    2. Runs the actual test workload and handles its timeout
    3. Uploads test results.json
    4. Gathers prometheus metrics
    5. Uploads prometheus metrics.json
    6. Uploads output.json
    """
    logger.info("### Starting ###")
    start_time = time.monotonic()

    if len(prepare_commands) != len(prepare_commands_timeouts):
        raise ValueError(
            "`prepare_commands` and `prepare_commands_timeouts` must "
            "have the same length."
        )

    # Run prepare commands. All prepare commands must pass.
    (
        prepare_passed,
        prepare_return_codes,
        last_prepare_time_taken,
    ) = run_prepare_commands(prepare_commands, prepare_commands_timeouts)

    uploaded_results = False
    collected_metrics = False
    uploaded_metrics = False
    uploaded_artifact = artifact_path is not None
    workload_time_taken = None

    # If all prepare commands passed, run actual test workload.
    if prepare_passed:
        logger.info("### Starting entrypoint ###")
        command_start_time = time.monotonic()
        return_code = run_bash_command(test_workload, test_workload_timeout)
        workload_time_taken = time.monotonic() - command_start_time

        timed_out = return_code == TIMEOUT_RETURN_CODE
        if timed_out:
            msg = f"Timed out. Time taken: {workload_time_taken}"
            if test_no_raise_on_timeout:
                logger.info(msg)
            else:
                logger.error(msg)
        else:
            logger.info(
                f"Finished with return code {return_code}. "
                f"Time taken: {workload_time_taken}"
            )

        # Upload results.json
        uploaded_results = run_storage_cp(
            os.environ.get("TEST_OUTPUT_JSON", None), results_cloud_storage_uri
        )

        # Collect prometheus metrics
        collected_metrics = collect_metrics(workload_time_taken)
        if collected_metrics:
            # Upload prometheus metrics
            uploaded_metrics = run_storage_cp(
                os.environ.get("METRICS_OUTPUT_JSON", None), metrics_cloud_storage_uri
            )

        uploaded_artifact = run_storage_cp(
            artifact_path,
            os.path.join(
                upload_cloud_storage_uri, os.environ["USER_GENERATED_ARTIFACT"]
            )
            if "USER_GENERATED_ARTIFACT" in os.environ
            else None,
        )

    else:
        return_code = None

    total_time_taken = time.monotonic() - start_time
    output_json = {
        "return_code": return_code,
        "prepare_return_codes": prepare_return_codes,
        "last_prepare_time_taken": last_prepare_time_taken,
        "workload_time_taken": workload_time_taken,
        "total_time_taken": total_time_taken,
        "uploaded_results": uploaded_results,
        "collected_metrics": collected_metrics,
        "uploaded_metrics": uploaded_metrics,
        "uploaded_artifact": uploaded_artifact,
    }
    output_json = json.dumps(
        output_json, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )

    output_json_file = (Path.cwd() / OUTPUT_JSON_FILENAME).resolve()
    with open(output_json_file, "w") as fp:
        fp.write(output_json)

    # Upload output.json
    run_storage_cp(str(output_json_file), output_cloud_storage_uri)

    logger.info("### Finished ###")
    # This will be read by the AnyscaleJobRunner on the buildkite runner
    # if output.json cannot be obtained from cloud storage
    logger.info(f"### JSON |{output_json}| ###")

    # Flush buffers
    logging.shutdown()
    print("", flush=True)
    print("", file=sys.stderr, flush=True)

    if return_code == TIMEOUT_RETURN_CODE and test_no_raise_on_timeout:
        return_code = 0
    elif return_code is None:
        return_code = 1

    time.sleep(1)
    return return_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "test_workload", type=str, help="test workload, eg. python workloads/script.py"
    )
    parser.add_argument(
        "--test-workload-timeout",
        default=3600,
        type=float,
        help="test workload timeout (set to <0 for infinite)",
    )
    parser.add_argument(
        "--test-no-raise-on-timeout",
        action="store_true",
        help="don't fail on timeout",
    )
    parser.add_argument(
        "--results-cloud-storage-uri",
        type=str,
        help="bucket address to upload results.json to",
        required=False,
    )
    parser.add_argument(
        "--metrics-cloud-storage-uri",
        type=str,
        help="bucket address to upload metrics.json to",
        required=False,
    )
    parser.add_argument(
        "--output-cloud-storage-uri",
        type=str,
        help="bucket address to upload output.json to",
        required=False,
    )
    parser.add_argument(
        "--upload-cloud-storage-uri",
        type=str,
        help="root cloud-storage bucket address to upload stuff",
        required=False,
    )
    parser.add_argument(
        "--artifact-path",
        type=str,
        help="user provided artifact path (on head node), must be a single file path",
        required=False,
    )
    parser.add_argument(
        "--prepare-commands", type=str, nargs="*", help="prepare commands to run"
    )
    parser.add_argument(
        "--prepare-commands-timeouts",
        default=3600,
        type=float,
        nargs="*",
        help="timeout for prepare commands (set to <0 for infinite)",
    )

    args = parser.parse_args()
    sys.exit(main(**args.__dict__))
