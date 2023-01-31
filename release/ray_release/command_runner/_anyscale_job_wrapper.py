import argparse
import time
import os
from pathlib import Path
import subprocess
import multiprocessing
import json
import sys
import logging
from typing import List

AWS_CP_TIMEOUT = 300

logger = logging.getLogger(__name__)


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


def run_aws_cp(source: str, target: str):
    if not Path(source).exists():
        logger.error(f"Couldn't upload to s3: '{source}' does not exist.")
        return False
    try:
        exponential_backoff_retry(
            lambda: subprocess.run(
                [
                    "aws",
                    "s3",
                    "cp",
                    source,
                    target,
                    "--acl",
                    "bucket-owner-full-control",
                ],
                timeout=AWS_CP_TIMEOUT,
                check=True,
            ),
            subprocess.SubprocessError,
            initial_retry_delay_s=10,
            max_retries=3,
        )
        return True
    except subprocess.SubprocessError:
        logger.exception("Couldn't upload to s3.")
        return False


def collect_metrics(time_taken: float) -> bool:
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
    try:
        subprocess.run(command, check=True, timeout=timeout)
        return_code = 0
    except subprocess.TimeoutExpired:
        return_code = -1
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
    sys.exit(return_code)


def run_bash_command(workload: str, timeout: float):
    timeout = timeout if timeout > 0 else None
    cwd = Path.cwd()
    workload_path = cwd / "workload.sh"
    workload_path = workload_path.absolute().resolve()
    with open(workload_path, "w") as fp:
        fp.write(workload)

    command = ["bash", "-x", str(workload_path)]
    logger.info(f"Running command {workload}")

    # We use multiprocessing with 'spawn' context to avoid
    # forking (as happens when using subprocess directly),
    # because that messes up Ray interactions and causes
    # deadlocks.
    try:
        ctx = multiprocessing.get_context("spawn")
        p = ctx.Process(target=_run_bash_command_subprocess, args=(command, timeout))
        p.start()
        # Add a little extra to the timeout as _run_bash_command_subprocess
        # also has a timeout internally and it's cleaner to use that
        p.join(timeout=timeout + 10)
        return_code = 0
    except multiprocessing.TimeoutError:
        return_code = -1
    except multiprocessing.ProcessError:
        return_code = p.exitcode
    finally:
        if p.is_alive():
            p.terminate()
        os.remove(str(workload_path))
    return return_code


def main(
    test_workload: str,
    test_workload_timeout: float,
    test_long_running: bool,
    results_s3_uri: str,
    metrics_s3_uri: str,
    prepare_commands: List[str],
    prepare_commands_timeouts: List[str],
):
    logger.info("### Starting ###")
    start_time = time.monotonic()

    prepare_return_codes = []
    if prepare_commands:
        logger.info("### Starting prepare commands ###")
        assert len(prepare_commands) == len(prepare_commands_timeouts)
        for prepare_command, timeout in zip(
            prepare_commands, prepare_commands_timeouts
        ):
            command_start_time = time.monotonic()
            prepare_return_codes.append(run_bash_command(prepare_command, timeout))
            prepare_time_taken = time.monotonic() - command_start_time
            return_code = prepare_return_codes[-1]
            if return_code != 0:
                timeout = return_code == -1
                if timeout:
                    logger.error(
                        "Prepare command timed out. "
                        f"Time taken: {prepare_time_taken}"
                    )
                else:
                    logger.info(
                        f"Prepare command finished with return code {return_code}. "
                        f"Time taken: {prepare_time_taken}"
                    )
                logger.error("Prepare command failed.")
                break

    if prepare_return_codes[-1] == 0:
        logger.info("### Starting entrypoint ###")
        command_start_time = time.monotonic()
        return_code = run_bash_command(test_workload, test_workload_timeout)
        workload_time_taken = time.monotonic() - command_start_time

        timeout = return_code == -1
        if timeout:
            msg = f"Timed out. Time taken: {workload_time_taken}"
            if test_long_running:
                logger.info(msg)
            else:
                logger.error(msg)
        else:
            logger.info(
                f"Finished with return code {return_code}. "
                f"Time taken: {workload_time_taken}"
            )

        # Install awscli for uploading to s3
        subprocess.run(["pip", "install", "-q", "awscli"], check=True)
        uploaded_results = run_aws_cp(os.environ["TEST_OUTPUT_JSON"], results_s3_uri)

        collected_metrics = collect_metrics(workload_time_taken)
        if collected_metrics:
            uploaded_metrics = run_aws_cp(
                os.environ["METRICS_OUTPUT_JSON"], metrics_s3_uri
            )
    else:
        return_code = None

    total_time_taken = time.monotonic() - start_time
    output_json = {
        "return_code": return_code,
        "prepare_return_codes": prepare_return_codes,
        "last_prepare_time_taken": prepare_time_taken,
        "workload_time_taken": workload_time_taken,
        "total_time_taken": total_time_taken,
        "uploaded_results": uploaded_results,
        "collected_metrics": collected_metrics,
        "uploaded_metrics": uploaded_metrics,
    }
    output_json = json.dumps(
        output_json, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )

    logger.info("### Finished ###")
    logger.info(f"### JSON |{output_json}| ###")
    if return_code == -1:
        if test_long_running:
            return_code = 0
        else:
            return_code = 124
    elif return_code is None:
        return_code = 1
    sys.exit(return_code)


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
        "--test-long-running",
        action="store_true",
        help="is test long running (don't fail on timeout)",
    )
    parser.add_argument(
        "--results-s3-uri", type=str, help="bucket address to upload results.json to"
    )
    parser.add_argument(
        "--metrics-s3-uri", type=str, help="bucket address to upload metrics.json to"
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
    main(**args.__dict__)
