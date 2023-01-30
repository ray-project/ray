import argparse
import time
import os
from pathlib import Path
import subprocess
import traceback
import json
import sys

AWS_CP_TIMEOUT = 300


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
            print(
                f"Retry function call failed due to {e} "
                f"in {retry_delay_s} seconds..."
            )
            time.sleep(retry_delay_s)
            retry_delay_s *= 2


def run_aws_cp(source: str, target: str):
    try:
        exponential_backoff_retry(
            lambda: subprocess.check_call(
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
                stdout=subprocess.PIPE,
            ),
            subprocess.SubprocessError,
            initial_retry_delay_s=10,
            max_retries=3,
        )
        return True
    except subprocess.SubprocessError:
        print("Couldn't upload to s3.")
        traceback.print_exc()
        return False


def collect_metrics(time_taken: float) -> bool:
    # Timeout is the time the test took divided by 200
    # (~7 minutes for a 24h test) but no less than 90s
    # and no more than 900s
    metrics_timeout = max(90, min((time.time() - time_taken) / 200, 900))
    try:
        subprocess.check_call(
            [
                "python",
                "prometheus_metrics.py",
                str(time_taken),
                "--path",
                os.environ["METRICS_OUTPUT_JSON"],
            ],
            timeout=metrics_timeout,
        )
        return True
    except subprocess.SubprocessError:
        print("Couldn't collect metrics.")
        traceback.print_exc()
        return False


def main(
    test_workload: str,
    test_workload_timeout: float,
    test_long_running: bool,
    results_s3_uri: str,
    metrics_s3_uri: str,
):
    print("### Starting entrypoint ###")
    test_workload_timeout = test_workload_timeout if test_workload_timeout > 0 else None
    start_time = time.monotonic()
    cwd = Path.cwd()
    workload_path = cwd / "workload.sh"
    with open(workload_path, "w") as fp:
        fp.write(test_workload)
    os.chmod(str(workload_path), 777)

    command = ["bash", "-x", str(workload_path.absolute().resolve())]
    print(f"Running command {command}")

    try:
        subprocess.check_call(
            command, timeout=test_workload_timeout, stdout=subprocess.PIPE
        )
        return_code = 0
    except subprocess.CalledProcessError as e:
        return_code = e.returncode
    except subprocess.TimeoutExpired:
        return_code = -1

    timeout = return_code == -1
    workload_time_taken = time.monotonic() - start_time
    if timeout:
        print(f"Timed out. Time taken: {workload_time_taken}")
    else:
        print(
            f"Finished with return code {return_code}. "
            f"Time taken: {workload_time_taken}"
        )

    subprocess.check_call(["pip", "install", "-q", "awscli"])
    uploaded_results = run_aws_cp(os.environ["TEST_OUTPUT_JSON"], results_s3_uri)

    collected_metrics = collect_metrics(workload_time_taken)
    if collect_metrics:
        uploaded_metrics = run_aws_cp(os.environ["METRICS_OUTPUT_JSON"], metrics_s3_uri)

    total_time_taken = time.monotonic() - start_time
    output_json = {
        "return_code": return_code,
        "workload_time_taken": workload_time_taken,
        "total_time_taken": total_time_taken,
        "uploaded_results": uploaded_results,
        "collected_metrics": collected_metrics,
        "uploaded_metrics": uploaded_metrics,
    }
    output_json = json.dumps(
        output_json, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )

    print("### Finished ###")
    print(f"### JSON |{output_json}| ###")
    if return_code == -1:
        if test_long_running:
            return_code = 0
        else:
            return_code = 124
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

    args = parser.parse_args()
    main(**args.__dict__)
