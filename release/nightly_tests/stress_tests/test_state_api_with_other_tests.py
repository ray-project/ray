import time
from typing import Dict, List
import click
import json
import os

import ray

from ray.experimental.state.api import (
    list_actors,
    list_nodes,
    list_objects,
    list_tasks,
    summarize_actors,
    summarize_objects,
    summarize_tasks,
)

import ray._private.test_utils as test_utils

from ray._private.state_api_test_utils import (
    StateAPICallSpec,
    periodic_invoke_state_apis_with_actor,
    STATE_LIST_LIMIT,
)


def download_release_test(test_file_path: str) -> None:
    """Download the release test file from github.

    It is currently assumed individual release test is independent from each other,
    and isolated in its own file.

    This always downloads the file into current working directory so that this
    python script could invoke the release test w/o path imports.

    Args:
        test_file_path: File path relevant to the `/release` folder.

    Return:
        Basename (file name) of the test file path if download successfully.
    """
    import urllib.request as rq
    import urllib.parse as parse

    RAW_RAY_GITHUB_URL = (
        "https://raw.githubusercontent.com/ray-project/ray/master/release/"
    )
    file_name = os.path.basename(test_file_path)
    try:
        rq.urlretrieve(parse.urljoin(RAW_RAY_GITHUB_URL, test_file_path), file_name)
        return file_name
    except Exception as e:
        print(f"Failed to retrieve :{test_file_path} :\n{e}")
        return None


def cleanup_release_test(test_file_name: str) -> bool:
    try:
        os.remove(test_file_name)
        return True
    except Exception as e:
        print(f"Failed to remove file: {test_file_name}: \n{e}")
        return False


def run_release_test_in_subprocess(test_file: str, args: List[str]) -> bool:
    import subprocess as sp

    # Run the test in subprocess
    cmds = ["python", test_file, *args]

    print(f"Running: {' '.join(cmds)}")
    proc = None
    try:
        proc = sp.run(cmds, check=True, text=True, capture_output=True)
        proc.check_returncode()
        return True
    except sp.CalledProcessError as e:
        print(f"Failed to run :{' '.join(cmds)}")
        print(e)
        print(e.stdout)
        print(e.stderr)
        return False


def run_test(test_name: str, test_args: List[str]):

    monitor_actor = test_utils.monitor_memory_usage()

    start = time.perf_counter()
    run_release_test_in_subprocess(test_name, test_args)
    end = time.perf_counter()
    # Collect mem usage
    ray.get(monitor_actor.stop_run.remote())
    used_gb, usage = ray.get(monitor_actor.get_peak_memory_info.remote())

    results = {
        "duration": end - start,
        "peak_memory": round(used_gb, 2),
        "peak_process_memory": usage,
    }
    return results


def run_test_with_state_api(
    test_name: str,
    test_args: List[str],
    apis: List[StateAPICallSpec],
    call_interval_s: int = 3,
    print_interval_s: int = 15,
) -> Dict:

    start_time = time.perf_counter()

    # Stage 1: Run with state APIs
    api_caller = periodic_invoke_state_apis_with_actor(
        apis=apis, call_interval_s=call_interval_s, print_interval_s=print_interval_s
    )

    stats_with_state_apis = run_test(test_name, test_args)
    ray.get(api_caller.stop.remote())
    print(json.dumps(ray.get(api_caller.get_stats.remote()), indent=2))

    # Stage 2: Run without API generator
    stats_without_state_apis = run_test(test_name, test_args)
    end_time = time.perf_counter()

    # Dumping results
    results = {
        "time": end_time - start_time,
        "success": "1",
        "perf_metrics": [
            {
                "perf_metric_name": "state_api_extra_latency_sec",
                "perf_metric_value": stats_with_state_apis["duration"]
                - stats_without_state_apis["duration"],
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "state_api_extra_latency_sec_percentage",
                "perf_metric_value": (
                    stats_with_state_apis["duration"]
                    / stats_without_state_apis["duration"]
                    - 1
                )
                * 100,
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "state_api_extra_mem",
                "perf_metric_value": stats_with_state_apis["peak_memory"]
                - stats_without_state_apis["peak_memory"],
                "perf_metric_type": "MEMORY",
            },
        ],
    }

    return results


@click.command()
@click.argument(
    "test_path",
)
@click.option(
    "--test-args",
    type=str,
)
@click.option(
    "--call-interval-s", type=int, default=3, help="interval of state api calls"
)
def test(
    test_path,
    test_args,
    call_interval_s,
):

    # Set up state API calling methods
    def not_none(res):
        return res is not None

    apis = [
        StateAPICallSpec(list_nodes, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(list_objects, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(list_tasks, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(list_actors, not_none, {"limit": STATE_LIST_LIMIT}),
        StateAPICallSpec(summarize_tasks, not_none),
        StateAPICallSpec(summarize_actors, not_none),
        StateAPICallSpec(summarize_objects, not_none),
    ]

    # Set up benchmark test by downloading the release test file directly
    test_name = download_release_test(test_path)
    assert test_name is not None, f"Failed to retrieve release test: {test_path}"

    ray.init()
    results = run_test_with_state_api(
        test_name,
        test_args.split(),
        apis,
        call_interval_s=call_interval_s,
    )

    if "TEST_OUTPUT_JSON" in os.environ:
        # This will overwrite all other release tests result
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        json.dump(results, out_file)
    print(json.dumps(results, indent=2))

    assert cleanup_release_test(test_name)


if __name__ == "__main__":
    test()
