#!/usr/bin/env python3
import json
import logging
import os
import random
import ray
import re
import subprocess
from collections import defaultdict

from serve_test_cluster_utils import NUM_CPU_PER_NODE
from subprocess import PIPE
from typing import Dict, List, Union

logger = logging.getLogger(__file__)


def is_smoke_test():
    return os.environ.get("IS_SMOKE_TEST", "0") == "1"


def parse_time_to_ms(time_string: str) -> float:
    """Given a time string with various unit, convert
    to ms in float:

    wrk time unit reference
    https://github.com/wg/wrk/blob/master/src/units.c#L17-L21

        Example:
            "71.91ms" -> 71.91
            "50us" -> 0.05
            "1.5s" -> 1500
    """
    # Group 1 - (one or more digits + optional dot + one or more digits)
    # 71.91 / 50 / 1.5
    # Group 2 - (All words)
    # ms / us / s
    parsed = re.split(r"(\d+.?\d+)(\w+)", time_string)
    values = [val for val in parsed if val]

    if values[1] == "ms":
        return float(values[0])
    elif values[1] == "us":
        return float(values[0]) / 1000
    elif values[1] == "s":
        return float(values[0]) * 1000

    # Should not return here in common benchmark
    return values[1]


def parse_size_to_KB(size_string: str) -> float:
    """Given a size string with various unit, convert
    to KB in float:

    wrk binary unit reference
    https://github.com/wg/wrk/blob/master/src/units.c#L29-L33

        Example:
            "200.56KB" -> 200.56
            "50MB" -> 51200
            "0.5GB" -> 524288
    """
    # Group 1 - (one or more digits + optional dot + one or more digits)
    # 200.56 / 50 / 0.5
    # Group 2 - (All words)
    # KB / MB / GB
    parsed = re.split(r"(\d+.?\d+)(\w*)", size_string)
    values = [val for val in parsed if val]

    if values[1] == "KB":
        return float(values[0])
    elif values[1] == "MB":
        return float(values[0]) * 1024
    elif values[1] == "GB":
        return float(values[0]) * 1024 * 1024

    # Bytes
    return float(values[0]) / 1000


def parse_metric_to_base(metric_string: str) -> float:
    """Given a metric string with various unit, convert
    to original base

    wrk metric unit reference
    https://github.com/wg/wrk/blob/master/src/units.c#L35-L39

        Example:
            "71.91" -> 71.91
            "1.32k" -> 1320
            "1.5M" -> 1500000
    """

    parsed = re.split(r"(\d+.?\d+)(\w*)", metric_string)
    values = [val for val in parsed if val]

    if len(values) == 1:
        return float(values[0])
    if values[1] == "k":
        return float(values[0]) * 1000
    elif values[1] == "M":
        return float(values[0]) * 1000 * 1000

    # Should not return here in common benchmark
    return values[1]


def parse_wrk_decoded_stdout(decoded_out):
    """
    Parse decoded wrk stdout to a dictionary.

    # Sample wrk stdout:
    #
    Running 10s test @ http://127.0.0.1:8000/echo
    8 threads and 96 connections
    Thread Stats   Avg      Stdev     Max   +/- Stdev
        Latency    72.32ms    6.00ms 139.00ms   91.60%
        Req/Sec   165.99     34.84   242.00     57.20%
    Latency Distribution
        50%   70.78ms
        75%   72.59ms
        90%   75.67ms
        99%   98.71ms
    13306 requests in 10.10s, 1.95MB read
    Requests/sec:   1317.73
    Transfer/sec:    198.19KB

    Returns:
        {'latency_avg_ms': 72.32, 'latency_stdev_ms': 6.0,
         'latency_max_ms': 139.0, 'latency_+/-_stdev %': 91.6,
        'req/sec_avg': 165.99, 'req/sec_stdev': 34.84,
        'req/sec_max': 242.0, 'req/sec_+/-_stdev %': 57.2,
        'P50_latency_ms': 70.78, 'P75_latency_ms': 72.59,
        'P90_latency_ms': 75.67, 'P99_latency_ms': 98.71,
        'requests/sec': 1317.73, 'transfer/sec_KB': 198.19
    """
    metrics_dict = {}
    for line in decoded_out.splitlines():
        parsed = re.split(r"\s+", line.strip())
        # Statistics section
        # Thread Stats   Avg      Stdev     Max   +/- Stdev
        #   Latency    72.32ms    6.00ms 139.00ms   91.60%
        #   Req/Sec   165.99     34.84   242.00     57.20%
        if parsed[0] == "Latency" and len(parsed) == 5:
            metrics_dict["per_thread_latency_avg_ms"] = parse_time_to_ms(parsed[1])
            metrics_dict["per_thread_latency_max_ms"] = parse_time_to_ms(parsed[3])
        elif parsed[0] == "Req/Sec" and len(parsed) == 5:
            metrics_dict["per_thread_tps"] = parse_metric_to_base(parsed[1])
            metrics_dict["per_thread_max_tps"] = parse_metric_to_base(parsed[3])
        # Latency Distribution header, ignored
        elif parsed[0] == "Latency" and parsed[1] == "Distribution":
            continue
        # Percentile section
        # 50%   70.78ms
        # 75%   72.59ms
        # 90%   75.67ms
        # 99%   98.71ms
        elif parsed[0] == "50%":
            metrics_dict["P50_latency_ms"] = parse_time_to_ms(parsed[1])
        elif parsed[0] == "75%":
            metrics_dict["P75_latency_ms"] = parse_time_to_ms(parsed[1])
        elif parsed[0] == "90%":
            metrics_dict["P90_latency_ms"] = parse_time_to_ms(parsed[1])
        elif parsed[0] == "99%":
            metrics_dict["P99_latency_ms"] = parse_time_to_ms(parsed[1])
        # Total requests and transfer (might have timeout too)
        # 13306 requests in 10.10s, 1.95MB read
        elif len(parsed) >= 6 and parsed[1] == "requests":
            metrics_dict["per_node_total_thoughput"] = int(parsed[0])
            metrics_dict["per_node_total_transfer_KB"] = parse_size_to_KB(parsed[4])
        # Socket errors: connect 0, read 0, write 0, timeout 100
        elif parsed[0] == "Socket" and parsed[1] == "errors:":
            metrics_dict["per_node_total_timeout_requests"] = parse_metric_to_base(
                parsed[-1]
            )
        # Summary section
        # Requests/sec:   1317.73
        # Transfer/sec:    198.19KB
        elif parsed[0] == "Requests/sec:":
            metrics_dict["per_nodel_tps"] = parse_metric_to_base(parsed[1])
        elif parsed[0] == "Transfer/sec:":
            metrics_dict["per_node_transfer_per_sec_KB"] = parse_size_to_KB(parsed[1])

    return metrics_dict


@ray.remote
def run_one_wrk_trial(
    trial_length: str,
    num_connections: int,
    http_host: str,
    http_port: str,
    endpoint: str = "",
) -> None:
    proc = subprocess.Popen(
        [
            "wrk",
            "-c",
            str(num_connections),
            "-t",
            str(NUM_CPU_PER_NODE),
            "-d",
            trial_length,
            "--latency",
            f"http://{http_host}:{http_port}/{endpoint}",
        ],
        stdout=PIPE,
        stderr=PIPE,
    )
    proc.wait()
    out, err = proc.communicate()

    if err.decode() != "":
        logger.error(err.decode())

    return out.decode()


def aggregate_all_metrics(metrics_from_all_nodes: Dict[str, List[Union[float, int]]]):
    num_nodes = len(metrics_from_all_nodes["per_nodel_tps"])
    return {
        # Per thread metrics
        "per_thread_latency_avg_ms": round(
            sum(metrics_from_all_nodes["per_thread_latency_avg_ms"]) / num_nodes, 2
        ),
        "per_thread_latency_max_ms": max(
            metrics_from_all_nodes["per_thread_latency_max_ms"]
        ),
        "per_thread_avg_tps": round(
            sum(metrics_from_all_nodes["per_thread_tps"]) / num_nodes, 2
        ),
        "per_thread_max_tps": max(metrics_from_all_nodes["per_thread_max_tps"]),
        # Per wrk node metrics
        "per_node_avg_tps": round(
            sum(metrics_from_all_nodes["per_nodel_tps"]) / num_nodes, 2
        ),
        "per_node_avg_transfer_per_sec_KB": round(
            sum(metrics_from_all_nodes["per_node_transfer_per_sec_KB"]) / num_nodes, 2
        ),
        # Cluster metrics
        "cluster_total_thoughput": sum(
            metrics_from_all_nodes["per_node_total_thoughput"]
        ),
        "cluster_total_transfer_KB": sum(
            metrics_from_all_nodes["per_node_total_transfer_KB"]
        ),
        "cluster_total_timeout_requests": sum(
            metrics_from_all_nodes["per_node_total_timeout_requests"]
        ),
        "cluster_max_P50_latency_ms": max(metrics_from_all_nodes["P50_latency_ms"]),
        "cluster_max_P75_latency_ms": max(metrics_from_all_nodes["P75_latency_ms"]),
        "cluster_max_P90_latency_ms": max(metrics_from_all_nodes["P90_latency_ms"]),
        "cluster_max_P99_latency_ms": max(metrics_from_all_nodes["P99_latency_ms"]),
    }


def run_wrk_on_all_nodes(
    trial_length: str,
    num_connections: int,
    http_host: str,
    http_port: str,
    all_endpoints: List[str] = None,
    ignore_output: bool = False,
    debug: bool = False,
):
    """
    Use ray task to run one wrk trial on each node alive, picked randomly
    from all available deployments.

    Returns:
        all_metrics: (Dict[str, List[Union[float, int]]]) Parsed wrk metrics
            from each wrk on each running node
        all_wrk_stdout: (List[str]) decoded stdout of each wrk trial for per
            node checks at the end of experiment
    """
    all_metrics = defaultdict(list)
    all_wrk_stdout = []
    rst_ray_refs = []
    for node in ray.nodes():
        if node["Alive"]:
            node_resource = f"node:{node['NodeManagerAddress']}"
            # Randomly pick one from all available endpoints in ray cluster
            endpoint = random.choice(all_endpoints)
            rst_ray_refs.append(
                run_one_wrk_trial.options(
                    num_cpus=0, resources={node_resource: 0.01}
                ).remote(trial_length, num_connections, http_host, http_port, endpoint)
            )

    print("Waiting for wrk trials to finish...")
    ray.wait(rst_ray_refs, num_returns=len(rst_ray_refs))
    print("Trials finished!")

    if ignore_output:
        return

    for i, decoded_output in enumerate(ray.get(rst_ray_refs)):
        if debug:
            print(f"decoded_output {i}: {decoded_output}")
        all_wrk_stdout.append(decoded_output)
        parsed_metrics = parse_wrk_decoded_stdout(decoded_output)

        # Per thread metrics
        all_metrics["per_thread_latency_avg_ms"].append(
            parsed_metrics["per_thread_latency_avg_ms"]
        )
        all_metrics["per_thread_latency_max_ms"].append(
            parsed_metrics["per_thread_latency_max_ms"]
        )
        all_metrics["per_thread_tps"].append(parsed_metrics["per_thread_tps"])
        all_metrics["per_thread_max_tps"].append(parsed_metrics["per_thread_max_tps"])

        # Per node metrics
        all_metrics["P50_latency_ms"].append(parsed_metrics["P50_latency_ms"])
        all_metrics["P75_latency_ms"].append(parsed_metrics["P75_latency_ms"])
        all_metrics["P90_latency_ms"].append(parsed_metrics["P90_latency_ms"])
        all_metrics["P99_latency_ms"].append(parsed_metrics["P99_latency_ms"])

        all_metrics["per_node_total_thoughput"].append(
            parsed_metrics["per_node_total_thoughput"]
        )
        all_metrics["per_node_total_transfer_KB"].append(
            parsed_metrics["per_node_total_transfer_KB"]
        )

        all_metrics["per_nodel_tps"].append(parsed_metrics["per_nodel_tps"])
        all_metrics["per_node_transfer_per_sec_KB"].append(
            parsed_metrics["per_node_transfer_per_sec_KB"]
        )
        all_metrics["per_node_total_timeout_requests"].append(
            parsed_metrics.get("per_node_total_timeout_requests", 0)
        )

    return all_metrics, all_wrk_stdout


def save_test_results(final_result, default_output_file="/tmp/release_test_out.json"):
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", default_output_file)
    with open(test_output_json, "wt") as f:
        json.dump(final_result, f)
