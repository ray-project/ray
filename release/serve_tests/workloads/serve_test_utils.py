#!/usr/bin/env python3
import re


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
    parsed = re.split(r"(\d+.?\d+)(\w+)", size_string)
    values = [val for val in parsed if val]

    if values[1] == "KB":
        return float(values[0])
    elif values[1] == "MB":
        return float(values[0]) * 1024
    elif values[1] == "GB":
        return float(values[0]) * 1024 * 1024

    # Should not return here in common benchmark
    return values[1]


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

    parsed = re.split(r"(\d+.?\d+)(\w+)", metric_string)
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
            metrics_dict["latency_avg_ms"] = parse_time_to_ms(parsed[1])
            metrics_dict["latency_stdev_ms"] = parse_time_to_ms(parsed[2])
            metrics_dict["latency_max_ms"] = parse_time_to_ms(parsed[3])
            metrics_dict["latency_+/-_stdev %"] = float(parsed[4][:-1])
        elif parsed[0] == "Req/Sec" and len(parsed) == 5:
            metrics_dict["req/sec_avg"] = parse_metric_to_base(parsed[1])
            metrics_dict["req/sec_stdev"] = parse_metric_to_base(parsed[2])
            metrics_dict["req/sec_max"] = parse_metric_to_base(parsed[3])
            metrics_dict["req/sec_+/-_stdev %"] = float(parsed[4][:-1])
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
        # Summary section
        # Requests/sec:   1317.73
        # Transfer/sec:    198.19KB
        elif parsed[0] == "Requests/sec:":
            metrics_dict["requests/sec"] = parse_metric_to_base(parsed[1])
        elif parsed[0] == "Transfer/sec:":
            metrics_dict["transfer/sec_KB"] = parse_size_to_KB(parsed[1])

    return metrics_dict
