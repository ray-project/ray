#!/usr/bin/env python3
import re


def parse_wrk_decoded_stdout(decoded_out):
    """
    Parse decoded wrk stdout to a dictionary.

    # Sample wrk stdout:
    #
    # Running 10s test @ http://127.0.0.1:8000/echo
    # 2 threads and 84 connections
    # Thread Stats   Avg      Stdev     Max   +/- Stdev
    #     Latency    59.33ms   13.51ms 113.83ms   64.20%
    #     Req/Sec   709.16     61.73   848.00     78.50%
    # 14133 requests in 10.02s, 2.08MB read
    # Requests/sec:   1410.71
    # Transfer/sec:    212.16KB

    Returns:
        metrics_dict (Dict[str, str]):
            {'latency_avg': '66.57ms', 'latency_stdev': '4.47ms',
             'latency_max': '113.34ms', 'latency_+/-_stdev': '91.33%',
             'req/sec_avg': '180.88', 'req/sec_stdev': '26.83',
             'req/sec_max': '323.00', 'req/sec_+/-_stdev': '65.22%',
             'requests/sec': '1440.06', 'transfer/sec': '216.57KB'}
    """
    metrics_dict = {}
    for line in decoded_out.splitlines():
        parsed = re.split(r"\s+", line.strip())
        if parsed[0] == "Latency":
            metrics_dict["latency_avg"] = parsed[1]
            metrics_dict["latency_stdev"] = parsed[2]
            metrics_dict["latency_max"] = parsed[3]
            metrics_dict["latency_+/-_stdev"] = parsed[4]
        elif parsed[0] == "Req/Sec":
            metrics_dict["req/sec_avg"] = parsed[1]
            metrics_dict["req/sec_stdev"] = parsed[2]
            metrics_dict["req/sec_max"] = parsed[3]
            metrics_dict["req/sec_+/-_stdev"] = parsed[4]
        elif parsed[0] == "Requests/sec:":
            metrics_dict["requests/sec"] = parsed[1]
        elif parsed[0] == "Transfer/sec:":
            metrics_dict["transfer/sec"] = parsed[1]

    return metrics_dict
