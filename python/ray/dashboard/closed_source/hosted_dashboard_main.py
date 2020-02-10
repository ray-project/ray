import argparse
import re
import traceback
import os

import redis

import ray
import ray.ray_constants as ray_constants
from ray.dashboard.dashboard import Dashboard
from ray.dashboard.closed_source.hosted_dashboard_controller import HostedDashboardController

"""Run hosted dashboard server on Anyscale hosts.

This is a standalone main script that is independent from
Ray clusters. It runs a hosted dashboard server that
reads data from Anyscale DBs instead of Raylet and GCS. Note
that it should be closed sourced and only ran in Anyscale clusters.

As described above, hosted dashboard server should run in the same way
as an open source dashboard, but how it reads data is different.
To achieve this, you should inject a HostedDashboardController class that has
the same interface as an open source DashboardController. Refer to
`Hosted Dashboard Architecture Design` and `Hosted Dashboard Implementation Plan`
in a shared google doc (under a design doc section) to understand the
architecture and implementation in details.

To run a hosted dashboard in the local environment, follow the below.
1. Make sure Redis server is running in `--redis-address`. 
2. Ingest server should be running. 
```
python ingest_server.py
```
TODO(sang): Change this part of the instruction. This is a temporary way
    to simulate the whole workflow.
3. Run any Ray driver locally with an argument `is_hosted_dashboard=True`.
    Note that the argument should be explictly set for now.
4. Finally, run the hosted_dashboard_main.py
```
python hosted_dashboard_main.py 
    --host 127.0.0.1
    --port 8266
    --redis-address 127.0.0.1:6379
    --temp-dir [Any temp dir of your own]
```
5. You can see the hosted dashboard is working in the address
[--host]:[--port]
"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "dashboard to connect to."))
    parser.add_argument(
        "--host",
        required=True,
        type=str,
        help="The host to use for the HTTP server.")
    parser.add_argument(
        "--port",
        required=True,
        type=int,
        help="The port to use for the HTTP server.")
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="The address to use for Redis.")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="the password to use for Redis")
    parser.add_argument(
        "--logging-level",
        required=False,
        type=str,
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP)
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP)
    parser.add_argument(
        "--temp-dir",
        required=False,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.")
    args = parser.parse_args()
    ray.utils.setup_logger(args.logging_level, args.logging_format)

    redis_address, redis_port = args.redis_address.strip().split(':')
    redis_client = redis.StrictRedis(host=redis_address, port=redis_port)
    web_ui_url = "{}:{}".format(args.host, args.port)
    # Should record the webui address to Redis before running a dashboard
    # so that the dashboard class can find the proper addresses.
    redis_client.hmset("webui", {"url": web_ui_url})

    hosted_dashboard_client = False

    try:
        dashboard = Dashboard(
            args.host,
            args.port,
            args.redis_address,
            args.temp_dir,
            redis_password=args.redis_password,
            hosted_dashboard_client=hosted_dashboard_client,
            DashboardController=HostedDashboardController
        )
        dashboard.run()
    except Exception as e:
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The dashboard on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        raise e