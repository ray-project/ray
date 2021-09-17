from python.ray.util.client.server.server import create_ray_handler
import uvicorn
from fastapi import FastAPI

import logging
from concurrent import futures
import grpc
import base64
from collections import defaultdict
import queue
import pickle

import threading
from typing import Dict
from typing import Set
from typing import Optional
from typing import Callable
from ray import cloudpickle
from ray.job_config import JobConfig
import ray
import ray.state
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
import time
import inspect
import json
from ray.util.client.common import (ClientServerHandle, GRPC_OPTIONS,
                                    CLIENT_SERVER_MAX_THREADS)
from ray.util.client.server.proxier import serve_proxier
from ray.util.client.server.server_pickler import convert_from_arg
from ray.util.client.server.server_pickler import dumps_from_server
from ray.util.client.server.server_pickler import loads_from_client
from ray.util.client.server.dataservicer import DataServicer
from ray.util.client.server.logservicer import LogstreamServicer
from ray.util.client.server.server_stubs import current_server
from ray.ray_constants import env_integer
from ray.util.placement_group import PlacementGroup
from ray._private.client_mode_hook import disable_client_hook

logger = logging.getLogger(__name__)

TIMEOUT_FOR_SPECIFIC_SERVER_S = env_integer("TIMEOUT_FOR_SPECIFIC_SERVER_S",
                                            30)
app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host IP to bind to")
    parser.add_argument(
        "-p", "--port", type=int, default=10011, help="Port to bind to")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["proxy", "legacy", "specific-server"],
        default="proxy")
    parser.add_argument(
        "--redis-address",
        required=False,
        type=str,
        help="Address to use to connect to Ray")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        help="Password for connecting to Redis")
    parser.add_argument(
        "--worker-shim-pid",
        required=False,
        type=int,
        default=0,
        help="The PID of the process for setup worker runtime env.")
    parser.add_argument(
        "--metrics-agent-port",
        required=False,
        type=int,
        default=0,
        help="The port to use for connecting to the runtime_env agent.")
    args, _ = parser.parse_known_args()
    logging.basicConfig(level="INFO")

    ray_connect_handler = create_ray_handler(args.redis_address,
                                             args.redis_password)

    ray_connect_handler()
    uvicorn.run(app, port=args.port)


if __name__ == "__main__":
    main()

