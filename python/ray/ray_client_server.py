"""Ray client server process.

This is a background process that connects to the Ray cluster and
serves as a driver that Ray client requests are proxied through.
"""

import argparse
import time

import ray
from ray.util.client.server.server import serve

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Arguments for Ray client server process"))
    parser.add_argument(
        "--address",
        required=True,
        type=str,
        help="Address to use to connect to Ray")
    parser.add_argument(
        "--redis-password",
        required=True,
        type=str,
        help="Password for connecting to Redis")
    parser.add_argument(
        "--ray-client-server-port",
        required=True,
        type=str,
        help="Address the Ray client server process will listen on")
    args = parser.parse_args()

    ray.init(address=args.address, _redis_password=args.redis_password)
    server = serve("0.0.0.0:{}".format(args.ray_client_server_port))

    try:
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        server.stop(0)
