import argparse
import asyncio
import os

from aiohttp import ClientSession, UnixConnector

from ray.anyscale._private.constants import ANYSCALE_DATAPLANE_SERVICE_SOCKET

# Example: [
#     '/home/ubuntu/ray/python/ray/_private/workers/default_worker.py',
#     '--node-ip-address=10.103.98.7',
#     '--node-manager-port=53621',
#     '--object-store-name=/tmp/ray/session_2024-05-21_10-56-47_612166_12494/sockets/plasma_store',
#     '--raylet-name=/tmp/ray/session_2024-05-21_10-56-47_612166_12494/sockets/raylet',
#     '--redis-address=None',
#     '--metrics-agent-port=64483',
#     '--runtime-env-agent-port=54987',
#     '--logging-rotate-bytes=536870912',
#     '--logging-rotate-backup-count=5',
#     '--runtime-env-agent-port=54987',
#     '--gcs-address=10.103.98.7:62996',
#     '--session-name=session_2024-05-21_10-56-47_612166_12494',
#     '--temp-dir=/tmp/ray',
#     '--webui=127.0.0.1:8265',
#     '--cluster-id=6a3aac2a011f5c4d2447848e4b6918587c25bf07a64a74814eb9f420',
#     '--startup-token=10',
#     '--worker-launch-time-ms=1716314209990',
#     '--node-id=aa620f8dda939a97be9ce031b7794ac94512972c9cea407508818024',
#     '--runtime-env-hash=815095513'
# ]


async def main(working_dir: str):
    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-worker-image-uri")
    parser.add_argument("--env-var-names", nargs="*", type=str, default=[])
    known_args, passthrough_args = parser.parse_known_args()

    # Pass environment variables
    env_vars = {
        name: value
        for name, value in os.environ.items()
        if name.startswith("RAY_")
        or name.startswith("ANYSCALE_")
        or name == "PYTHONPATH"
        or name in known_args.env_var_names
    }
    data = {
        "image_uri": known_args.ray_worker_image_uri,
        "args": passthrough_args[1:],  # Pop default_worker.py
        "envs": env_vars,
        "working_dir": working_dir
    }
    conn = UnixConnector(path=ANYSCALE_DATAPLANE_SERVICE_SOCKET)
    async with ClientSession(connector=conn) as session:
        async with session.post("http://unix/execute_ray_worker", json=data) as resp:
            resp.raise_for_status()


if __name__ == "__main__":
    working_dir = os.getcwd()
    asyncio.run(main(working_dir))
