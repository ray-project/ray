import aiohttp
import asyncio
import ray
from typing import Any
from ray.job_submission import JobSubmissionClient
import ray.cloudpickle as cloudpickle
import os

# TODO: change cloudpickle.dumps to pickle_dumps


class ClientSession:
    def __init__(self, server_addr: str) -> None:
        self.server_addr = server_addr
        self.old_ray_get = None
        self.old_ray_put = None
        self.old_ray_remotefunction_remote = None

    def __enter__(self):
        # object_refs, timeout
        self.old_ray_get = ray.get
        self.old_ray_put = ray.put
        self.old_ray_remotefunction_remote = ray.remote_function.RemoteFunction._remote
        ray.get = self.ray_get
        ray.put = self.ray_put

        def remote(func, args, kwargs, **task_options):
            return self.ray_remotefunction_remote(func, args, kwargs, **task_options)

        ray.remote_function.RemoteFunction._remote = remote

    def __exit__(self, exc_type, exc_val, exc_tb):
        # TODO: exc handling
        ray.get = self.old_ray_get
        ray.put = self.old_ray_put
        ray.remote_function.RemoteFunction._remote = self.old_ray_remotefunction_remote
        self.old_ray_get = None
        self.old_ray_put = None
        self.old_ray_remotefunction_remote = None

    async def ray_get_async(self, object_refs, timeout=None):
        # TODO: long connection
        async with aiohttp.ClientSession(read_timeout=timeout) as session:
            # TODO: support more than 1 object ref
            async with session.post(
                f"{self.server_addr}/get", data=object_refs.binary()
            ) as resp:
                # TODO: error handling
                data = await resp.read()
                obj = cloudpickle.loads(data)
                return obj

    def ray_get(self, object_refs, timeout=None):
        # TODO: support those overloads
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.ray_get_async(object_refs, timeout))

    async def ray_put_async(self, value: Any):
        # TODO: long connection
        async with aiohttp.ClientSession(read_timeout=None) as session:
            async with session.post(
                f"{self.server_addr}/put", data=cloudpickle.dumps(value)
            ) as resp:
                # TODO: error handling
                ref_data = await resp.read()
                return ray.ObjectRef(ref_data)

    def ray_put(self, value):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.ray_put_async(value))

    async def ray_remotefunction_remote_async(self, func, args, kwargs, **task_options):
        data = cloudpickle.dumps((func._function, args, kwargs, task_options))
        async with aiohttp.ClientSession(read_timeout=None) as session:
            async with session.post(
                f"{self.server_addr}/remotefunction/remote", data=data
            ) as resp:
                # TODO: error handling
                ref_data = await resp.read()
                print(ref_data)
                # TODO: support more than 1 refs
                return ray.ObjectRef(ref_data)

    def ray_remotefunction_remote(self, func, args, kwargs, **task_options):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            self.ray_remotefunction_remote_async(func, args, kwargs, **task_options)
        )


def start_session(cluster_address: str, name: str) -> ClientSession:
    job_client = JobSubmissionClient(cluster_address)
    # TODO: pass the name.
    # TODO: first list jobs. if the job by name already exists, return the http address in metadata.
    job_client.submit_job(
        entrypoint="python -m ray.util.client2.driver", runtime_env={"pip": ["aiohttp"]}
    )
    import time

    # wait for actor and http server to be on.
    # TODO: instead, wait for notification
    time.sleep(5)
    # TODO: somehow let the driver to pass back the http address
    return ClientSession("http://127.0.0.1:25001")
