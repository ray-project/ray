import aiohttp
import asyncio
import ray
from typing import Any, Optional
from ray.job_submission import JobSubmissionClient
from ray.util.client2.pickler import (
    ServerToClientUnpickler,
    ClientToServerPickler,
    dumps_with_pickler_cls,
    loads_with_unpickler_cls,
)
from ray.runtime_env import RuntimeEnv
from ray._private.utils import get_or_create_event_loop

import logging

logger = logging.getLogger(__name__)


class ClientSession:
    def __init__(
        self,
        server_addr: str,
        session_name: str,
        runtime_env: Optional[RuntimeEnv] = None,
        detached: bool = True,
    ) -> None:
        """
        detached: if the session closed, do we want the driver (and all running tasks) to continue. If True, the driver will keep on working infinitely, until you kill it. If False, the driver is killed when the code leaves the `with` clause.
        """
        self.server_addr = server_addr
        self.session_name = session_name
        self.actor_name = actor_name_for_session_name(self.session_name)
        self.runtime_env = runtime_env
        self.detached = detached

        self.loop = get_or_create_event_loop()
        self.aiohttp_client_session = aiohttp.ClientSession(read_timeout=None)

        self.old_ray_get = None
        self.old_ray_put = None
        self.old_ray_remotefunction_remote = None

        self.dumps = dumps_with_pickler_cls(ClientToServerPickler)
        self.loads = loads_with_unpickler_cls(ServerToClientUnpickler)

    async def connect(self):
        if await self.is_connected():
            return True

        job_client = JobSubmissionClient(self.server_addr)

        job_client.submit_job(
            entrypoint=f"/opt/homebrew/bin/python3.9 -m ray.util.client2.driver {self.actor_name}",
            runtime_env=self.runtime_env,
        )

        # Try to connect several times to wait for the ClientSupervisor start up.
        for i in range(10):
            if await self.is_connected():
                return True
            await asyncio.sleep(1)
        return False

    async def is_connected(self) -> bool:
        async with self.aiohttp_client_session.get(
            f"{self.server_addr}/api/clients/{self.actor_name}"
        ) as resp:
            if resp.status == 200:
                return True
            if resp.status == 404:
                logger.info(
                    "client2 session not connected, maybe the ClientSupervisor is still starting..."
                )
                return False
            logger.warning(
                f"client2 session connection error {resp.status}: {await resp.text()}"
            )
            return False

    # TODO: don't use self as async context manager
    async def aenter(self):
        await self.aiohttp_client_session.__aenter__()
        if not await self.connect():
            raise ValueError("can't connect...")

        self.old_ray_get = ray.get
        self.old_ray_put = ray.put
        self.old_ray_remotefunction_remote = ray.remote_function.RemoteFunction._remote
        self.old_ray_actorclass_remote = ray.actor.ActorClass._remote
        self.old_ray_actormethod_remote = ray.actor.ActorMethod._remote

        ray.get = self.warning_dont_use("ray.get(obj_ref)", "client.get(obj_ref)")
        ray.put = self.warning_dont_use("ray.put(obj)", "client.put(obj)")

        def task_remote_proxy(self_remote_function, args, kwargs, **task_options):
            return self.task_remote(self_remote_function, args, kwargs, task_options)

        ray.remote_function.RemoteFunction._remote = task_remote_proxy

        def actor_remote_proxy(self_actor_class, args, kwargs, **task_options):
            return self.actor_remote(self_actor_class, args, kwargs, task_options)

        ray.actor.ActorClass._remote = actor_remote_proxy

        def method_remote_proxy(self_actor_method, args, kwargs, **task_options):
            return self.method_remote(self_actor_method, args, kwargs, task_options)

        ray.actor.ActorMethod._remote = method_remote_proxy

        return self

    async def aexit(self, exc_type, exc_val, exc_tb):
        # TODO: exc handling
        ray.get = self.old_ray_get
        ray.put = self.old_ray_put
        ray.remote_function.RemoteFunction._remote = self.old_ray_remotefunction_remote
        ray.actor.ActorClass._remote = self.old_ray_actorclass_remote
        ray.actor.ActorMethod._remote = self.old_ray_actormethod_remote

        self.old_ray_get = None
        self.old_ray_put = None
        self.old_ray_remotefunction_remote = None
        self.old_ray_actorclass_remote = None
        self.old_ray_actormethod_remote = None

        if not self.detached:
            await self.kill_actor()

        await self.aiohttp_client_session.__aexit__(exc_type, exc_val, exc_tb)

    async def kill_actor(self):
        async with self.aiohttp_client_session.delete(
            f"{self.server_addr}/api/clients/{self.actor_name}"
        ) as resp:
            if resp.status != 200:
                raise ValueError(f"error: {resp.status}, {await resp.read()}")

    def warning_dont_use(self, api_name, suggest_name):
        my_context_name = self.__class__.__name__

        def wrapped(*args, **kwargs):
            raise ValueError(
                f"WARNING: You are using ray API: `{api_name}` which can "
                "only be accessed within a Ray Cluster. You are in a "
                f"{my_context_name} context, consider using: `{suggest_name}`"
            )

        return wrapped

    async def get_async(self, object_refs, timeout=None):
        # TODO: support more than 1 object ref, already done?
        async with self.aiohttp_client_session.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/get",
            data=self.dumps(object_refs),
        ) as resp:
            if resp.status == 200:
                data = await resp.read()
                obj = self.loads(data)
                return obj
            else:
                # TODO: better error handling
                raise ValueError(f"error: {resp.status}, {await resp.read()}")

    async def put_async(self, value: Any):
        data = self.dumps(value)
        async with self.aiohttp_client_session.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/put", data=data
        ) as resp:
            if resp.status == 200:
                ref_data = await resp.read()
                print(f"ref data {ref_data}")
                object_ref = self.loads(ref_data)
                print(f"ref data {ref_data}, object ref {object_ref}")
                return object_ref
            else:
                # TODO: better error handling
                raise ValueError(f"error: {resp.status}, {await resp.read()}")

    async def task_remote_async(self, remote_function, args, kwargs, task_options):
        function = remote_function._function
        data = self.dumps((function, args, kwargs, task_options))
        async with self.aiohttp_client_session.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/task_remote", data=data
        ) as resp:
            if resp.status == 200:
                ref_data = await resp.read()
                object_ref = self.loads(ref_data)
                print(f"task_remote: ref data {ref_data}, object ref {object_ref}")
                return object_ref
            else:
                # TODO: better error handling
                raise ValueError(f"error: {resp.status}, {await resp.read()}")

    async def actor_remote_async(self, actor_cls, args, kwargs, task_options):
        data = self.dumps((actor_cls, args, kwargs, task_options))
        async with self.aiohttp_client_session.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/actor_remote", data=data
        ) as resp:
            actor_states_binary = await resp.read()
            actor_id, actor_method_cpu, current_session_and_job = self.loads(
                actor_states_binary
            )
            meta = actor_cls.__ray_metadata__
            handle = ray.actor.ActorHandle(
                meta.language,
                actor_id,
                meta.method_meta.decorators,
                meta.method_meta.signatures,
                meta.method_meta.num_returns,
                actor_method_cpu,
                meta.actor_creation_function_descriptor,
                current_session_and_job,
                original_handle=True,
            )
            return handle

    async def method_remote_async(self, actor_method, args, kwargs, task_options):
        # num_returns = task_options.get('num_returns')
        # if num_returns is None:
        #     num_returns = actor_method._num_returns
        # if actor_method._decorator is not None:

        actor = actor_method._actor_hard_ref or actor_method._actor_ref()
        method_name = actor_method._method_name
        data = self.dumps((actor._actor_id, method_name, args, kwargs, task_options))
        async with self.aiohttp_client_session.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/method_remote", data=data
        ) as resp:
            if resp.status == 200:
                ref_data = await resp.read()
                print(f"ref data {ref_data}")
                object_ref = self.loads(ref_data)
                print(f"ref data {ref_data}, object ref {object_ref}")
                return object_ref
            else:
                # TODO: better error handling
                raise ValueError(f"error: {resp.status}, {await resp.read()}")

    def __enter__(self):
        self.loop.run_until_complete(self.aenter())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.loop.run_until_complete(self.aexit(exc_type, exc_val, exc_tb))

    def get(self, object_refs, timeout=None):
        return self.loop.run_until_complete(self.get_async(object_refs, timeout))

    def put(self, value):
        return self.loop.run_until_complete(self.put_async(value))

    def task_remote(self, func, *args, **kwargs) -> "ray.ObjectRef":
        return self.loop.run_until_complete(
            self.task_remote_async(func, *args, **kwargs)
        )

    def actor_remote(self, cls, *args, **kwargs):
        return self.loop.run_until_complete(
            self.actor_remote_async(cls, *args, **kwargs)
        )

    def method_remote(self, method, *args, **kwargs):
        return self.loop.run_until_complete(
            self.method_remote_async(method, *args, **kwargs)
        )


def actor_name_for_session_name(name: str):
    return f"ray_client2_actor_{name}"
