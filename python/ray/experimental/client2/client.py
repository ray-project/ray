import logging
import time
from typing import Any, Callable, Optional

import requests

import ray
from ray._private.utils import get_or_create_event_loop
from ray.dashboard.modules.job.common import JobStatus
from ray.experimental.client2.pickler import (
    ClientToServerPickler,
    ServerToClientUnpickler,
    dumps_with_pickler_cls,
    loads_with_unpickler_cls,
)
from ray.job_submission import JobSubmissionClient
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)


def poll_until(func: Callable[[], bool], times: int, delay_s: int):
    # Wait for N loops, each sleeps D secs until func() to return True
    for _ in range(times):
        if func():
            return True
        time.sleep(delay_s)
    return False


class Client:
    # static variables
    dumps = dumps_with_pickler_cls(ClientToServerPickler)
    loads = loads_with_unpickler_cls(ServerToClientUnpickler)
    # Ray methods to be hijecked
    ray_get = ray.get
    ray_put = ray.put
    ray_remotefunction_remote = ray.remote_function.RemoteFunction._remote
    ray_actorclass_remote = ray.actor.ActorClass._remote
    ray_actormethod_remote = ray.actor.ActorMethod._remote
    # active client (can only have 1)
    active_client = None

    def __init__(
        self,
        server_addr: str,
        channel_name: str,
        connect_only: bool = False,
        runtime_env: Optional[RuntimeEnv] = None,
        ttl_secs: int = 60 * 60,  # 1hr
    ) -> None:
        """
        Connects to a Ray Client2 Channel, or create one if it does not exist.

        If connect_only == True, only try to connect and raises if channel does not
        exist.

        ttl_secs: Time-to-live for a driver to live since the last client channel
        disconnected. After the TTL, the driver exits, killing any remaining objects,
        tasks, actors. Must be >= 10.
        """

        if ttl_secs < 10:
            raise ValueError("ttl_secs must be >= 10")

        self.server_addr = server_addr
        self.channel_name = channel_name
        self.actor_name = actor_name_for_channel_name(self.channel_name)
        self.connect_only = connect_only
        self.runtime_env = runtime_env
        self.ttl_secs = ttl_secs

        self.check_no_active_client_or_ray()

        if self.connect_only:
            self.connect()
        else:
            self.connect_or_create()

    def connect_or_create(self):
        if self.connect_once():
            return
        self.create()
        self.connect()

    def connect(self):
        if not poll_until(lambda: self.connect_once(), times=10, delay_s=1):
            raise ValueError("Can't connect after 10s of waiting")
        self.set_active()

    def disconnect(self, kill_channel=False):
        """
        Disconnects this client.
        If kill_channel is True, also kills the remote channel.
        """
        ray.get = Client.ray_get
        ray.put = Client.ray_put
        ray.remote_function.RemoteFunction._remote = Client.ray_remotefunction_remote
        ray.actor.ActorClass._remote = Client.ray_actorclass_remote
        ray.actor.ActorMethod._remote = Client.ray_actormethod_remote

        Client.active_client = None

        if kill_channel:
            self.kill_actor()

    def get(self, object_refs, timeout=None):
        """
        Example:

        obj = client.get(obj_ref)
        obj1, obj2 = client.get([ref1, ref2])
        """
        self.check_self_is_active()
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/get",
            data=Client.dumps(object_refs),
            timeout=timeout,
        )
        resp.raise_for_status()

        return Client.loads(resp.content)

    def put(self, value: Any):
        """
        Example:

        obj_ref = client.put(obj_ref)
        """
        self.check_self_is_active()
        # TODO: streaming/chunked big ones to reduce agent mem cost,
        # assemble in the actor
        data = Client.dumps(value)
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/put", data=data
        )
        resp.raise_for_status()
        object_ref = Client.loads(resp.content)
        return object_ref

    def task(self, remote_function: ray.remote_function.RemoteFunction):
        """
        Example:

        obj = client.task(my_task).remote(param1, param2)
        obj = client.task(my_task).options(num_cpus=12).remote(param1, param2)
        """
        client_self = self

        class FuncWrapper:
            def __init__(self) -> None:
                self.task_options = None

            def remote(self, *args, **kwargs):
                return client_self.task_remote(
                    remote_function, args, kwargs, self.task_options
                )

            def options(self, **task_options):
                if self.task_options is not None:
                    raise ValueError("task_options already set")
                self.task_options = task_options
                return self

        return FuncWrapper()

    def actor(self, actor_cls):
        """
        Example:

        actor = client.actor(Actor).remote(param1, param2)
        actor = client.actor(Actor).options(num_cpus=12).remote(param1, param2)
        """

        client_self = self

        class ActorWrapper:
            def __init__(self) -> None:
                self.task_options = None

            def remote(self, *args, **kwargs):
                return client_self.actor_remote(
                    actor_cls, args, kwargs, self.task_options
                )

            def options(self, **task_options):
                if self.task_options is not None:
                    raise ValueError("task_options already set")
                self.task_options = task_options
                return self

        return ActorWrapper()

    def method(self, method: ray.actor.ActorMethod):
        """
        Examples:

        obj_ref = client.method(actor.method).remote(param1, param2)
        obj_ref = client.method(actor.method).options(num_cpus=12).remote(param1, param2)
        """

        client_self = self

        class MethodWrapper:
            def __init__(self) -> None:
                self.task_options = None

            def remote(self, *args, **kwargs):
                return client_self.method_remote(
                    method, args, kwargs, self.task_options
                )

            def options(self, **task_options):
                if self.task_options is not None:
                    raise ValueError("task_options already set")
                self.task_options = task_options
                return self

        return MethodWrapper()

    ###################### private methods ######################

    def create(self) -> None:
        """
        Submits a job. Returns after the job is running. Raises if after 10s it's still
        not running.
        """
        job_client = JobSubmissionClient(self.server_addr)

        self.submission_id = job_client.submit_job(
            entrypoint=f"python -m ray.experimental.client2.driver {self.actor_name} {self.ttl_secs}",
            runtime_env=self.runtime_env,
            # TODO: now re-creating a job w/ same ID raises error.
            submission_id=self.actor_name,
        )
        if poll_until(
            # TODO: if job failed, we will wait for 10s and report timeout.
            lambda: job_client.get_job_status(self.submission_id) == JobStatus.RUNNING,
            times=10,
            delay_s=1,
        ):
            return
        raise ValueError(
            f"Job {self.submission_id} not running after 10 seconds of waiting"
        )

    def connect_once(self) -> bool:
        """
        - if 200 -> return True
        - if 404 -> not exist, return False
        - others: unexpected, raise
        """
        resp = requests.get(f"{self.server_addr}/api/clients/{self.actor_name}")
        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            logger.info(
                "client2 channel not connected, maybe the ClientSupervisor "
                "is still starting..."
            )
            return False
        resp.raise_for_status()

    def check_no_active_client_or_ray(self):
        if Client.active_client is not None:
            raise ValueError(
                f"Already have active client {Client.active_client.channel_name}, "
                "consider client.disconnect()."
            )
        if ray.is_initialized():
            raise ValueError("Already connected to Ray, consider ray.shutdown().")

    def check_self_is_active(self):
        if Client.active_client is None:
            raise ValueError(
                f"This client {self.channel_name} is not active, consider client.create_or_connect()"
            )
        if Client.active_client is not self:
            raise ValueError(
                f"This client {self.channel_name} is not active, instead another client {Client.active_client.channel_name} is active. consider Client.active_client.disconnect() then client.create_or_connect()"
            )

    def set_active(self):
        self.check_no_active_client_or_ray()

        Client.active_client = self

        ray.get = self.warning_dont_use("ray.get(obj_ref)", "client.get(obj_ref)")
        ray.put = self.warning_dont_use("ray.put(obj)", "client.put(obj)")
        ray.remote_function.RemoteFunction._remote = self.warning_dont_use(
            "my_task.remote(params)", "client.task(my_task).remote(params)"
        )
        ray.actor.ActorClass._remote = self.warning_dont_use(
            "MyActor.remote(params)", "client.actor(MyActor).remote(params)"
        )
        ray.actor.ActorMethod._remote = self.warning_dont_use(
            "my_actor.my_method.remote(params)",
            "client.method(my_actor.my_method).remote(params)",
        )
        # TODO: also mock out RemoteFunction.options and actor

    def kill_actor(self):
        resp = requests.delete(f"{self.server_addr}/api/clients/{self.actor_name}")
        resp.raise_for_status()

    def warning_dont_use(self, api_name, suggest_name):
        my_context_name = self.__class__.__name__

        def wrapped(*args, **kwargs):
            raise ValueError(
                f"WARNING: You are using ray API: `{api_name}` which can "
                "only be accessed within a Ray Cluster. You are in a "
                f"{my_context_name} context, consider using: `{suggest_name}`"
            )

        return wrapped

    def task_remote(self, remote_function, args, kwargs, task_options):
        """
        remote_function: ray.remote_function.RemoteFunction, verbatim, includes default options
        task_options: the extra options from client.task(f).options(..here..)
        """
        self.check_self_is_active()

        data = Client.dumps((remote_function, args, kwargs, task_options))
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/task_remote", data=data
        )
        resp.raise_for_status()
        object_ref = Client.loads(resp.content)
        return object_ref

    def actor_remote(
        self, actor_cls, args, kwargs, task_options
    ) -> ray.actor.ActorHandle:
        self.check_self_is_active()

        data = Client.dumps((actor_cls, args, kwargs, task_options))
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/actor_remote", data=data
        )
        resp.raise_for_status()
        # Our unpickler constructs an ActorHandle.
        handle = Client.loads(resp.content)
        return handle

    def method_remote(self, actor_method, args, kwargs, task_options):
        self.check_self_is_active()

        actor = actor_method._actor_hard_ref or actor_method._actor_ref()
        method_name = actor_method._method_name
        data = Client.dumps((actor._actor_id, method_name, args, kwargs, task_options))
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/method_remote", data=data
        )
        resp.raise_for_status()
        object_ref = Client.loads(resp.content)
        return object_ref


def actor_name_for_channel_name(name: str):
    return f"ray_client2_actor_{name}"
