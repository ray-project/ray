import json
import logging
import random
import string
import threading
import time
from typing import Any, Callable, Optional

import requests

import ray
from ray._private.worker_logging import print_to_stdstream
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

WATCHDOG_PING_PERIOD_SECS = 60  # ping every 1 minute


def poll_until(func: Callable[[], bool], times: int, delay_s: int):
    # Wait for N loops, each sleeps D secs until func() to return True
    for _ in range(times):
        if func():
            return True
        time.sleep(delay_s)
    return False


def generate_client_actor_name() -> str:
    """Returns an actor_id of the form 'rayclient2_XYZ'.

    Code copied from dashboard/modules/job/job_manager.py
    """
    rand = random.SystemRandom()
    possible_characters = list(
        set(string.ascii_letters + string.digits)
        - {"I", "l", "o", "O", "0"}  # No confusing characters
    )
    id_part = "".join(rand.choices(possible_characters, k=16))
    return f"rayclient2_{id_part}"


def raise_or_return(resp: requests.Response):
    """
    If `resp` is good and represents a return value, returns it.
    If it represents an actor-raised Exception, raises it.
    Otherwise (something wrong, not from actor), raises HTTPError.
    """
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise ray.exceptions.RaySystemError(e.response.text)
    result_or_exc = Client.loads(resp.content)
    if result_or_exc.exc is not None:
        raise result_or_exc.exc
    return result_or_exc.result


class Client:
    """
    Client to connect to an existing Ray Cluster.

    # Overview

    client = Client2("http://localhost:8265")

    Upon connnection, this Python instance can not initialize with Ray, or connect to
    another Client, unless this Client has been disconnected.

    User can use these methods to talk to Ray Cluster:

    - ray.get
    - ray.put
    - f.remote()
    - Actor.remote()
    - actor.method.remote()

    # Connection

    Initing a Client instance creates a Ray job in the existing Ray Cluster.

    By default the time-to-live for the job is 1 hour. This means if the network
    breaks and there's no client connected to the same job, the remote job stays
    alive for 1 hour. During this period, if the client makes any communication to the
    job, the job would live on. The client also sends periodical heartbeats to keep the job alive.

    If the job did not recieve any communication or heartbeat and the TTL expired, the remote job terminates itself along with
    all the objects, tasks and actors. To set this number, use `ttl_secs=your_ttl_secs`
    in the Client constructor.

    If the client manually called disconnect() or is `del`ed, it explicitly terminates the remote job.

    One can specify a runtime_env during client creation. For example,

    client = Client2("http://localhost:8265", runtime_env={"pip":["torch", "transformers", "datasets"]})

    # Tips

    - Version skew: you can have a different `torch` version locally vs remotely. You
        can even don't have a local `torch` installation; the remote one is the source
        of truth.
    - Wrap your code in a `@ray.remote` function as much as possible, and only do `client.get`
        if you want do do local visualizations (e.g. plotting).

    # Limitations (TODOs)

    Before we can call it "production-ready", there are a bunch of things to do:

    - if you code involves custom type, you need to use runtime_env even if the custom type is defiend in your code.
    - client.get now limits to 200MB, needs a larger limit, maybe chunking and streaming.
    - tasks' prints are not forwarded to the client.
    - One still have to pip install and import a lib to easily use them, even though the usage is mostly wrapped in a remote function.
        - for example, if you define a `class NeuralNetwork(nn.Module)` you need to first `from torch import nn`, even though the invocations are in remote.
    - Not showing good exception info on driver init failure (e.g. invalid rt env)
    - connect() polls are fixed 1s * 10. Can change to a proper backoff.
    - All other non supported Ray APIs needs to be mocked out.
    """

    # static variables
    dumps = dumps_with_pickler_cls(ClientToServerPickler)
    loads = loads_with_unpickler_cls(ServerToClientUnpickler)
    # Ray methods to be hijacked
    ray_get = ray.get
    ray_put = ray.put
    ray_init = ray.init
    ray_remotefunction_remote = ray.remote_function.RemoteFunction._remote
    ray_actorclass_remote = ray.actor.ActorClass._remote
    ray_actormethod_remote = ray.actor.ActorMethod._remote
    # active client (can only have 1 in a python process)
    active_client = None
    watchdog_thread = None
    print_logs_thread = None

    def __init__(
        self,
        server_addr: str,
        runtime_env: Optional[RuntimeEnv] = None,
        ttl_secs: int = 60 * 60,  # 1hr
    ) -> None:
        """
        Creates a remote Ray job and connects to it.

        ttl_secs: Time-to-live for a driver to live since the last client communication.
        After the TTL, the driver exits, killing any remaining objects, tasks, actors.
        Must be >= 10.
        """

        if ttl_secs < 10:
            raise ValueError("ttl_secs must be >= 10")

        self.server_addr = server_addr
        self.actor_name = generate_client_actor_name()
        self.submission_id = None  # set in create()
        self.runtime_env = runtime_env
        self.ttl_secs = ttl_secs

        self.check_no_active_client_or_ray()
        self.create_and_connect()

    def create_and_connect(self):
        self.create()
        self.connect()

    def connect(self):
        """
        Polls the agent until the actor in the job can receive calls. Then mocks out Ray APIs.
        """
        # TODO: now it waits for 10s for the Job to spin up. If we have `pip` runtime envs
        # this may not be enough. Add probes on the client_head to return 503 Service Unavailable
        # and wait further here.
        if not poll_until(lambda: self.ping_once(), times=10, delay_s=1):
            raise ValueError("Can't connect after 10s of waiting")
        logger.info(f"client2 actor {self.actor_name} connected!")
        self.set_active()

    def disconnect(self):
        """
        Disconnects this client. Also kills the job.
        """
        self.unset_active()
        self.kill_actor()

    def get(self, object_refs, timeout=None):
        """
        Example:

        obj = ray.get(obj_ref)
        obj1, obj2 = ray.get([ref1, ref2])
        """
        self.check_self_is_active()
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/get",
            data=Client.dumps(object_refs),
            timeout=timeout,
        )
        return raise_or_return(resp)

    def put(self, value: Any):
        """
        Example:

        obj_ref = ray.put(obj_ref)

        TODO: size of an upload is limited to 100MB (see dashboard/http_server_head.py:181)
        """
        self.check_self_is_active()
        # TODO: streaming/chunked big ones to reduce agent mem cost,
        # assemble in the actor
        data = Client.dumps(value)
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/put", data=data
        )
        return raise_or_return(resp)

    def task(self, remote_function: ray.remote_function.RemoteFunction):
        """
        Example:

        obj = my_task.remote(param1, param2)
        obj = my_task.options(num_cpus=12).remote(param1, param2)
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

    def actor(self, actor_cls: ray.actor.ActorClass):
        """
        Example:

        actor = Actor.remote(param1, param2)
        actor = Actor.options(num_cpus=12).remote(param1, param2)
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

        obj_ref = actor.method.remote(param1, param2)
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

    ###################### experimental public methods ######################

    def run(self, obj):
        """
        EXPERIMENTAL.

        Convenience helper to invoke a method remotely on an object ref. This only works
        for a single method call. If you have chained method calls, or don't want a transmission,
        define a `@ray.remote` function and call `client.task(f).remote(obj)`.

        Transforms this:
            client.task(ray.remote(lambda o: o.f(arg1)))).remote(obj)
        To this:
            client.run(obj).f(arg1)
        """
        client = self

        class RunRemotely:
            def __getattr__(self, attr: str) -> Any:
                def callable(*args, **kwargs):
                    @ray.remote
                    def run_get_helper(remote_obj):
                        m = getattr(remote_obj, attr)
                        return m(*args, **kwargs)

                    return client.task(run_get_helper).remote(obj)

                return callable

        return RunRemotely()

    def __call__(self, task_or_actor_or_method):
        if isinstance(task_or_actor_or_method, ray.remote_function.RemoteFunction):
            return self.task(task_or_actor_or_method)
        if isinstance(task_or_actor_or_method, ray.actor.ActorClass):
            return self.actor(task_or_actor_or_method)
        if isinstance(task_or_actor_or_method, ray.actor.ActorMethod):
            return self.method(task_or_actor_or_method)
        raise TypeError(f"arg needs to be a Ray task, or Actor, or Actor's Method.")

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

    def ping_once(self) -> bool:
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
                f"client2 actor {self.actor_name} not connected, maybe the ClientSupervisor "
                "is still starting..."
            )
            return False
        resp.raise_for_status()

    def check_no_active_client_or_ray(self):
        if Client.active_client is not None:
            raise ValueError(
                f"Already have active client {Client.active_client.actor_name}, "
                "consider Client.active_client.disconnect()."
            )
        if ray.is_initialized():
            raise ValueError("Already connected to Ray, consider ray.shutdown().")

    def check_self_is_active(self):
        if Client.active_client is None:
            raise ValueError(
                f"This client {self.actor_name} is not active, maybe it's already disconnected?"
            )
        if Client.active_client is not self:
            raise ValueError(
                f"This client {self.actor_name} is not active, instead another client {Client.active_client.actor_name} is active. consider use the new client."
            )

    def set_active(self):
        self.check_no_active_client_or_ray()

        Client.active_client = self
        Client.watchdog_thread = self.start_watchdog_thread()
        Client.print_logs_thread = self.start_print_logs_thread()

        ray.get = self.get
        ray.put = self.put
        ray.init = self.warning_dont_use("ray.init()", "client = Client(addr)")

        def task_remote(self_func, args, kwargs, **task_options):
            return self.task_remote(self_func, args, kwargs, task_options)

        ray.remote_function.RemoteFunction._remote = task_remote

        def actor_remote(self_actor, args, kwargs, **actor_options):
            return self.actor_remote(self_actor, args, kwargs, actor_options)

        ray.actor.ActorClass._remote = actor_remote

        def method_remote(self_method, args, kwargs, **actor_options):
            return self.method_remote(self_method, args, kwargs, actor_options)

        ray.actor.ActorMethod._remote = method_remote

    def unset_active(self):
        Client.active_client = None
        # No need to "stop" the thread; it stops itself since self is no longer Client.active_client.
        Client.watchdog_thread = None
        # TODO: stop the thread, or check in the loop
        Client.print_logs_thread = None

        ray.get = Client.ray_get
        ray.put = Client.ray_put
        ray.init = Client.ray_init
        ray.remote_function.RemoteFunction._remote = Client.ray_remotefunction_remote
        ray.actor.ActorClass._remote = Client.ray_actorclass_remote
        ray.actor.ActorMethod._remote = Client.ray_actormethod_remote

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
        task_options: the extra options from f.options(..here..).remote()
        """
        self.check_self_is_active()

        data = Client.dumps((remote_function, args, kwargs, task_options))
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/task_remote", data=data
        )
        return raise_or_return(resp)

    def actor_remote(
        self, actor_cls, args, kwargs, task_options
    ) -> ray.actor.ActorHandle:
        self.check_self_is_active()

        data = Client.dumps((actor_cls, args, kwargs, task_options))
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/actor_remote", data=data
        )
        return raise_or_return(resp)

    def method_remote(self, actor_method, args, kwargs, task_options):
        self.check_self_is_active()

        actor = actor_method._actor_hard_ref or actor_method._actor_ref()
        method_name = actor_method._method_name
        data = Client.dumps((actor._actor_id, method_name, args, kwargs, task_options))
        resp = requests.post(
            f"{self.server_addr}/api/clients/{self.actor_name}/method_remote", data=data
        )
        return raise_or_return(resp)

    def ping_forever(self):
        while self is Client.active_client:
            try:
                if not self.ping_once():
                    logger.warning(f"Client {self.actor_name} ping failure: not found")
            except Exception as e:
                logger.exception(f"Client {self.actor_name} ping failure")
            time.sleep(WATCHDOG_PING_PERIOD_SECS)
        logger.info(f"Client {self.actor_name} is no longer active, stop pinging...")

    def start_watchdog_thread(self):
        thread = threading.Thread(target=lambda: self.ping_forever())
        thread.daemon = True
        thread.start()
        return thread

    def print_logs_forever(self):
        # TODO: if self is Client.active_client, else return
        logger.info(f"starting to print logs for {self.actor_name}")
        try:
            with requests.post(
                f"{self.server_addr}/api/clients/{self.actor_name}/logs", stream=True
            ) as resp:
                raise_or_return(resp)
                for chunk in resp.iter_lines():
                    if chunk:
                        log_json = json.loads(chunk)
                        # dict: ip, pid, job, is_err, lines: List[str], actor_name, task_name
                        print_to_stdstream(log_json)
        except requests.RequestException as e:
            logger.warning(f"Error: {e}")

    def start_print_logs_thread(self):
        thread = threading.Thread(target=lambda: self.print_logs_forever())
        thread.daemon = True
        thread.start()
        return thread
