import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
import ray.util.state
from aiohttp.web import Request, Response, StreamResponse
import ray._private.ray_constants as ray_constants
import ray
import logging
import asyncio
from collections import defaultdict
import threading
import json
import functools

routes = optional_utils.DashboardHeadRouteTable
logger = logging.getLogger(__name__)


class GcsLogSubscriberPerJob:
    """
    Creates a thread that does the sync polling from GcsLogSubscriber, and provides a
    per-job, *async* view.

    Each job (by job ID) gets a queue of `dict` for their logs.
    Cluster-wide logs (e.g. from autoscaler) are discarded.

    TODO(ryw): if no one is waiting for the logs, they accumulate here. Maybe add log
    rotations?
    """

    def __init__(self):
        """
        MUST be called *after* Ray has been inited.
        """
        runtime_context = ray.get_runtime_context()
        worker_id = runtime_context.get_worker_id()
        gcs_address = runtime_context.gcs_address

        self.gcs_log_subscriber = ray._raylet.GcsLogSubscriber(
            worker_id=worker_id, address=gcs_address
        )
        self.gcs_log_subscriber.subscribe()

        self.per_job_logs = defaultdict(asyncio.Queue)

        self.thread = threading.Thread(target=lambda: self.poll_forever())
        self.thread.daemon = True
        self.thread.start()

    def poll_forever(self):
        while True:
            data = self.gcs_log_subscriber.poll()
            job_id = data.get("job", "")
            if job_id is not None and len(job_id) > 0:
                self.per_job_logs[job_id].put_nowait(data)

    async def poll(self, job_id):
        return await self.per_job_logs[job_id].get()


def ray_actor_error_as_410_gone(func):
    """
    Decorator on the handler functions. If got RayActorError, returns 410 Gone, assuming
    the actor is dead.

    Decorator order is important. This must be put in the inner layer.
    """

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ray.exceptions.RayActorError as e:
            return Response(text=str(e), status=410)

    return wrapper


class ClientHead(dashboard_utils.DashboardHeadModule):
    """Client proxy in the head.

    This module manages CRUD of the client, as well as proxying client get/put calls
    to the corresponding actors.

    All client2 traffic goes through this head module on the dashboard http port so we
    don't require a new port.

    client head (http server) <1-*> each client's actor

    Create a client: the client uses job submission API to submit a job which starts a
    deteched actor in RAY_INTERNAL_CLIENT2_NAMESPACE namespace.

    Delete a client:
        DELETE /api/clients/{name}

    List clients: (not supported)

    Ping a client to test liveness:
        GET /api/clients/{name} (404 or 200)

    Use a client: the APIs
        POST /api/clients/{name}/get
        POST /api/clients/{name}/put
        POST /api/clients/{name}/task_remote
        POST /api/clients/{name}/actor_remote
        POST /api/clients/{name}/method_remote

        Returns 200 for return values or any Exceptions within the actor.

    Streamed logs:
        POST /api/clients/{name}/logs
            Long connection, sending logs in a stream

    Note: all these APIs are getting/putting in serialized form. This is because we
    don't want to deserialize it here in the http agent and serialize it again just to
    pass them to the actor; Further, it's not doable because they may share a same
    serialization context (e.g. some modules defined in runtime env) that this http
    server does not know.
    """

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        # have to be initiated *after* ray.init().
        self.log_subscriber = None

    def get_log_subscriber(self) -> GcsLogSubscriberPerJob:
        """
        NOTE: must be called **after** ray has been inited.
        """
        if self.log_subscriber is None:
            self.log_subscriber = GcsLogSubscriberPerJob()
        return self.log_subscriber

    @routes.get("/api/clients/{client_actor_name}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def ping(self, req: Request) -> Response:
        """
        Pings the actor.
        Payload: None.
        Returns: 200, payload=text("OK").
        If actor not found -> 404.
        """
        client_actor_name = req.match_info["client_actor_name"]
        try:
            ray.get_actor(
                client_actor_name,
                namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE,
            )
        except Exception as e:
            return Response(text=str(e), status=404)
        return Response(text=client_actor_name)

    @routes.post("/api/clients/{client_actor_name}/get")
    @optional_utils.init_ray_and_catch_exceptions()
    @ray_actor_error_as_410_gone
    async def ray_get(self, req: Request) -> Response:
        """
        Proxies `client.get()`.
        Payload: binary of an Object Ref.
        Returns: pickled object.
        """
        client_actor_name = req.match_info["client_actor_name"]
        actor = ray.get_actor(
            client_actor_name, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        obj_ref_binary = await req.read()
        obj_serialized = await actor.ray_get.remote(obj_ref_binary)
        return Response(body=obj_serialized)

    @routes.post("/api/clients/{client_actor_name}/put")
    @optional_utils.init_ray_and_catch_exceptions()
    @ray_actor_error_as_410_gone
    async def ray_put(self, req: Request) -> Response:
        """
        Proxies `client.put()`.
        Payload: pickled object.
        Returns: binary of an Object Ref.
        """
        client_actor_name = req.match_info["client_actor_name"]
        actor = ray.get_actor(
            client_actor_name, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        obj_serialized = await req.read()
        obj_ref_bytes = await actor.ray_put.remote(obj_serialized)
        return Response(body=obj_ref_bytes)

    @routes.post("/api/clients/{client_actor_name}/task_remote")
    @optional_utils.init_ray_and_catch_exceptions()
    @ray_actor_error_as_410_gone
    async def task_remote(self, req: Request):
        """
        Proxies `func.remote(args, kwargs, task_options)`.
        Payload: pickled 4-tuple.
        Returns: binary of an Object Ref.
        """
        client_actor_name = req.match_info["client_actor_name"]
        actor = ray.get_actor(
            client_actor_name, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        pickled_func_and_args = await req.read()
        obj_ref_bytes = await actor.task_remote.remote(pickled_func_and_args)
        return Response(body=obj_ref_bytes)

    @routes.post("/api/clients/{client_actor_name}/actor_remote")
    @optional_utils.init_ray_and_catch_exceptions()
    @ray_actor_error_as_410_gone
    async def actor_remote(self, req: Request):
        """
        Proxies `actor.remote(args, kwargs)`.
        Payload: pickled 3-tuple.
        Returns: pickled actor handle's state.
        """
        client_actor_name = req.match_info["client_actor_name"]
        actor = ray.get_actor(
            client_actor_name, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        pickled_func_and_args = await req.read()
        actor_state_bytes = await actor.actor_remote.remote(pickled_func_and_args)
        return Response(body=actor_state_bytes)

    @routes.post("/api/clients/{client_actor_name}/method_remote")
    @optional_utils.init_ray_and_catch_exceptions()
    @ray_actor_error_as_410_gone
    async def method_remote(self, req: Request):
        """
        Proxies `actor.method.remote(args, kwargs)`.
        Payload: pickled 3-tuple.
        Returns: pickled object ref.
        """
        client_actor_name = req.match_info["client_actor_name"]
        actor = ray.get_actor(
            client_actor_name, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        pickled_func_and_args = await req.read()
        obj_ref_bytes = await actor.method_remote.remote(pickled_func_and_args)
        return Response(body=obj_ref_bytes)

    @routes.post("/api/clients/{client_actor_name}/logs")
    @optional_utils.init_ray_and_catch_exceptions()
    @ray_actor_error_as_410_gone
    async def logs(self, req: Request):
        """
        Subscribes all logs in the job and streams them in a long connection.
        Returns: JSON objects one per line.

        Implementation: Lazily creates a singleton GcsLogSubscriber. We can't subscribe
        to global_worker_stdstream_dispatcher because in we set `log_to_driver=False` in
        `init_ray_and_catch_exceptions` to avoid Ray logs polluting `dashboard.log`.

        GcsLogSubscriber has a sync API `poll()` so I create a separate thread just to
        poll and triage the logs by JobID. Each client session corresponds to a job,
        and the JobID is fetched by asking the Actor.

        Cluster-wide logs, e.g. from autoscaler are *ignored* for now.
        """
        client_actor_name = req.match_info["client_actor_name"]
        actor = ray.get_actor(
            client_actor_name, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )
        job_id = await actor.get_job_id.remote()

        # It's strange we don't have an MIME type for a stream of JSONs delimited by \n.
        # There are some x- ones and unregistered ones, but I don't like them.
        response = StreamResponse(headers={"Content-Type": "application/octet-stream"})
        await response.prepare(req)

        # Infinite loop to stream data
        try:
            while True:
                subscriber = self.get_log_subscriber()
                data = await subscriber.poll(job_id)
                if data is not None:
                    await response.write(json.dumps(data).encode())
                    await response.write(b"\n")
        except ConnectionResetError:
            logger.info(f"Client disconnected, exiting logs for {client_actor_name}")
        except Exception:
            logger.exception(f"unexpected exception in logs for {client_actor_name}")
        return response

    @routes.delete("/api/clients/{client_actor_name}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def kill_actor(self, req: Request):
        """
        Kills an actor, together with all tasks, actors and object references.

        Idempotent. If the actor is already not found, returns 200.

        Payload: (None).
        Returns: empty resposne with status=200.
        """

        client_actor_name = req.match_info["client_actor_name"]
        try:
            actor = ray.get_actor(
                client_actor_name,
                namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE,
            )
            ray.kill(actor)
        except ray.exceptions.RayActorError as e:
            pass
        return Response(status=200)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
