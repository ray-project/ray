import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
from aiohttp.web import Request, Response
import ray._private.ray_constants as ray_constants
import ray
import logging

routes = optional_utils.DashboardHeadRouteTable
logger = logging.getLogger(__name__)


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

    List a client: (not supported)

    Ping a client to test liveness:
        GET /api/clients/{name} (404 or 200)

    Use a client: the APIs
        POST /api/clients/{name}/get
        POST /api/clients/{name}/put
        POST /api/clients/{name}/task_remote
        POST /api/clients/{name}/actor_remote
        POST /api/clients/{name}/method_remote

        Returns 200 for return values or any Exceptions within the actor.

    Note: all these APIs are getting/putting in serialized form. This is because we don't want to deserialize it here in the http agent and serialize it again just to pass them to the actor; Further, it's not doable because they may share a same serialization context (e.g. some modules defined in runtime env) that this http server does not know.
    """

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/clients/{client_actor_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def ping(self, req: Request) -> Response:
        """
        Pings the actor.
        Payload: None.
        Returns: 200, payload=text("OK").
        If actor not found -> 404.
        """
        client_actor_id = req.match_info["client_actor_id"]
        try:
            actor = ray.get_actor(
                client_actor_id, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
            )
            # client_actor_id = await actor.get_name.remote()
        except Exception as e:
            return Response(text=str(e), status=404)
        return Response(text=client_actor_id)

    @routes.post("/api/clients/{client_actor_id}/get")
    @optional_utils.init_ray_and_catch_exceptions()
    async def ray_get(self, req: Request) -> Response:
        """
        Proxies `client.get()`.
        Payload: binary of an Object Ref.
        Returns: pickled object.
        """
        client_actor_id = req.match_info["client_actor_id"]
        actor = ray.get_actor(
            client_actor_id, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        obj_ref_binary = await req.read()
        obj_serialized = await actor.ray_get.remote(obj_ref_binary)
        return Response(body=obj_serialized)

    @routes.post("/api/clients/{client_actor_id}/put")
    @optional_utils.init_ray_and_catch_exceptions()
    async def ray_put(self, req: Request) -> Response:
        """
        Proxies `client.put()`.
        Payload: pickled object.
        Returns: binary of an Object Ref.
        """
        client_actor_id = req.match_info["client_actor_id"]
        actor = ray.get_actor(
            client_actor_id, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        obj_serialized = await req.read()
        obj_ref_bytes = await actor.ray_put.remote(obj_serialized)
        return Response(body=obj_ref_bytes)

    @routes.post("/api/clients/{client_actor_id}/task_remote")
    @optional_utils.init_ray_and_catch_exceptions()
    async def task_remote(self, req: Request):
        """
        Proxies `func.remote(args, kwargs, task_options)`.
        Payload: pickled 4-tuple.
        Returns: binary of an Object Ref.
        """
        client_actor_id = req.match_info["client_actor_id"]
        actor = ray.get_actor(
            client_actor_id, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        pickled_func_and_args = await req.read()
        obj_ref_bytes = await actor.task_remote.remote(pickled_func_and_args)
        return Response(body=obj_ref_bytes)

    @routes.post("/api/clients/{client_actor_id}/actor_remote")
    @optional_utils.init_ray_and_catch_exceptions()
    async def actor_remote(self, req: Request):
        """
        Proxies `actor.remote(args, kwargs)`.
        Payload: pickled 3-tuple.
        Returns: pickled actor handle's state.
        """
        client_actor_id = req.match_info["client_actor_id"]
        actor = ray.get_actor(
            client_actor_id, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        pickled_func_and_args = await req.read()
        actor_state_bytes = await actor.actor_remote.remote(pickled_func_and_args)
        return Response(body=actor_state_bytes)

    @routes.post("/api/clients/{client_actor_id}/method_remote")
    @optional_utils.init_ray_and_catch_exceptions()
    async def method_remote(self, req: Request):
        """
        Proxies `actor.method.remote(args, kwargs)`.
        Payload: pickled 3-tuple.
        Returns: pickled object ref.
        """
        client_actor_id = req.match_info["client_actor_id"]
        actor = ray.get_actor(
            client_actor_id, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )

        pickled_func_and_args = await req.read()
        obj_ref_bytes = await actor.method_remote.remote(pickled_func_and_args)
        return Response(body=obj_ref_bytes)

    @routes.delete("/api/clients/{client_actor_id}")
    @optional_utils.init_ray_and_catch_exceptions()
    async def kill_actor(self, req: Request):
        """
        Kills an actor, together with all tasks, actors and object references.
        Payload: (None).
        Returns: empty resposne with status=200.
        """

        client_actor_id = req.match_info["client_actor_id"]
        actor = ray.get_actor(
            client_actor_id, namespace=ray_constants.RAY_INTERNAL_CLIENT2_NAMESPACE
        )
        ray.kill(actor)
        return Response(status=200)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
