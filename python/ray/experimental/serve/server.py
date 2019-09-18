import asyncio
import json
from io import BytesIO

import uvicorn
from flask import Request
import flask

import ray
from ray.experimental.async_api import _async_init, as_future
from ray.experimental.serve.utils import BytesEncoder
from ray.experimental.serve.constants import HTTP_ROUTER_CHECKER_INTERVAL_S


def build_wsgi_environ(scope, body):
    """
    Builds a scope and request body into a WSGI environ object.

    This code snippet is taken from https://github.com/django/asgiref/blob
    /36c3e8dc70bf38fe2db87ac20b514f21aaf5ea9d/asgiref/wsgi.py#L52

    This function helps translate ASGI scope and body into a flask request.
    """
    environ = {
        "REQUEST_METHOD": scope["method"],
        "SCRIPT_NAME": scope.get("root_path", ""),
        "PATH_INFO": scope["path"],
        "QUERY_STRING": scope["query_string"].decode("ascii"),
        "SERVER_PROTOCOL": "HTTP/{}".format(scope["http_version"]),
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": scope.get("scheme", "http"),
        "wsgi.input": body,
        "wsgi.errors": BytesIO(),
        "wsgi.multithread": True,
        "wsgi.multiprocess": True,
        "wsgi.run_once": False,
    }
    # Get server name and port - required in WSGI, not in ASGI
    if "server" in scope:
        environ["SERVER_NAME"] = scope["server"][0]
        environ["SERVER_PORT"] = str(scope["server"][1])
    else:
        environ["SERVER_NAME"] = "localhost"
        environ["SERVER_PORT"] = "80"

    if "client" in scope:
        environ["REMOTE_ADDR"] = scope["client"][0]

    # Go through headers and make them into environ entries
    for name, value in scope.get("headers", []):
        name = name.decode("latin1")
        if name == "content-length":
            corrected_name = "CONTENT_LENGTH"
        elif name == "content-type":
            corrected_name = "CONTENT_TYPE"
        else:
            corrected_name = "HTTP_%s" % name.upper().replace("-", "_")
        # HTTPbis say only ASCII chars are allowed in headers, but we latin1 just in case
        value = value.decode("latin1")
        if corrected_name in environ:
            value = environ[corrected_name] + "," + value
        environ[corrected_name] = value
    return environ

class JSONResponse:
    """ASGI compliant response class.

    It is expected to be called in async context and pass along
    `scope, receive, send` as in ASGI spec.

    >>> await JSONResponse({"k": "v"})(scope, receive, send)
    """

    def __init__(self, content=None, status_code=200):
        """Construct a JSON HTTP Response.

        Args:
            content (optional): Any JSON serializable object.
            status_code (int, optional): Default status code is 200.
        """
        self.body = self.render(content)
        self.status_code = status_code
        self.raw_headers = [[b"content-type", b"application/json"]]

        ray.register_custom_serializer(flask.Request, use_pickle=True)

    def render(self, content):
        if content is None:
            return b""
        if isinstance(content, bytes):
            return content
        return json.dumps(content, cls=BytesEncoder, indent=2).encode()

    async def __call__(self, scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })
        await send({"type": "http.response.body", "body": self.body})


class HTTPProxy:
    """
    This class should be instantiated and ran by ASGI server.

    >>> import uvicorn
    >>> uvicorn.run(HTTPProxy(kv_store_actor_handle, router_handle))
    # blocks forever
    """

    def __init__(self, kv_store_actor_handle, router_handle):
        """
        Args:
            kv_store_actor_handle (ray.actor.ActorHandle): handle to routing
               table actor. It will be used to populate routing table. It
               should implement `handle.list_service()`
            router_handle (ray.actor.ActorHandle): actor handle to push request
               to. It should implement
               `handle.enqueue_request.remote(endpoint, body)`
        """
        assert ray.is_initialized()

        self.admin_actor = kv_store_actor_handle
        self.router = router_handle
        self.route_table = dict()

    async def route_checker(self, interval):
        while True:
            try:
                self.route_table = await as_future(
                    self.admin_actor.list_service.remote())
            except ray.exceptions.RayletError:  # Gracefully handle termination
                return

            await asyncio.sleep(interval)

    async def __call__(self, scope, receive, send):
        # NOTE: This implements ASGI protocol specified in
        #       https://asgi.readthedocs.io/en/latest/specs/index.html

        if scope["type"] == "lifespan":
            await _async_init()
            asyncio.ensure_future(
                self.route_checker(interval=HTTP_ROUTER_CHECKER_INTERVAL_S))
            return

        current_path = scope["path"]
        if current_path == "/":
            await JSONResponse(self.route_table)(scope, receive, send)
        elif current_path in self.route_table:
            endpoint_name = self.route_table[current_path]

            # TODO(simon): Parse body using a SpooledTemporaryFile like 
            # https://github.com/django/asgiref/blob/master/asgiref/wsgi.py#37
            flask_request = Request(build_wsgi_environ(scope, body=""))
            print(flask_request)
            import pickle
            buffer = BytesIO()
            pickle.dump(flask_request, buffer)
            print("Pickle flask request V. len {}".format(len(buffer.getvalue())))

            result_object_id_bytes = await as_future(
                self.router.enqueue_request.remote(endpoint_name, flask_request))
            result = await as_future(ray.ObjectID(result_object_id_bytes))

            if isinstance(result, ray.exceptions.RayTaskError):
                await JSONResponse({
                    "error": "internal error, please use python API to debug"
                })(scope, receive, send)
            else:
                await JSONResponse({"result": result})(scope, receive, send)
        else:
            error_message = ("Path {} not found. "
                             "Please ping http://.../ for routing table"
                             ).format(current_path)

            await JSONResponse(
                {
                    "error": error_message
                }, status_code=404)(scope, receive, send)


@ray.remote
class HTTPActor:
    def __init__(self, kv_store_actor_handle, router_handle):
        self.app = HTTPProxy(kv_store_actor_handle, router_handle)

    def run(self, host="0.0.0.0", port=8000):
        uvicorn.run(self.app, host=host, port=port, lifespan="on")
