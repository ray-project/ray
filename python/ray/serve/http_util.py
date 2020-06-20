import io
import json

import flask


def build_flask_request(asgi_scope_dict, request_body):
    """Build and return a flask request from ASGI payload

    This function is indented to be used immediately before task invocation
    happen.
    """
    wsgi_environ = build_wsgi_environ(asgi_scope_dict, request_body)
    return flask.Request(wsgi_environ)


def build_wsgi_environ(scope, body):
    """
    Builds a scope and request body into a WSGI environ object.

    This code snippet is taken from https://github.com/django/asgiref/blob
    /36c3e8dc70bf38fe2db87ac20b514f21aaf5ea9d/asgiref/wsgi.py#L52

    WSGI specification can be found at
    https://www.python.org/dev/peps/pep-0333/

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
        "wsgi.errors": io.BytesIO(),
        "wsgi.multithread": True,
        "wsgi.multiprocess": True,
        "wsgi.run_once": False,
    }

    # Get server name and port - required in WSGI, not in ASGI
    environ["SERVER_NAME"] = scope["server"][0]
    environ["SERVER_PORT"] = str(scope["server"][1])
    environ["REMOTE_ADDR"] = scope["client"][0]

    # Transforms headers into environ entries.
    for name, value in scope.get("headers", []):
        # name, values are both bytes, we need to decode them to string
        name = name.decode("latin1")
        value = value.decode("latin1")

        # Handle name correction to conform to WSGI spec
        # https://www.python.org/dev/peps/pep-0333/#environ-variables
        if name == "content-length":
            corrected_name = "CONTENT_LENGTH"
        elif name == "content-type":
            corrected_name = "CONTENT_TYPE"
        else:
            corrected_name = "HTTP_%s" % name.upper().replace("-", "_")

        # If the header value repeated,
        # we will just concatenate it to the field.
        if corrected_name in environ:
            value = environ[corrected_name] + "," + value

        environ[corrected_name] = value
    return environ


class Response:
    """ASGI compliant response class.

    It is expected to be called in async context and pass along
    `scope, receive, send` as in ASGI spec.

    >>> await Response({"k": "v"}).send(scope, receive, send)
    """

    def __init__(self, content=None, status_code=200):
        """Construct a HTTP Response based on input type.

        Args:
            content (optional): Any JSON serializable object.
            status_code (int, optional): Default status code is 200.
        """
        self.status_code = status_code
        self.raw_headers = []

        if content is None:
            self.body = b""
            self.set_content_type("text")
        elif isinstance(content, bytes):
            self.body = content
            self.set_content_type("text")
        elif isinstance(content, str):
            self.body = content.encode("utf-8")
            self.set_content_type("text-utf8")
        else:
            # Delayed import since utils depends on http_util
            from ray.serve.utils import ServeEncoder
            self.body = json.dumps(
                content, cls=ServeEncoder, indent=2).encode()
            self.set_content_type("json")

    def set_content_type(self, content_type):
        if content_type == "text":
            self.raw_headers.append([b"content-type", b"text/plain"])
        elif content_type == "text-utf8":
            self.raw_headers.append(
                [b"content-type", b"text/plain; charset=utf-8"])
        elif content_type == "json":
            self.raw_headers.append([b"content-type", b"application/json"])
        else:
            raise ValueError("Invalid content type {}".foramt(content_type))

    async def send(self, scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })
        await send({"type": "http.response.body", "body": self.body})
