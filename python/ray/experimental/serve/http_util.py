import io

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
