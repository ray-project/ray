#: Actor name used to register controller
SERVE_CONTROLLER_NAME = "SERVE_CONTROLLER_ACTOR"

#: Internal name used for the HTTP proxy deployment.
HTTP_PROXY_DEPLOYMENT_NAME = "ServeHTTPProxy"

#: HTTP Address
DEFAULT_HTTP_ADDRESS = "http://127.0.0.1:8000"

#: HTTP Host
DEFAULT_HTTP_HOST = "127.0.0.1"

#: HTTP Port
DEFAULT_HTTP_PORT = 8000

#: Max concurrency
ASYNC_CONCURRENCY = int(1e6)

#: Default histogram buckets for latency tracker.
DEFAULT_LATENCY_BUCKET_MS = [
    1,
    2,
    5,
    10,
    20,
    50,
    100,
    200,
    500,
    1000,
    2000,
    5000,
]

#: Name of backend reconfiguration method implemented by user.
BACKEND_RECONFIGURE_METHOD = "reconfigure"

#: Internally reserved version tag that cannot be used by applications.
# TODO(edoakes): this should be removed when we remove the old codepath.
RESERVED_VERSION_TAG = "__serve_version__"

#: All defined HTTP methods.
# https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods
ALL_HTTP_METHODS = [
    "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE",
    "PATCH"
]

SERVE_ROOT_URL_ENV_KEY = "RAY_SERVE_ROOT_URL"

#: Number of historically deleted deployments to store in the checkpoint.
MAX_NUM_DELETED_DEPLOYMENTS = 1000
