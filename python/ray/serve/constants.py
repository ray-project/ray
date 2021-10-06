import os

#: Actor name used to register controller
SERVE_CONTROLLER_NAME = "SERVE_CONTROLLER_ACTOR"

#: Actor name used to register HTTP proxy actor
SERVE_PROXY_NAME = "SERVE_PROXY_ACTOR"

#: HTTP Address
DEFAULT_HTTP_ADDRESS = "http://127.0.0.1:8000"

#: HTTP Host
DEFAULT_HTTP_HOST = "127.0.0.1"

#: HTTP Port
DEFAULT_HTTP_PORT = 8000

#: Controller checkpoint path
DEFAULT_CHECKPOINT_PATH = "ray://"

#: Max concurrency
ASYNC_CONCURRENCY = int(1e6)

# How often to call the control loop on the controller.
CONTROL_LOOP_PERIOD_S = 0.1

# Upon controller failure and recovery with running actor names,
# we will update replica handles that halt all traffic to the cluster.
# This constant indicates grace period to avoid controller thrashing.
CONTROLLER_STARTUP_GRACE_PERIOD_S = 5

#: Max time to wait for HTTP proxy in `serve.start()`.
HTTP_PROXY_TIMEOUT = 60

#: Max retry count for allowing failures in replica constructor.
#: If no replicas at target version is running by the time we're at
#: max construtor retry count, deploy() is considered failed.
#: By default we set threshold as min(num_replicas * 3, this value)
MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT = 100

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

SERVE_ROOT_URL_ENV_KEY = "RAY_SERVE_ROOT_URL"

#: Number of historically deleted deployments to store in the checkpoint.
MAX_NUM_DELETED_DEPLOYMENTS = 1000

# Duration that the controller will wait for graceful shutdown before killing
# a replica forcefully.
GRACEFUL_SHUTDOWN_TIMEOUT_S_ENV_VAR = "RAY_SERVE_GRACEFUL_SHUTDOWN_TIMEOUT_S"
GRACEFUL_SHUTDOWN_TIMEOUT_S = float(
    os.getenv(GRACEFUL_SHUTDOWN_TIMEOUT_S_ENV_VAR, "20"))

# Duration that workers will wait until there is no more work to be done.
GRACEFUL_SHUTDOWN_WAIT_LOOP_S_ENV_VAR = "RAY_SERVE_GRACEFUL_SHUTDOWN_WAIT_LOOP_S"  # noqa: E501
GRACEFUL_SHUTDOWN_WAIT_LOOP_S = float(
    os.getenv(GRACEFUL_SHUTDOWN_WAIT_LOOP_S_ENV_VAR, "2"))
