import time
from collections import defaultdict, deque

import ray
from ray.experimental.serve.kv_store_service import KVStoreProxyActor
from ray.experimental.serve.queues import CentralizedQueuesActor
from ray.experimental.serve.utils import logger
from ray.experimental.serve.server import HTTPActor

# TODO(simon): this will be moved in namespaced kv stores


class GlobalState:
    """Encapsulate the global state in the serving system.

    Warning:
        Currently the state resides inside driver process. The state will be
        moved into a key value stored service AND a supervisor service.
    """

    def __init__(self):
        #: holds all actor handles.
        self.actor_nursery = []

        #: actor handle to KV store actor
        self.api_handle = None
        #: actor handle to HTTP server
        self.http_handle = None
        #: actor handle the the router/queues actor
        self.router = None

        #: List[str] list of backend names, used for deduplication
        self.registered_backends = []
        #: List[str] list of service endpoint names, used for deduplication
        self.registered_endpoints = []

        #: Mapping of endpoints -> a stack of traffic policy
        self.policy_action_history = defaultdict(deque)

        #: HTTP address. Currently it's hard coded to localhost with port 8000
        self.http_address = ""

    def init_api_server(self):
        logger.info("[Global State] Initalizing Routing Table")
        self.api_handle = KVStoreProxyActor.remote()
        logger.info(
            "[Global State] Health Checking Routing Table %s",
            ray.get(self.api_handle.get_request_count.remote()),
        )

    def init_http_server(self):
        logger.info("[Global State] Initializing HTTP Server")
        self.http_handle = HTTPActor.remote(self.api_handle, self.router)
        self.http_handle.run.remote(host="0.0.0.0", port=8000)
        self.http_address = f"http://localhost:8000"

    def init_router(self):
        logger.info("[Global State] Initializing Queuing System")
        self.router = CentralizedQueuesActor.remote()
        self.router.register_self_handle.remote(self.router)

    def shutdown(self):
        ray.shutdown()

    def __del__(self):
        self.shutdown()

    def wait_until_http_ready(self):
        routing_table_request_count = 0
        retries = 5

        while not routing_table_request_count:
            routing_table_request_count = (ray.get(
                self.api_handle.get_request_count.remote()))
            logger.debug(("[Global State] Making sure HTTP Server is ready."
                          "{} retries left.").format(retries))
            time.sleep(1)
            retries -= 1
            if retries == 0:
                raise Exception("Too many retries, HTTP is not ready")
