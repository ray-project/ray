import time
from collections import defaultdict, deque, namedtuple

import requests

import ray
import ray.experimental.signal as signal
from ray.experimental.serve.kv_store_service import (SQLiteKVStore, 
                RoutingTable, BackendTable, TrafficPolicyTable)
from ray.experimental.serve.queues import CentralizedQueuesActor
from ray.experimental.serve.utils import logger
from ray.experimental.serve.server import HTTPActor
from ray.experimental.serve.metric import (MetricMonitor,
                                           start_metric_monitor_loop)

# TODO(simon): Global state currently is designed to resides in the driver
#     process. In the next iteration, we will move all mutable states into
#     two actors: (1) namespaced key-value store backed by persistent store
#     (2) actor supervisors holding all actor handles and is responsible
#     for new actor instantiation and dead actor termination.

LOG_PREFIX = "[Global State] "

def start_initial_state():
    # Start the nursery and block until actor nursery has
    # done initialization
    actor_nursery_initializer = start_actor_nursery_loop.remote()
    signal.receive(actor_nursery_initializer)


@ray.remote(num_cpus=0)
def start_actor_nursery_loop():
    """Start and hold the handle to actor nursery"""
    handle = ActorNursery.remote()
    signal.send(handle)
    while True:
        time.sleep(3600)

class ServeActorInfoSignal(signal.Signal):
    def __init__(self, tag, actor_handle):
        self.tag = tag
        self.actor_handle = actor_handle

@ray.remote
class ActorNursery:
    """Initialize and store all actor handles.

    Note:
        This actor is necessary because ray will destory actors when the
        original actor handle goes out of scope (when driver exit). Therefore
        we need to initialize and store actor handles in a seperate actor. 
    """

    def __init__(self):
        # Dict: Actor handles -> (actor_class, init_args)
        self.actor_handles = dict()

    def start_actor(self, actor_cls, init_args, tag):
        """Start an actor and add it to the nursery"""
        handle = actor_cls.remote(*init_args)
        self.actor_handles[handle] = (actor_cls, init_args)
        signal.send(ServeActorInfoSignal(tag, handle))
        return [handle]

class GlobalState:
    """Encapsulate all global state in the serving system.
    
    The information is fetch lazily from 
        1. A collection of namespaced key value stores
        2. A actor supervisor service
    """

    def __init__(self, 
            actor_nursery_handle,
            kv_store_connector=lambda namespace: SQLiteKVStore(namespace), 
        ):
        self.actor_nursery_handle = actor_nursery_handle

        # Connect to all the table
        self.route_table = RoutingTable(kv_store_connector)
        self.backend_table = BackendTable(kv_store_connector)
        self.policy_table = TrafficPolicyTable(kv_store_connector)

        # #: actor handle to KV store actor
        # self.kv_store_actor_handle = None
        # #: actor handle to HTTP server
        # self.http_actor_handle = None
        # #: actor handle the router actor
        # self.router_actor_handle = None

        # #: Set[str] list of backend names, used for deduplication
        # self.registered_backends = set()
        # #: Set[str] list of service endpoint names, used for deduplication
        # self.registered_endpoints = set()

        # #: Mapping of endpoints -> a stack of traffic policy
        # self.policy_action_history = defaultdict(deque)

        # #: Backend creaters. Mapping backend_tag -> callable creator
        # self.backend_creators = dict()
        # #: Number of replicas per backend.
        # #  Mapping backend_tag -> deque(actor_handles)
        # self.backend_replicas = defaultdict(deque)

        # #: HTTP address. Currently it's hard coded to localhost with port 8000
        # #  In future iteration, HTTP server will be started on every node and
        # #  use random/available port in a pre-defined port range. TODO(simon)
        # self.http_address = ""

        # #: Metric monitor handle
        # self.metric_monitor_handle = None


    def init_or_get_http_server(self):
        logger.info(LOG_PREFIX + "Initializing HTTP server")
        self.http_actor_handle = HTTPActor.remote(self.kv_store_actor_handle,
                                                  self.router_actor_handle)
        self.http_actor_handle.run.remote(host="0.0.0.0", port=8000)
        self.http_address = "http://localhost:8000"

    def init_or_get_router(self):
        self.actor_nursery_handle.start_actor.remote(
            CentralizedQueuesActor, 
            init_args=(),
            tag="queue_actor"
        )
        signal.receive(self.actor_nursery_handle)
        self.router_actor_handle = CentralizedQueuesActor.remote()
        self.router_actor_handle.register_self_handle.remote(
            self.router_actor_handle)

    def init_or_get_metric_monitor(self, gc_window_seconds=3600):
        logger.info(LOG_PREFIX + "Initializing metric monitor")
        self.metric_monitor_handle = MetricMonitor.remote(gc_window_seconds)
        start_metric_monitor_loop.remote(self.metric_monitor_handle)
        self.metric_monitor_handle.add_target.remote(self.router_actor_handle)

