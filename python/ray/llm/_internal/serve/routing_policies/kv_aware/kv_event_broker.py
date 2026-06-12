import logging
import threading

import zmq

import ray
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)


class KvEventBroker:
    """ZMQ XSUB/XPUB proxy backing a deployment's event plane.

    Hosted by the deployment's ``KVRouterActor``. With ``DYN_ZMQ_BROKER_URL``
    pointing here, Dynamo's event plane runs in broker mode: each replica's
    ``KvEventPublisher`` connects its PUB socket to the XSUB side and the
    router's ``KvEventConsumer`` connects its SUB socket to the XPUB side.
    """

    def __init__(self):
        self._ctx = zmq.Context()
        self._xsub = self._ctx.socket(zmq.XSUB)
        xsub_port = self._xsub.bind_to_random_port("tcp://0.0.0.0")
        self._xpub = self._ctx.socket(zmq.XPUB)
        xpub_port = self._xpub.bind_to_random_port("tcp://0.0.0.0")

        node_ip = ray.util.get_node_ip_address()
        self.broker_url = (
            f"xsub=tcp://{node_ip}:{xsub_port},xpub=tcp://{node_ip}:{xpub_port}"
        )

        self._proxy_thread = threading.Thread(
            target=self._run_proxy, name="kv-event-broker", daemon=True
        )
        self._proxy_thread.start()
        logger.info("KV event broker started: %s", self.broker_url)

    def _run_proxy(self) -> None:
        try:
            zmq.proxy(self._xsub, self._xpub)
        except zmq.ContextTerminated:
            # close() terminated the context: the proxy's shutdown signal.
            pass

    def close(self) -> None:
        self._ctx.destroy(linger=0)
        self._proxy_thread.join(timeout=5)
