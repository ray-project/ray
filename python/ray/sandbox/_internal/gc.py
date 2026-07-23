import logging
import time

import ray
from ray.sandbox._internal.k8s_client import get_k8s_core_v1_api

logger = logging.getLogger(__name__)


@ray.remote
class SandboxGCDaemon:
    """Background Ray Actor to clean up expired or orphaned sandbox pods in Kubernetes."""

    def __init__(
        self, namespace: str = "default", check_interval_seconds: float = 60.0
    ):
        self.namespace = namespace
        self.check_interval_seconds = check_interval_seconds
        self.api = get_k8s_core_v1_api()

    def run_gc_sweep(self) -> int:
        """Scan Kubernetes cluster for expired sandbox pods and delete them."""
        deleted_count = 0
        try:
            pods = self.api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector="app=ray-sandbox",
            )
            now = int(time.time())
            for pod in pods.items:
                labels = pod.metadata.labels or {}
                created_at = int(labels.get("ray.io/created-at", now))
                ttl = labels.get("ray.io/ttl")

                if ttl is not None and (now - created_at) > int(ttl):
                    logger.info(
                        f"GC Daemon deleting expired sandbox pod '{pod.metadata.name}' "
                        f"(age: {now - created_at}s, ttl: {ttl}s)"
                    )
                    try:
                        self.api.delete_namespaced_pod(
                            name=pod.metadata.name,
                            namespace=self.namespace,
                            grace_period_seconds=0,
                        )
                        deleted_count += 1
                    except Exception as err:
                        logger.warning(
                            f"GC failed to delete pod '{pod.metadata.name}': {err}"
                        )
        except Exception as err:
            logger.error(f"Error during Sandbox GC sweep: {err}")
        return deleted_count
