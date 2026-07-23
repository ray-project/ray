import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

_k8s_client_initialized = False


def get_k8s_core_v1_api(custom_client: Optional[Any] = None) -> Any:
    """Retrieve or initialize the Kubernetes CoreV1Api client.

    Loads in-cluster config when running inside a Kubernetes pod (e.g. Ray cluster on K8s/KubeRay),
    or falls back to local kubeconfig.

    Args:
        custom_client: Optional pre-configured API client instance (useful for unit testing).

    Returns:
        kubernetes.client.CoreV1Api instance.
    """
    if custom_client is not None:
        return custom_client

    try:
        from kubernetes import client, config
    except ImportError as e:
        raise ImportError(
            "The 'kubernetes' Python package is required for the Kubernetes sandbox backend. "
            "Please install it via 'pip install kubernetes'."
        ) from e

    global _k8s_client_initialized
    if not _k8s_client_initialized:
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration.")
        except config.ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded local kubeconfig file.")
            except config.ConfigException as ce:
                logger.warning(
                    f"Failed to load both in-cluster and kubeconfig settings: {ce}"
                )
        _k8s_client_initialized = True

    return client.CoreV1Api()
