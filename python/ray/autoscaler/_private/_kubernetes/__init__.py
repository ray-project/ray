import kubernetes
from kubernetes.config.config_exception import ConfigException

_configured = False
_core_api = None
_auth_api = None
_extensions_beta_api = None
_custom_objects_api = None


def _load_config():
    global _configured
    if _configured:
        return
    try:
        kubernetes.config.load_incluster_config()
    except ConfigException:
        kubernetes.config.load_kube_config()
    _configured = True


def core_api():
    global _core_api
    if _core_api is None:
        _load_config()
        _core_api = kubernetes.client.CoreV1Api()

    return _core_api


def auth_api():
    global _auth_api
    if _auth_api is None:
        _load_config()
        _auth_api = kubernetes.client.RbacAuthorizationV1Api()

    return _auth_api


def extensions_beta_api():
    global _extensions_beta_api
    if _extensions_beta_api is None:
        _load_config()
        _extensions_beta_api = kubernetes.client.ExtensionsV1beta1Api()

    return _extensions_beta_api


def custom_objects_api():
    global _custom_objects_api
    if _custom_objects_api is None:
        _load_config()
        _custom_objects_api = kubernetes.client.CustomObjectsApi()

    return _custom_objects_api


log_prefix = "KubernetesNodeProvider: "
