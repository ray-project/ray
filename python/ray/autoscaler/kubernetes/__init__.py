import kubernetes
from kubernetes.config.config_exception import ConfigException

try:
    kubernetes.config.load_incluster_config()
except ConfigException:
    kubernetes.config.load_kube_config()
core_api = kubernetes.client.CoreV1Api()
auth_api = kubernetes.client.RbacAuthorizationV1Api()

log_prefix = "KubernetesNodeProvider: "
