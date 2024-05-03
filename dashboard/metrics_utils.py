import os
import psutil
from ray.dashboard import k8s_utils

# Are we in a K8s pod?
IN_KUBERNETES_POD = "KUBERNETES_SERVICE_HOST" in os.environ


def get_cpu_percent(in_k8s: bool):
    if in_k8s:
        return k8s_utils.cpu_percent()
    else:
        return psutil.cpu_percent()
