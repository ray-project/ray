import os
from ray.autoscaler._private.command_runner import KubernetesCommandRunner


class StaroidCommandRunner(KubernetesCommandRunner):
    def __init__(
        self,
        log_prefix,
        namespace,
        node_id,
        auth_config,
        process_runner,
        kube_api_server=None,
    ):

        super(StaroidCommandRunner, self).__init__(
            log_prefix, namespace, node_id, auth_config, process_runner
        )

        if kube_api_server is not None:
            self.kubectl.extend(["--server", kube_api_server])
            os.environ["KUBE_API_SERVER"] = kube_api_server
