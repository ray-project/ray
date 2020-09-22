import os
from ray.autoscaler._private.command_runner import KubernetesCommandRunner


class StaroidCommandRunner(KubernetesCommandRunner):
    def __init__(self,
                 log_prefix,
                 namespace,
                 node_id,
                 auth_config,
                 process_runner,
                 kube_api_server=None):

        super(StaroidCommandRunner, self).__init__(
            log_prefix, namespace, node_id, auth_config, process_runner)

        if kube_api_server is not None:
            self.kubectl.extend(["--server", kube_api_server])
            os.environ["KUBE_API_SERVER"] = kube_api_server

    def _rewrite_target_home_dir(self, target):
        # Staroid forces containers to run non-root permission. Ray docker
        # image does not have a support for non-root user at the moment.
        # Use /tmp/ray as a home directory until docker image supports
        # non-root user.

        if target.startswith("~/"):
            return "/home/ray" + target[1:]
        return target

    def run_rsync_up(self, source, target, options=None):
        target = self._rewrite_target_home_dir(target)
        super().run_rsync_up(source, target, options)

    def run_rsync_down(self, source, target, options=None):
        target = self._rewrite_target_home_dir(target)
        super().run_rsync_down(source, target, options)
