import os
import subprocess
from typing import Dict, List, Tuple

from ray.autoscaler._private.docker import with_docker_exec
from ray.autoscaler.command_runner import CommandRunnerInterface


class FakeDockerCommandRunner(CommandRunnerInterface):
    """Command runner for the fke docker multinode cluster.

    This command runner uses ``docker exec`` and ``docker cp`` to
    run commands and copy files, respectively.

    The regular ``DockerCommandRunner`` is made for use in SSH settings
    where Docker runs on a remote hose. In contrast, this command runner
    does not wrap the docker commands in ssh calls.
    """

    def __init__(self, docker_config, **common_args):
        self.container_name = docker_config["container_name"]
        self.docker_config = docker_config
        self.home_dir = None
        self.initialized = False
        # Optionally use 'podman' instead of 'docker'
        use_podman = docker_config.get("use_podman", False)
        self.docker_cmd = "podman" if use_podman else "docker"

    def _run_shell(self, cmd: str, timeout: int = 120) -> str:
        return subprocess.check_output(
            cmd, shell=True, timeout=timeout, encoding="utf-8"
        )

    def run(
        self,
        cmd: str = None,
        timeout: int = 120,
        exit_on_fail: bool = False,
        port_forward: List[Tuple[int, int]] = None,
        with_output: bool = False,
        environment_variables: Dict[str, object] = None,
        run_env: str = "auto",
        ssh_options_override_ssh_key: str = "",
        shutdown_after_run: bool = False,
    ) -> str:
        prefix = with_docker_exec(
            [cmd],
            container_name=self.container_name,
            with_interactive=False,
            docker_cmd=self.docker_cmd,
        )[0]
        return self._run_shell(prefix)

    def run_init(
        self, *, as_head: bool, file_mounts: Dict[str, str], sync_run_yet: bool
    ):
        pass

    def remote_shell_command_str(self):
        return "{} exec -it {} bash".format(self.docker_cmd, self.container_name)

    def run_rsync_down(self, source, target, options=None):
        docker_dir = os.path.dirname(self._docker_expand_user(source))

        self._run_shell(f"docker cp {self.container_name}:{docker_dir} {target}")

    def run_rsync_up(self, source, target, options=None):
        docker_dir = os.path.dirname(self._docker_expand_user(target))
        self.run(cmd=f"mkdir -p {docker_dir}")

        self._run_shell(f"docker cp {source} {self.container_name}:{docker_dir}")

    def _docker_expand_user(self, string, any_char=False):
        user_pos = string.find("~")
        if user_pos > -1:
            if self.home_dir is None:
                self.home_dir = self._run_shell(
                    with_docker_exec(
                        ["printenv HOME"],
                        container_name=self.container_name,
                        docker_cmd=self.docker_cmd,
                    )
                ).strip()

            if any_char:
                return string.replace("~/", self.home_dir + "/")

            elif not any_char and user_pos == 0:
                return string.replace("~", self.home_dir, 1)

        return string
