from release.ray_release.command_runner.command_runner import CommandRunner


class SDKRunner(CommandRunner):
    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        pass
