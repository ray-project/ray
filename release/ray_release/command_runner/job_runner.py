from ray_release.command_runner.command_runner import CommandRunner


class JobRunner(CommandRunner):
    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        pass
