from typing import List, Optional

from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.windows_container import WindowsContainer


class WindowsTesterContainer(TesterContainer, WindowsContainer):
    def __init__(
        self,
        docker_tag: str,
        shard_count: int = 1,
        test_envs: Optional[List[str]] = None,
        shard_ids: Optional[List[int]] = None,
        network: Optional[str] = None,
        skip_ray_installation: bool = False,
    ) -> None:
        WindowsContainer.__init__(self, docker_tag, envs=test_envs)
        TesterContainer.__init__(
            self,
            shard_count,
            gpus=0,  # We don't support GPU tests on Windows yet.
            bazel_log_dir="C:\\msys64\\tmp\\bazel_event_logs",
            network=network,
            test_envs=test_envs,
            shard_ids=shard_ids,
            skip_ray_installation=skip_ray_installation,
            build_type=None,
        )
