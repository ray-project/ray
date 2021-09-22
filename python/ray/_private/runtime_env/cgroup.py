import logging
from typing import Optional

from ray._private.runtime_env import RuntimeEnvContext

default_logger = logging.getLogger(__name__)


class CgroupManager:
    def __init__(self):
        pass

    def setup(self,
              allocated_resource: dict,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):

        # TODO (chenk008): create allocated resource cgroup
        logger.info(f"Setting up cgroup with resource: {allocated_resource}")
