from typing import Any, List, Optional

from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.connectors.module_to_env.default_module_to_env import DefaultModuleToEnv


class ModuleToEnvPipeline(ConnectorPipelineV2):
    """The superclass for any module-to-env pipelines."""

    def __init__(
        self,
        *,
        ctx: ConnectorContextV2,
        connectors: Optional[List[ConnectorV2]] = None,
        **kwargs,
    ):
        super().__init__(ctx=ctx, connectors=connectors, **kwargs)

        # Add the default final connector piece for env-to-module pipelines:
        # Sampling actions from action_dist_inputs and add them to input, iff this has
        # not happened in any connector piece in this pipeline before.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultModuleToEnv
        ):
            self.append(DefaultModuleToEnv(ctx=ctx))
