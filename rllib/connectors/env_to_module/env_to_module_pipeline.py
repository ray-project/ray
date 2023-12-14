from typing import Any, List, Optional

from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.connectors.env_to_module.default_env_to_module import DefaultEnvToModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class EnvToModulePipeline(ConnectorPipelineV2):
    def __init__(
        self,
        *,
        ctx: ConnectorContextV2,
        connectors: Optional[List[ConnectorV2]] = None,
        **kwargs,
    ):
        super().__init__(ctx=ctx, connectors=connectors, **kwargs)
        # Add the default final connector piece for env-to-module pipelines:
        # Extracting last obs from episodes and add them to input, iff this has not
        # happened in any connector piece in this pipeline before.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultEnvToModule
        ):
            self.append(DefaultEnvToModule(ctx=ctx))

    @override(ConnectorPipelineV2)
    def __call__(
        self,
        *,
        input_: Optional[Any] = None,
        episodes: List[EpisodeType],
        ctx: ConnectorContextV2,
        **kwargs,
    ) -> Any:

        return super().__call__(
            # Make sure user does not have to send initial `input_` into this env-to-module
            # pipeline. This would be the expected behavior b/c after calling the env,
            # we don't have any data dict yet, only a list of Episode objects.
            input_=input_ or {},
            episodes=episodes,
            ctx=ctx,
            **kwargs,
        )
