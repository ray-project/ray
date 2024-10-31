from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class EnvToModulePipeline(ConnectorPipelineV2):
    @override(ConnectorPipelineV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Optional[Dict[str, Any]] = None,
        episodes: List[EpisodeType],
        explore: bool,
        shared_data: Optional[dict] = None,
        **kwargs,
    ):
        # Make sure user does not necessarily send initial input into this pipeline.
        # Might just be empty and to be populated from `episodes`.
        return super().__call__(
            rl_module=rl_module,
            batch=batch if batch is not None else {},
            episodes=episodes,
            explore=explore,
            shared_data=shared_data if shared_data is not None else {},
            **kwargs,
        )
