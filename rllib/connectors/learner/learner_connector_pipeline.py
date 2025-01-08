from typing import List, Optional
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_CONNECTOR_SUM_EPISODES_LENGTH_IN,
    LEARNER_CONNECTOR_SUM_EPISODES_LENGTH_OUT,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class LearnerConnectorPipeline(ConnectorPipelineV2):
    @override(ConnectorPipelineV2)
    def __call__(
        self,
        *,
        episodes: List[EpisodeType],
        metrics: Optional[MetricsLogger] = None,
        **kwargs,
    ):
        # Log the sum of lengths of all episodes incoming.
        if metrics:
            metrics.log_value(
                (ALL_MODULES, LEARNER_CONNECTOR_SUM_EPISODES_LENGTH_IN),
                sum(map(len, episodes)),
            )

        # Make sure user does not necessarily send initial input into this pipeline.
        # Might just be empty and to be populated from `episodes`.
        ret = super().__call__(
            episodes=episodes,
            metrics=metrics,
            prefix_key=(ALL_MODULES,),
            **kwargs,
        )

        # Log the sum of lengths of all episodes outgoing.
        if metrics:
            metrics.log_value(
                (ALL_MODULES, LEARNER_CONNECTOR_SUM_EPISODES_LENGTH_OUT),
                sum(map(len, episodes)),
            )

        return ret
