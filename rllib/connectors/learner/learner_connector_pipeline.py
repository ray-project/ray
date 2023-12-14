from typing import Any, List, Optional

from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.rllib.connectors.learner.default_learner_connector import (
    DefaultLearnerConnector,
)


class LearnerConnectorPipeline(ConnectorPipelineV2):
    """The superclass for any module-to-env pipelines."""

    def __init__(
        self,
        *,
        ctx: ConnectorContextV2,
        connectors: Optional[List[ConnectorV2]] = None,
        **kwargs,
    ):
        super().__init__(ctx=ctx, connectors=connectors, **kwargs)

        # Add the default final connector piece for learner pipelines:
        # Makes sure observations from episodes are in the train batch as well as
        # the correct state inputs in case the RLModule is stateful. In the latter case,
        # also takes care of the time rank and zero padding.
        if (
            len(self.connectors) == 0
            or type(self.connectors[-1]) is not DefaultLearnerConnector
        ):
            # Append default learner connector piece at the end.
            self.append(DefaultLearnerConnector(ctx=ctx))
