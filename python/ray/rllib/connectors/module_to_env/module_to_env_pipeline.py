from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ModuleToEnvPipeline(ConnectorPipelineV2):
    """The connector pipeline that runs between the RLModule and the env.

    Runs its connector pieces in order to turn the RLModule's forward pass output
    into the actions sent to the env.
    """
