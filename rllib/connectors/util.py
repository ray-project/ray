import logging
from typing import Any, Tuple, TYPE_CHECKING

from ray.rllib.connectors.action.clip import ClipActionsConnector
from ray.rllib.connectors.action.immutable import ImmutableActionsConnector
from ray.rllib.connectors.action.lambdas import ConvertToNumpyConnector
from ray.rllib.connectors.action.normalize import NormalizeActionsConnector
from ray.rllib.connectors.action.pipeline import ActionConnectorPipeline
from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline
from ray.rllib.connectors.agent.state_buffer import StateBufferConnector
from ray.rllib.connectors.agent.view_requirement import ViewRequirementAgentConnector
from ray.rllib.connectors.connector import Connector, ConnectorContext, get_connector
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.policy.policy import Policy

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
def get_agent_connectors_from_config(
    ctx: ConnectorContext,
    config: TrainerConfigDict,
) -> AgentConnectorPipeline:
    connectors = []

    if config["clip_rewards"] is True:
        connectors.append(ClipRewardAgentConnector(ctx, sign=True))
    elif type(config["clip_rewards"]) == float:
        connectors.append(
            ClipRewardAgentConnector(ctx, limit=abs(config["clip_rewards"]))
        )

    if not config["_disable_preprocessor_api"]:
        connectors.append(ObsPreprocessorConnector(ctx))

    connectors.extend(
        [
            StateBufferConnector(ctx),
            ViewRequirementAgentConnector(ctx),
        ]
    )

    return AgentConnectorPipeline(ctx, connectors)


@PublicAPI(stability="alpha")
def get_action_connectors_from_config(
    ctx: ConnectorContext,
    config: TrainerConfigDict,
) -> ActionConnectorPipeline:
    """Default list of action connectors to use for a new policy.

    Args:
        ctx: context used to create connectors.
        config: trainer config.
    """
    connectors = [ConvertToNumpyConnector(ctx)]
    if config.get("normalize_actions", False):
        connectors.append(NormalizeActionsConnector(ctx))
    if config.get("clip_actions", False):
        connectors.append(ClipActionsConnector(ctx))
    connectors.append(ImmutableActionsConnector(ctx))
    return ActionConnectorPipeline(ctx, connectors)


@PublicAPI(stability="alpha")
def create_connectors_for_policy(policy: "Policy", config: TrainerConfigDict):
    """Util to create agent and action connectors for a Policy.

    Args:
        policy: Policy instance.
        config: Trainer config dict.
    """
    ctx: ConnectorContext = ConnectorContext.from_policy(policy)

    policy.agent_connectors = get_agent_connectors_from_config(ctx, config)
    policy.action_connectors = get_action_connectors_from_config(ctx, config)

    logger.info("Using connectors:")
    logger.info(policy.agent_connectors.__str__(indentation=4))
    logger.info(policy.action_connectors.__str__(indentation=4))


@PublicAPI(stability="alpha")
def restore_connectors_for_policy(
    policy: "Policy", connector_config: Tuple[str, Tuple[Any]]
) -> Connector:
    """Util to create connector for a Policy based on serialized config.

    Args:
        policy: Policy instance.
        connector_config: Serialized connector config.
    """
    ctx: ConnectorContext = ConnectorContext.from_policy(policy)
    name, params = connector_config
    return get_connector(ctx, name, params)
