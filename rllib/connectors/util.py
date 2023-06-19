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
from ray.rllib.connectors.connector import Connector, ConnectorContext
from ray.rllib.connectors.registry import get_connector
from ray.rllib.connectors.agent.mean_std_filter import (
    MeanStdObservationFilterAgentConnector,
    ConcurrentMeanStdObservationFilterAgentConnector,
)
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.policy.policy import Policy

logger = logging.getLogger(__name__)


def __preprocessing_enabled(config: "AlgorithmConfig"):
    if config._disable_preprocessor_api:
        return False
    # Same conditions as in RolloutWorker.__init__.
    if config.is_atari and config.preprocessor_pref == "deepmind":
        return False
    if config.preprocessor_pref is None:
        return False
    return True


def __clip_rewards(config: "AlgorithmConfig"):
    # Same logic as in RolloutWorker.__init__.
    # We always clip rewards for Atari games.
    return config.clip_rewards or config.is_atari


@PublicAPI(stability="alpha")
def get_agent_connectors_from_config(
    ctx: ConnectorContext,
    config: "AlgorithmConfig",
) -> AgentConnectorPipeline:
    connectors = []

    clip_rewards = __clip_rewards(config)
    if clip_rewards is True:
        connectors.append(ClipRewardAgentConnector(ctx, sign=True))
    elif type(clip_rewards) == float:
        connectors.append(ClipRewardAgentConnector(ctx, limit=abs(clip_rewards)))

    if __preprocessing_enabled(config):
        connectors.append(ObsPreprocessorConnector(ctx))

    # Filters should be after observation preprocessing
    filter_connector = get_synced_filter_connector(
        ctx,
    )
    # Configuration option "NoFilter" results in `filter_connector==None`.
    if filter_connector:
        connectors.append(filter_connector)

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
    config: "AlgorithmConfig",
) -> ActionConnectorPipeline:
    """Default list of action connectors to use for a new policy.

    Args:
        ctx: context used to create connectors.
        config: The AlgorithmConfig object.
    """
    connectors = [ConvertToNumpyConnector(ctx)]
    if config.get("normalize_actions", False):
        connectors.append(NormalizeActionsConnector(ctx))
    if config.get("clip_actions", False):
        connectors.append(ClipActionsConnector(ctx))
    connectors.append(ImmutableActionsConnector(ctx))
    return ActionConnectorPipeline(ctx, connectors)


@PublicAPI(stability="alpha")
def create_connectors_for_policy(policy: "Policy", config: "AlgorithmConfig"):
    """Util to create agent and action connectors for a Policy.

    Args:
        policy: Policy instance.
        config: Algorithm config dict.
    """
    ctx: ConnectorContext = ConnectorContext.from_policy(policy)

    assert (
        policy.agent_connectors is None and policy.action_connectors is None
    ), "Can not create connectors for a policy that already has connectors."

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
    return get_connector(name, ctx, params)


# We need this filter selection mechanism temporarily to remain compatible to old API
@DeveloperAPI
def get_synced_filter_connector(ctx: ConnectorContext):
    filter_specifier = ctx.config.get("observation_filter")
    if filter_specifier == "MeanStdFilter":
        return MeanStdObservationFilterAgentConnector(ctx, clip=None)
    elif filter_specifier == "ConcurrentMeanStdFilter":
        return ConcurrentMeanStdObservationFilterAgentConnector(ctx, clip=None)
    elif filter_specifier == "NoFilter":
        return None
    else:
        raise Exception("Unknown observation_filter: " + str(filter_specifier))


@DeveloperAPI
def maybe_get_filters_for_syncing(rollout_worker, policy_id):
    # As long as the historic filter synchronization mechanism is in
    # place, we need to put filters into self.filters so that they get
    # synchronized
    policy = rollout_worker.policy_map[policy_id]
    if not policy.agent_connectors:
        return

    filter_connectors = policy.agent_connectors[SyncedFilterAgentConnector]
    # There can only be one filter at a time
    if not filter_connectors:
        return

    assert len(filter_connectors) == 1, (
        "ConnectorPipeline has multiple connectors of type "
        "SyncedFilterAgentConnector but can only have one."
    )
    rollout_worker.filters[policy_id] = filter_connectors[0].filter
