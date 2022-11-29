import logging
from typing import Any, Tuple, TYPE_CHECKING

from ray.rllib.connectors.connector import Connector, ConnectorContext
from ray.rllib.connectors.registry import (
    get_connector,
    get_connector_cls,
)
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector

if TYPE_CHECKING:
    from ray.rllib.policy.policy import Policy
    from ray.rllib.connectors.action.pipeline import ActionConnectorPipeline
    from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
def get_agent_connectors_from_config(
    ctx: ConnectorContext,
    config: TrainerConfigDict,
) -> "AgentConnectorPipeline":
    connectors = []

    clip_reward_cls = get_connector_cls("ClipRewardAgentConnector")
    if config["clip_rewards"] is True:
        connectors.append(clip_reward_cls(ctx, sign=True))
    elif type(config["clip_rewards"]) == float:
        connectors.append(clip_reward_cls(ctx, limit=abs(config["clip_rewards"])))

    if not config["_disable_preprocessor_api"]:
        connectors.append(get_connector("ObsPreprocessorConnector", ctx))

    # Filters should be after observation preprocessing
    filter_connector = get_synced_filter_connector(
        ctx,
    )
    # Configuration option "NoFilter" results in `filter_connector==None`.
    if filter_connector:
        connectors.append(filter_connector)

    connectors.extend(
        [
            get_connector("StateBufferConnector", ctx),
            get_connector("ViewRequirementAgentConnector", ctx),
        ]
    )

    agent_connector_cls = get_connector_cls("AgentConnectorPipeline")
    return agent_connector_cls(ctx, connectors)


@PublicAPI(stability="alpha")
def get_action_connectors_from_config(
    ctx: ConnectorContext,
    config: TrainerConfigDict,
) -> "ActionConnectorPipeline":
    """Default list of action connectors to use for a new policy.

    Args:
        ctx: context used to create connectors.
        config: trainer config.
    """
    connectors = [get_connector("ConvertToNumpyConnector", ctx)]
    if config.get("normalize_actions", False):
        connectors.append(get_connector("NormalizeActionsConnector", ctx))
    if config.get("clip_actions", False):
        connectors.append(get_connector("ClipActionsConnector", ctx))
    connectors.append(get_connector("ImmutableActionsConnector", ctx))
    action_connector_cls = get_connector_cls("ActionConnectorPipeline")
    return action_connector_cls(ctx, connectors)


@PublicAPI(stability="alpha")
def create_connectors_for_policy(policy: "Policy", config: TrainerConfigDict):
    """Util to create agent and action connectors for a Policy.

    Args:
        policy: Policy instance.
        config: Trainer config dict.
    """
    ctx: ConnectorContext = ConnectorContext.from_policy(policy)

    assert policy.agent_connectors is None and policy.agent_connectors is None, (
        "Can not create connectors for a policy that already has connectors. This "
        "can happen if you add a Policy that has connectors attached to a "
        "RolloutWorker with add_policy()."
    )

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
        filter_cls = get_connector_cls("MeanStdObservationFilterAgentConnector")
        return filter_cls(ctx, clip=None)
    elif filter_specifier == "ConcurrentMeanStdFilter":
        filter_cls = get_connector_cls(
            "ConcurrentMeanStdObservationFilterAgentConnector"
        )
        return filter_cls(ctx, clip=None)
    elif filter_specifier == "NoFilter":
        return None
    else:
        raise Exception("Unknown observation_filter: " + str(filter_specifier))


@DeveloperAPI
def maybe_get_filters_for_syncing(rollout_worker, policy_id):
    # As long as the historic filter synchronization mechanism is in
    # place, we need to put filters into self.filters so that they get
    # synchronized
    filter_connectors = rollout_worker.policy_map[policy_id].agent_connectors[
        SyncedFilterAgentConnector
    ]
    # There can only be one filter at a time
    if filter_connectors:
        assert len(filter_connectors) == 1, (
            "ConnectorPipeline has multiple connectors of type "
            "SyncedFilterAgentConnector but can only have one."
        )
        rollout_worker.filters[policy_id] = filter_connectors[0].filter
