from collections import defaultdict
from typing import Any

from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
    register_connector,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import (
    AgentConnectorDataType,
    AgentConnectorsOutput,
)
from ray.util.annotations import PublicAPI
from ray.rllib.evaluation.collectors.agent_collector import AgentCollector


@PublicAPI(stability="alpha")
class ViewRequirementAgentConnector(AgentConnector):
    """This connector does 2 things:
    1. It filters data columns based on view_requirements for training and inference.
    2. It buffers the right amount of history for computing the sample batch for
       action computation.
    The output of this connector is AgentConnectorsOut, which basically is
    a tuple of 2 things:
    {
        "raw_dict": {"obs": ...}
        "sample_batch": SampleBatch
    }
    raw_dict, which contains raw data for the latest time slice,
    can be used to construct a complete episode by Sampler for training purpose.
    The "for_action" SampleBatch can be used to directly call the policy.
    """

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._view_requirements = ctx.view_requirements

        # a dict of env_id to a dict of agent_id to a list of agent_collector objects
        self.agent_collectors = defaultdict(
            lambda: defaultdict(
                lambda: AgentCollector(
                    self._view_requirements,
                    max_seq_len=ctx.config["model"]["max_seq_len"],
                    intial_states=ctx.initial_states,
                    disable_action_flattening=ctx.config.get(
                        "_disable_action_flattening", False
                    ),
                    is_policy_recurrent=ctx.is_policy_recurrent,
                    # Note(jungong): We only leverage AgentCollector for building sample
                    # batches for computing actions.
                    # So regardless of whether this ViewRequirement connector is in
                    # training or inference mode, we should tell these AgentCollectors
                    # to behave in inference mode, so they don't accumulate episode data
                    # that is not useful for inference.
                    is_training=False,
                )
            )
        )

    def reset(self, env_id: str):
        if env_id in self.agent_collectors:
            del self.agent_collectors[env_id]

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        env_id = ac_data.env_id
        agent_id = ac_data.agent_id
        # TODO: we don't keep episode_id around so use env_id as episode_id ?
        episode_id = env_id if SampleBatch.EPS_ID not in d else d[SampleBatch.EPS_ID]

        assert env_id is not None and agent_id is not None, (
            f"ViewRequirementAgentConnector requires env_id({env_id}) "
            "and agent_id({agent_id})"
        )

        vr = self._view_requirements
        assert vr, "ViewRequirements required by ViewRequirementAgentConnector"

        # Note(jungong) : we need to keep the entire input dict here.
        # A column may be used by postprocessing (GAE) even if its
        # iew_requirement.used_for_training is False.
        training_dict = d

        agent_collector = self.agent_collectors[env_id][agent_id]

        if SampleBatch.NEXT_OBS not in d:
            raise ValueError(f"connector data {d} should contain next_obs.")

        if agent_collector.is_empty():
            agent_collector.add_init_obs(
                episode_id=episode_id,
                agent_index=agent_id,
                env_id=env_id,
                init_obs=d[SampleBatch.NEXT_OBS],
            )
        else:
            agent_collector.add_action_reward_next_obs(d)
        sample_batch = agent_collector.build_for_inference()

        return_data = AgentConnectorDataType(
            env_id, agent_id, AgentConnectorsOutput(training_dict, sample_batch)
        )
        return return_data

    def to_state(self):
        return ViewRequirementAgentConnector.__name__, None

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return ViewRequirementAgentConnector(ctx)


register_connector(
    ViewRequirementAgentConnector.__name__, ViewRequirementAgentConnector
)
