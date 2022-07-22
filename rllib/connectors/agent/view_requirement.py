from collections import defaultdict
from typing import Any, List

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
from ray.rllib.evaluation.collectors.simple_list_collector import _AgentCollector


@PublicAPI(stability="alpha")
class ViewRequirementAgentConnector(AgentConnector):
    """This connector does 2 things:
    1. It filters data columns based on view_requirements for training and inference.
    2. It buffers the right amount of history for computing the sample batch for
       action computation.
    The output of this connector is AgentConnectorsOut, which basically is
    a tuple of 2 things:
    {
        "for_training": {"obs": ...}
        "for_action": SampleBatch
    }
    The "for_training" dict, which contains data for the latest time slice,
    can be used to construct a complete episode by Sampler for training purpose.
    The "for_action" SampleBatch can be used to directly call the policy.
    """

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._view_requirements = ctx.view_requirements
        self._agent_data = defaultdict(lambda: defaultdict(SampleBatch))

        # a dict of env_id to a dict of agent_id to a list of agent_collector objects
        env_default = defaultdict(
            lambda: _AgentCollector(
                self._view_requirements,
                max_seq_len=ctx.config["model"]["max_seq_len"],
                intial_states=ctx.initial_states,
                disable_action_flattening=ctx.config["_disable_action_flattening"],
            )
        )
        self.agent_collectors = defaultdict(lambda: env_default)

    def reset(self, env_id: str):
        if env_id in self._agent_data:
            del self._agent_data[env_id]

    def _get_sample_batch_for_action(
        self, view_requirements, agent_batch
    ) -> SampleBatch:
        # TODO(jungong) : actually support buildling input sample batch with all the
        #  view shift requirements, etc.
        # For now, we only support last elemen (no shift).
        input_dict = {}
        for col, req in view_requirements.items():
            if not req.used_for_compute_actions:
                continue
            if col not in agent_batch:
                continue
            input_dict[col] = agent_batch[col][-1]
        return SampleBatch(input_dict, is_training=False)

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        env_id = ac_data.env_id
        agent_id = ac_data.agent_id
        # use env_id as episode_id ?
        episode_id = env_id if SampleBatch.EPS_ID not in d else d[SampleBatch.EPS_ID]

        assert env_id is not None and agent_id is not None, (
            f"ViewRequirementAgentConnector requires env_id({env_id}) "
            "and agent_id({agent_id})"
        )

        vr = self._view_requirements
        assert vr, "ViewRequirements required by ViewRequirementConnector"

        training_dict = None
        # Return full training_dict for env runner to construct episodes.
        if self._is_training:
            # Note(jungong) : we need to keep the entire input dict here.
            # A column may be used by postprocessing (GAE) even if its
            # iew_requirement.used_for_training is False.
            training_dict = d

        # Agent batch is our buffer of necessary history for computing
        # a SampleBatch for policy forward pass.
        # This is used by both training and inference.
        # agent_batch = self._agent_data[env_id][agent_id]

        # TODO: Ask @jun: what is eps_id, agent_id, env_id? and how are they different?
        agent_collector = self.agent_collectors[env_id][agent_id]

        if agent_collector.is_empty():
            agent_collector.add_init_obs(
                episode_id=episode_id,
                agent_index=agent_id,
                env_id=env_id,
                t=-1,  # not sure about this?
                init_obs=d[SampleBatch.OBS],
            )
        else:
            agent_collector.add_action_reward_next_obs(d)

        breakpoint()
        sample_batch = agent_collector.build_for_inference()

        # for col, req in vr.items():
        #     # Not used for action computation.
        #     if not req.used_for_compute_actions:
        #         continue

        #     # Create the batch of data from the different buffers.
        #     if col == SampleBatch.OBS:
        #         # NEXT_OBS from the training sample is the current OBS
        #         # to run Policy with.
        #         data_col = SampleBatch.NEXT_OBS
        #     else:
        #         data_col = req.data_col or col
        #     if data_col not in d:
        #         continue

        #     if col not in agent_batch:
        #         agent_batch[col] = []
        #     # Stack along batch dim.
        #     agent_batch[col].append(d[data_col])

        #     # Only keep the useful part of the history.
        #     h = -1
        #     if req.shift_from is not None:
        #         h = req.shift_from
        #     elif type(req.shift) == int:
        #         h = req.shift
        #     assert h <= 0, "Cannot use future data to compute action"
        #     agent_batch[col] = agent_batch[col][h:]

        # sample_batch = self._get_sample_batch_for_action(vr, agent_batch)

        return_data = AgentConnectorDataType(
            env_id, agent_id, AgentConnectorsOutput(training_dict, sample_batch)
        )
        return return_data

    def to_config(self):
        return ViewRequirementAgentConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ViewRequirementAgentConnector(ctx)


register_connector(
    ViewRequirementAgentConnector.__name__, ViewRequirementAgentConnector
)
