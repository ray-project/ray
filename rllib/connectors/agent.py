from collections import defaultdict
import gym
import numpy as np
import tree  # dm_tree
from typing import Any, List

from ray.rllib.connectors.connector import (
    Connector,
    ConnectorContext,
    ConnectorPipeline,
    AgentConnector,
    PolicyOutputType,
    register_connector,
    get_connector,
)
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    AgentConnectorsOutput,
    TrainerConfigDict,
)


@DeveloperAPI
class EnvToPerAgentDataConnector(AgentConnector):
    """Xonverts per environment multi-agent obs into per agent SampleBatches."""

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)
        self._view_requirements = ctx.view_requirements

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        if ac_data.agent_id:
            # data is already for a single agent.
            return [ac_data]

        assert isinstance(ac_data.data, (tuple, list)) and len(ac_data.data) == 5, (
            "EnvToPerAgentDataConnector expects a tuple of "
            + "(obs, rewards, dones, infos, episode_infos)."
        )
        # episode_infos contains additional training related data bits
        # for each agent, such as SampleBatch.T, SampleBatch.AGENT_INDEX,
        # SampleBatch.ACTIONS, SampleBatch.DONES (if hitting horizon),
        # and is usually empty in inference mode.
        obs, rewards, dones, infos, training_episode_infos = ac_data.data
        for var, name in zip(
            (obs, rewards, dones, infos, training_episode_infos),
            ("obs", "rewards", "dones", "infos", "training_episode_infos"),
        ):
            assert isinstance(var, dict), (
                f"EnvToPerAgentDataConnector expects {name} "
                + "to be a MultiAgentDict."
            )

        env_id = ac_data.env_id
        per_agent_data = []
        for agent_id, obs in obs.items():
            input_dict = {
                SampleBatch.ENV_ID: env_id,
                SampleBatch.REWARDS: rewards[agent_id],
                # SampleBatch.DONES may be overridden by data from
                # training_episode_infos next.
                SampleBatch.DONES: dones[agent_id],
                SampleBatch.NEXT_OBS: obs,
            }
            if SampleBatch.INFOS in self._view_requirements:
                input_dict[SampleBatch.INFOS] = infos[agent_id]
            if agent_id in training_episode_infos:
                input_dict.update(training_episode_infos[agent_id])

            per_agent_data.append(AgentConnectorDataType(env_id, agent_id, input_dict))

        return per_agent_data

    def to_config(self):
        return EnvToPerAgentDataConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return EnvToPerAgentDataConnector(ctx)


register_connector(EnvToPerAgentDataConnector.__name__, EnvToPerAgentDataConnector)


@DeveloperAPI
class ObsPreprocessorConnector(AgentConnector):
    """A connector that wraps around existing RLlib observation preprocessors.

    This includes:
    - OneHotPreprocessor for Discrete and Multi-Discrete spaces.
    - GenericPixelPreprocessor and AtariRamPreprocessor for Atari spaces.
    - TupleFlatteningPreprocessor and DictFlatteningPreprocessor for flattening
      arbitrary nested input observations.
    - RepeatedValuesPreprocessor for padding observations from RLlib Repeated
      observation space.
    """

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._preprocessor = get_preprocessor(ctx.observation_space)(
            ctx.observation_space, ctx.config.get("model", {})
        )

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        if SampleBatch.OBS in d:
            d[SampleBatch.OBS] = self._preprocessor.transform(d[SampleBatch.OBS])
        if SampleBatch.NEXT_OBS in d:
            d[SampleBatch.NEXT_OBS] = self._preprocessor.transform(
                d[SampleBatch.NEXT_OBS]
            )

        return [ac_data]

    def to_config(self):
        return ObsPreprocessorConnector.__name__, {}

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ObsPreprocessorConnector(ctx, **params)


register_connector(ObsPreprocessorConnector.__name__, ObsPreprocessorConnector)


@DeveloperAPI
class FlattenDataConnector(AgentConnector):
    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        flattened = {}
        for k, v in d.items():
            if k in [SampleBatch.INFOS, SampleBatch.ACTIONS] or k.startswith(
                "state_out_"
            ):
                # Do not flatten infos, actions, and state_out_ columns.
                flattened[k] = v
                continue
            if v is None:
                # Keep the same column shape.
                flattened[k] = None
                continue
            flattened[k] = np.array(tree.flatten(v))

        return [AgentConnectorDataType(ac_data.env_id, ac_data.agent_id, flattened)]

    def to_config(self):
        return FlattenDataConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return FlattenDataConnector(ctx)


register_connector(FlattenDataConnector.__name__, FlattenDataConnector)


@DeveloperAPI
class ClipRewardConnector(AgentConnector):
    def __init__(self, ctx: ConnectorContext, sign=False, limit=None):
        super().__init__(ctx)
        assert (
            not sign or not limit
        ), "should not enable both sign and limit reward clipping."
        self.sign = sign
        self.limit = limit

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        assert SampleBatch.REWARDS in d, "input data does not have reward column."
        if self.sign:
            d[SampleBatch.REWARDS] = np.sign(d[SampleBatch.REWARDS])
        elif self.limit:
            d[SampleBatch.REWARDS] = np.clip(
                d[SampleBatch.REWARDS],
                a_min=-self.limit,
                a_max=self.limit,
            )
        return [ac_data]

    def to_config(self):
        return ClipRewardConnector.__name__, {"sign": self.sign, "limit": self.limit}

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ClipRewardConnector(ctx, **params)


register_connector(ClipRewardConnector.__name__, ClipRewardConnector)


@DeveloperAPI
class _AgentState(object):
    def __init__(self):
        self.t = 0
        self.action = None
        self.states = None


@DeveloperAPI
class StateBufferConnector(AgentConnector):
    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._initial_states = ctx.initial_states
        self._action_space_struct = get_base_struct_from_space(ctx.action_space)
        self._states = defaultdict(lambda: defaultdict(_AgentState))

    def reset(self, env_id: str):
        del self._states[env_id]

    def on_policy_output(self, env_id: str, agent_id: str, output: PolicyOutputType):
        # Buffer latest output states for next input __call__.
        action, states, _ = output
        agent_state = self._states[env_id][agent_id]
        agent_state.action = convert_to_numpy(action)
        agent_state.states = convert_to_numpy(states)

    def __call__(
        self, ctx: ConnectorContext, ac_data: AgentConnectorDataType
    ) -> List[AgentConnectorDataType]:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        env_id = ac_data.env_id
        agent_id = ac_data.agent_id
        assert env_id and agent_id, "StateBufferConnector requires env_id and agent_id"

        agent_state = self._states[env_id][agent_id]

        d.update(
            {
                SampleBatch.T: agent_state.t,
                SampleBatch.ENV_ID: env_id,
            }
        )

        if agent_state.states is not None:
            states = agent_state.states
        else:
            states = self._initial_states
        for i, v in enumerate(states):
            d["state_out_{}".format(i)] = v

        if agent_state.action is not None:
            d[SampleBatch.ACTIONS] = agent_state.action  # Last action
        else:
            # Default zero action.
            d[SampleBatch.ACTIONS] = tree.map_structure(
                lambda s: np.zeros_like(s.sample(), s.dtype)
                if hasattr(s, "dtype")
                else np.zeros_like(s.sample()),
                self._action_space_struct,
            )

        agent_state.t += 1

        return [ac_data]

    def to_config(self):
        return StateBufferConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return StateBufferConnector(ctx)


register_connector(StateBufferConnector.__name__, StateBufferConnector)


@DeveloperAPI
class ViewRequirementConnector(AgentConnector):
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

    def reset(self, env_id: str):
        if env_id in self._agent_data:
            del self._agent_data[env_id]

    def _get_sample_batch_for_action(
        self, view_requirements, agent_batch
    ) -> SampleBatch:
        # TODO(jungong) : actually support buildling input sample batch with all the
        #  view shift requirements, etc.
        # For now, we use some simple logics for demo purpose.
        input_batch = SampleBatch()
        for k, v in view_requirements.items():
            if not v.used_for_compute_actions:
                continue
            data_col = v.data_col or k
            if data_col not in agent_batch:
                continue
            input_batch[k] = agent_batch[data_col][-1:]
        input_batch.count = 1
        return input_batch

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        env_id = ac_data.env_id
        agent_id = ac_data.agent_id
        assert env_id and agent_id, "StateBufferConnector requires env_id and agent_id"

        vr = self._view_requirements
        assert vr, "ViewRequirements required by ViewRequirementConnector"

        training_dict = {}
        # We construct a proper per-timeslice dict in training mode,
        # for Sampler to construct a complete episode for back propagation.
        if self.is_training:
            # Filter columns that are not needed for traing.
            for col, req in vr.items():
                # Not used for training.
                if not req.used_for_training:
                    continue

                # Create the batch of data from the different buffers.
                data_col = req.data_col or col
                if data_col not in d:
                    continue

                training_dict[data_col] = d[data_col]

        # Agent batch is our buffer of necessary history for computing
        # a SampleBatch for policy forward pass.
        # This is used by both training and inference.
        agent_batch = self._agent_data[env_id][agent_id]
        for col, req in vr.items():
            # Not used for action computation.
            if not req.used_for_compute_actions:
                continue

            # Create the batch of data from the different buffers.
            data_col = req.data_col or col
            if data_col not in d:
                continue

            # Add batch dim to this data_col.
            d_col = np.expand_dims(d[data_col], axis=0)

            if col in agent_batch:
                # Stack along batch dim.
                agent_batch[data_col] = np.vstack((agent_batch[data_col], d_col))
            else:
                agent_batch[data_col] = d_col
            # Only keep the useful part of the history.
            h = req.shift_from if req.shift_from else -1
            assert h <= 0, "Can use future data to compute action"
            agent_batch[data_col] = agent_batch[data_col][h:]

        sample_batch = self._get_sample_batch_for_action(vr, agent_batch)

        return_data = AgentConnectorDataType(
            env_id, agent_id, AgentConnectorsOutput(training_dict, sample_batch)
        )
        return return_data

    def to_config(self):
        return ViewRequirementConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ViewRequirementConnector(ctx)


register_connector(ViewRequirementConnector.__name__, ViewRequirementConnector)


@DeveloperAPI
class AgentConnectorPipeline(AgentConnector, ConnectorPipeline):
    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        super().__init__(ctx)
        self.connectors = connectors

    def is_training(self, is_training: bool):
        self.is_training = is_training
        for c in self.connectors:
            c.is_training(is_training)

    def reset(self, env_id: str):
        for c in self.connectors:
            c.reset(env_id)

    def on_policy_output(self, output: ActionConnectorDataType):
        for c in self.connectors:
            c.on_policy_output(output)

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        ret = [ac_data]
        for c in self.connectors:
            # Run the list of input data through the next agent connect,
            # and collect the list of output data.
            new_ret = []
            for d in ret:
                new_ret += c(d)
            ret = new_ret
        return ret

    def to_config(self):
        return AgentConnectorPipeline.__name__, [c.to_config() for c in self.connectors]

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        assert (
            type(params) == list
        ), "AgentConnectorPipeline takes a list of connector params."
        connectors = [get_connector(ctx, name, subparams) for name, subparams in params]
        return AgentConnectorPipeline(ctx, connectors)


register_connector(AgentConnectorPipeline.__name__, AgentConnectorPipeline)


# TODO(jungong) : finish this.
@DeveloperAPI
def get_agent_connectors_from_config(
    config: TrainerConfigDict, obs_space: gym.spaces.Space
) -> AgentConnectorPipeline:
    connectors = [FlattenDataConnector()]

    if config["clip_rewards"] is True:
        connectors.append(ClipRewardConnector(sign=True))
    elif type(config["clip_rewards"]) == float:
        connectors.append(ClipRewardConnector(limit=abs(config["clip_rewards"])))

    return AgentConnectorPipeline(connectors)
