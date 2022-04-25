import gym
from typing import Optional, List, Dict

from ray.rllib.agents.sac.sac_torch_model import (
    SACTorchModel,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils import override, force_list
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelConfigDict, TensorType

torch, _ = try_import_torch()


class RNNSACTorchModel(SACTorchModel):
    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: Optional[int],
        model_config: ModelConfigDict,
        name: str,
        policy_model_config: ModelConfigDict = None,
        q_model_config: ModelConfigDict = None,
        twin_q: bool = False,
        initial_alpha: float = 1.0,
        target_entropy: Optional[float] = None,
    ):
        super().__init__(
            obs_space=obs_space,
            action_space=action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            name=name,
            policy_model_config=policy_model_config,
            q_model_config=q_model_config,
            twin_q=twin_q,
            initial_alpha=initial_alpha,
            target_entropy=target_entropy,
        )
        self.use_prev_action = (
            model_config["lstm_use_prev_action"]
            or policy_model_config["lstm_use_prev_action"]
            or q_model_config["lstm_use_prev_action"]
        )

        self.use_prev_reward = (
            model_config["lstm_use_prev_reward"]
            or policy_model_config["lstm_use_prev_reward"]
            or q_model_config["lstm_use_prev_reward"]
        )
        if self.use_prev_action:
            self.view_requirements[SampleBatch.PREV_ACTIONS] = ViewRequirement(
                SampleBatch.ACTIONS, space=self.action_space, shift=-1
            )
        if self.use_prev_reward:
            self.view_requirements[SampleBatch.PREV_REWARDS] = ViewRequirement(
                SampleBatch.REWARDS, shift=-1
            )

    @override(SACTorchModel)
    def forward(
        self,
        input_dict: Dict[str, TensorType],
        state: List[TensorType],
        seq_lens: TensorType,
    ) -> (TensorType, List[TensorType]):
        """The common (Q-net and policy-net) forward pass.

        NOTE: It is not(!) recommended to override this method as it would
        introduce a shared pre-network, which would be updated by both
        actor- and critic optimizers.

        For rnn support remove input_dict filter and pass state and seq_lens
        """
        model_out = {"obs": input_dict[SampleBatch.OBS]}

        if self.use_prev_action:
            model_out["prev_actions"] = input_dict[SampleBatch.PREV_ACTIONS]
        if self.use_prev_reward:
            model_out["prev_rewards"] = input_dict[SampleBatch.PREV_REWARDS]

        return model_out, state

    @override(SACTorchModel)
    def _get_q_value(
        self,
        model_out: TensorType,
        actions,
        net,
        state_in: List[TensorType],
        seq_lens: TensorType,
    ) -> (TensorType, List[TensorType]):
        # Continuous case -> concat actions to model_out.
        if (
            actions is not None
            and not model_out.get("obs_and_action_concatenated") is True
        ):
            # Make sure that, if we call this method twice with the same
            # input, we don't concatenate twice
            model_out["obs_and_action_concatenated"] = True

            if self.concat_obs_and_actions:
                model_out[SampleBatch.OBS] = torch.cat(
                    [model_out[SampleBatch.OBS], actions], dim=-1
                )
            else:
                model_out[SampleBatch.OBS] = force_list(model_out[SampleBatch.OBS]) + [
                    actions
                ]

        # Switch on training mode (when getting Q-values, we are usually in
        # training).
        model_out["is_training"] = True

        out, state_out = net(model_out, state_in, seq_lens)
        return out, state_out

    @override(SACTorchModel)
    def get_q_values(
        self,
        model_out: TensorType,
        state_in: List[TensorType],
        seq_lens: TensorType,
        actions: Optional[TensorType] = None,
    ) -> TensorType:
        return self._get_q_value(model_out, actions, self.q_net, state_in, seq_lens)

    @override(SACTorchModel)
    def get_twin_q_values(
        self,
        model_out: TensorType,
        state_in: List[TensorType],
        seq_lens: TensorType,
        actions: Optional[TensorType] = None,
    ) -> TensorType:
        return self._get_q_value(
            model_out, actions, self.twin_q_net, state_in, seq_lens
        )

    @override(ModelV2)
    def get_initial_state(self):
        policy_initial_state = self.action_model.get_initial_state()
        q_initial_state = self.q_net.get_initial_state()
        if self.twin_q_net:
            q_initial_state *= 2
        return policy_initial_state + q_initial_state

    def select_state(
        self, state_batch: List[TensorType], net: List[str]
    ) -> Dict[str, List[TensorType]]:
        assert all(
            n in ["policy", "q", "twin_q"] for n in net
        ), "Selected state must be either for policy, q or twin_q network"
        policy_state_len = len(self.action_model.get_initial_state())
        q_state_len = len(self.q_net.get_initial_state())

        selected_state = {}
        for n in net:
            if n == "policy":
                selected_state[n] = state_batch[:policy_state_len]
            elif n == "q":
                selected_state[n] = state_batch[
                    policy_state_len : policy_state_len + q_state_len
                ]
            elif n == "twin_q":
                if self.twin_q_net:
                    selected_state[n] = state_batch[policy_state_len + q_state_len :]
                else:
                    selected_state[n] = []
        return selected_state
