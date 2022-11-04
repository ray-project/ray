import numpy as np
from gymnasium.spaces import Box

from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import ModelWeights


class BareMetalPolicyWithCustomViewReqs(Policy):
    """
    This policy does not much with the state, but shows,
    how the training# Trajectory View API can be used to
    pass user-specific view requirements to RLlib.
    """

    def __init__(self, observation_space, action_space, model_config, *args, **kwargs):
        super().__init__(observation_space, action_space, model_config, *args, **kwargs)
        self.observation_space = observation_space
        self.action_space = action_space
        self.state_size = 10
        self.model_config = model_config or {}
        space = Box(
            low=-np.inf, high=np.inf, shape=(self.state_size,), dtype=np.float64
        )
        # Set view requirements such that the policy state is held in
        # memory for 2 environment steps.
        self.view_requirements["state_in_0"] = ViewRequirement(
            "state_out_0",
            shift="-2:-1",
            used_for_training=False,
            used_for_compute_actions=True,
            batch_repeat_value=1,
        )
        self.view_requirements["state_out_0"] = ViewRequirement(
            space=space,
            used_for_training=False,
            used_for_compute_actions=True,
            batch_repeat_value=1,
        )

    # Set the initial state. This is necessary for starting
    # the policy.
    def get_initial_state(self):
        return [np.zeros((self.state_size,), dtype=np.float32)]

    def compute_actions(
        self,
        obs_batch=None,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        info_batch=None,
        episodes=None,
        **kwargs
    ):
        # First dimension is the batch (in list), second is the number of
        # states, and third is the shift. Fourth is the size of the state.
        batch_size = state_batches[0].shape[0]
        actions = np.array([self.action_space.sample() for _ in range(batch_size)])
        new_state_batches = list(state_batches[0][0])
        return actions, [new_state_batches], {}

    def compute_actions_from_input_dict(
        self, input_dict, explore=None, timestep=None, episodes=None, **kwargs
    ):
        # Access the `infos` key here so it'll show up here always during
        # action sampling.
        infos = input_dict.get("infos")
        assert infos is not None

        # Default implementation just passes obs, prev-a/r, and states on to
        # `self.compute_actions()`.
        state_batches = [s for k, s in input_dict.items() if k[:9] == "state_in_"]
        # Make sure that two (shift="-2:-1") past states are contained in the
        # state_batch.
        assert state_batches[0].shape[1] == 2
        assert state_batches[0].shape[2] == self.state_size
        return self.compute_actions(
            input_dict[SampleBatch.OBS],
            state_batches,
            prev_action_batch=input_dict.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=input_dict.get(SampleBatch.PREV_REWARDS),
            info_batch=input_dict.get(SampleBatch.INFOS),
            explore=explore,
            timestep=timestep,
            episodes=episodes,
            **kwargs,
        )

    def learn_on_batch(self, samples):
        return

    @override(Policy)
    def get_weights(self) -> ModelWeights:
        """No weights to save."""
        return {}

    @override(Policy)
    def set_weights(self, weights: ModelWeights) -> None:
        """No weights to set."""
        pass
