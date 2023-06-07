import gymnasium as gym
from typing import Dict, Union, List, Tuple, Optional
import numpy as np

from ray.rllib.policy.policy import Policy, ViewRequirement
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.utils.typing import AlgorithmConfigDict, TensorStructType, TensorType
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import update_global_seed_if_necessary


class CliffWalkingWallPolicy(Policy):
    """Optimal RLlib policy for the CliffWalkingWallEnv environment, defined in
    ray/rllib/examples/env/cliff_walking_wall_env.py, with epsilon-greedy exploration.

    The policy takes a random action with probability epsilon, specified
    by `config["epsilon"]`, and the optimal action with probability  1 - epsilon.
    """

    @override(Policy)
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        config: AlgorithmConfigDict,
    ):
        update_global_seed_if_necessary(seed=config.get("seed"))
        super().__init__(observation_space, action_space, config)

        # Known optimal action dist for each of the 48 states and 4 actions
        self.action_dist = np.zeros((48, 4), dtype=float)
        # Starting state: go up
        self.action_dist[36] = (1, 0, 0, 0)
        # Cliff + Goal: never actually used, set to random
        self.action_dist[37:] = (0.25, 0.25, 0.25, 0.25)
        # Row 2; always go right
        self.action_dist[24:36] = (0, 1, 0, 0)
        # Row 0 and Row 1; go down or go right
        self.action_dist[0:24] = (0, 0.5, 0.5, 0)
        # Col 11; always go down, supercedes previous values
        self.action_dist[[11, 23, 35]] = (0, 0, 1, 0)
        assert np.allclose(self.action_dist.sum(-1), 1)

        # Epsilon-Greedy action selection
        epsilon = config.get("epsilon", 0.0)
        self.action_dist = self.action_dist * (1 - epsilon) + epsilon / 4
        assert np.allclose(self.action_dist.sum(-1), 1)

        # Attributes required for RLlib; note that while CliffWalkingWallPolicy
        # inherits from Policy, it actually implements TorchPolicyV2.
        self.view_requirements[SampleBatch.ACTION_PROB] = ViewRequirement()
        self.device = "cpu"
        self.model = None
        self.dist_class = TorchCategorical

    @override(Policy)
    def compute_actions(
        self,
        obs_batch: Union[List[TensorStructType], TensorStructType],
        state_batches: Optional[List[TensorType]] = None,
        **kwargs,
    ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:
        obs = np.array(obs_batch, dtype=int)
        action_probs = self.action_dist[obs]
        actions = np.zeros(len(obs), dtype=int)
        for i in range(len(obs)):
            actions[i] = np.random.choice(4, p=action_probs[i])
        return (
            actions,
            [],
            {SampleBatch.ACTION_PROB: action_probs[np.arange(len(obs)), actions]},
        )

    @override(Policy)
    def compute_log_likelihoods(
        self,
        actions: Union[List[TensorType], TensorType],
        obs_batch: Union[List[TensorType], TensorType],
        **kwargs,
    ) -> TensorType:
        obs = np.array(obs_batch, dtype=int)
        actions = np.array(actions, dtype=int)
        # Compute action probs for all possible actions
        action_probs = self.action_dist[obs]
        # Take the action_probs corresponding to the specified actions
        action_probs = action_probs[np.arange(len(obs)), actions]
        # Ignore RuntimeWarning thrown by np.log(0) if action_probs is 0
        with np.errstate(divide="ignore"):
            return np.log(action_probs)

    def action_distribution_fn(
        self, model, obs_batch: TensorStructType, **kwargs
    ) -> Tuple[TensorType, type, List[TensorType]]:
        obs = np.array(obs_batch[SampleBatch.OBS], dtype=int)
        action_probs = self.action_dist[obs]
        # Ignore RuntimeWarning thrown by np.log(0) if action_probs is 0
        with np.errstate(divide="ignore"):
            return np.log(action_probs), TorchCategorical, None
