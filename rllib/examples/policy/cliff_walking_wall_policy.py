from ray.rllib.policy.policy import Policy, ViewRequirement
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import AlgorithmConfigDict, TensorStructType, TensorType
from typing import Dict, Union, List, Tuple, Optional
from ray.rllib.utils.annotations import override
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
import numpy as np
import gym


class CliffWalkingPolicy(Policy):
    """Optimal RLlib policy for the CliffWalking environment with
    epsilon-greedy exploration"""

    @override(Policy)
    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        config: AlgorithmConfigDict,
    ):
        super().__init__(observation_space, action_space, config)
        # Known optimal action dist for each of the 48 states and 4 actions
        self.optimal_dist = np.zeros((48, 4), dtype=float)
        # Starting state: go up
        self.optimal_dist[36] = (1, 0, 0, 0)
        # Cliff + Goal: never actually used, set to random
        self.optimal_dist[37:] = (0.25, 0.25, 0.25, 0.25)
        # Row 2; always go right
        self.optimal_dist[24:36] = (0, 1, 0, 0)
        # Row 0 and Row 1; go down or go right
        self.optimal_dist[0:24] = (0, 0.5, 0.5, 0)
        # Col 11; always go down, supercedes previous values
        self.optimal_dist[[11, 23, 35]] = (0, 0, 1, 0)
        assert np.allclose(self.optimal_dist.sum(-1), 1)

        # Initialize action_dist used to compute actions as optimal_dist
        self.update_epsilon(config.get("epsilon", 0.0))

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
        action_probs = self.action_dist[obs]
        return np.log(action_probs[np.arange(len(obs)), actions])

    def update_epsilon(self, epsilon: float):
        # Epsilon-Greedy action selection
        self.action_dist = self.optimal_dist * (1 - epsilon) + epsilon / 4
        assert np.allclose(self.action_dist.sum(-1), 1)

    def action_distribution_fn(
        self, model, obs_batch: TensorStructType, **kwargs
    ) -> Tuple[TensorType, type, List[TensorType]]:
        obs = np.array(obs_batch[SampleBatch.OBS], dtype=int)
        action_probs = self.action_dist[obs]
        return np.log(action_probs), TorchCategorical, None
