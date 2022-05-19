from typing import Dict
from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator, OffPolicyEstimate
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType
from typing import List
from ray.rllib.offline.estimators.qreg_tf_model import QRegTFModel
from ray.rllib.offline.estimators.qreg_torch_model import QRegTorchModel
from gym.spaces import Discrete
import numpy as np

def k_fold_cv(episodes: List[SampleBatchType], k: int):
    n_episodes = len(episodes)
    if n_episodes < k:
        yield [], episodes
    n_fold = n_episodes // k
    for i in range(k):
        train_episodes = episodes[:i*n_fold] + episodes[(i + 1) * n_fold:]
        if i != k - 1:
            test_episodes = episodes[i * n_fold:(i + 1) * n_fold]
        else:
            # Append remaining episodes onto the last test_episodes
            test_episodes = episodes[i * n_fold:]
        yield train_episodes, test_episodes

class DirectMethod(OffPolicyEstimator):
    """The Direct Method (Q-Reg) estimator.

    config: {
        model: ModelConfigDict,
        k: k-fold cross validation for training model and evaluating OPE
    }

    Q-Reg estimator described in https://arxiv.org/pdf/1511.03722.pdf,
    https://arxiv.org/pdf/1911.06854.pdf"""
    @override(OffPolicyEstimator)
    def __init__(self, policy: Policy, gamma: float, config: Dict):
        super().__init__(policy, gamma)
        assert isinstance(
            policy.action_space, 
            Discrete), "DM Estimator only supports discrete action spaces!"
        model_cls = QRegTorchModel if policy.framework == "torch" else QRegTFModel
        self.model = model_cls(
            observation_space = policy.observation_space,
            action_space = policy.action_space,
            config=config,
        )

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> OffPolicyEstimate:
        self.check_can_estimate_for(batch)
        estimates = []
        for episode in batch.split_by_episode():
            rewards = episode["rewards"]
            V_prev, V_DM = 0.0, 0.0
            for t in range(episode.count):
                V_prev += rewards[t] * self.gamma ** t

            init_batch = episode[0]
            q_values = self.model.estimate_q([init_batch[SampleBatch.OBS]])
            for a in range(self.policy.action_space.n):
                init_batch[SampleBatch.ACTIONS] = np.array([a], dtype=np.float32)
                V_DM += q_values[a] * self.action_prob(init_batch)

            estimates.append(OffPolicyEstimate(
                "direct_method",
                {
                    "V_prev": V_prev,
                    "V_DM": V_DM,
                    "V_gain_est": V_DM / max(1e-8, V_prev),
                },
            ))
        return estimates
    
    @override(OffPolicyEstimator)
    def train(self, batch: SampleBatchType, reset_weights=True) -> None:
        if reset_weights:
            self.model.reset()

        batch_obs = []
        batch_actions = []
        batch_ps = []
        batch_returns = []
        batch_discounts = []
        for episode in batch.split_by_episode():
            rewards, old_prob = episode["rewards"], episode["action_prob"]
            new_prob = self.action_prob(episode)
            # calculate importance ratios and returns
            p = np.zeros_like(rewards)
            returns = np.zeros_like(rewards)
            discounts = np.zeros_like(rewards)
            for t in range(episode.count):
                discounts[t] = self.gamma ** t
                if t == 0:
                    pt_prev = 1.0
                    pt_next = 1.0
                else:
                    pt_prev = p[t - 1]
                    pt_next = pt_next * new_prob[-t] / old_prob[-t]
                p[t] = pt_prev * new_prob[t] / old_prob[t]
                # Trick: returns[0] is already 0 when t = T
                returns[-t-1] = rewards[-t-1] + self.discount * pt_next * returns[-t]
            batch_obs.append(episode[SampleBatch.OBS])
            batch_actions.append(episode[SampleBatch.ACTIONS])
            batch_ps.append(p)
            batch_returns.append(returns)
            batch_discounts.append(discounts)

        self.model.train_q(
            batch_obs,
            batch_actions,
            batch_ps,
            batch_returns,
            batch_discounts,
        )