from typing import Dict
from ray.rllib.offline.estimators.off_policy_estimator import (
    OffPolicyEstimator,
    OffPolicyEstimate,
)
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.offline.estimators.qreg_tf_model import QRegTFModel
from ray.rllib.offline.estimators.qreg_torch_model import QRegTorchModel
from gym.spaces import Discrete
import numpy as np


def k_fold_cv(batch: SampleBatchType, k: int):
    episodes = batch.split_by_episode()
    n_episodes = len(episodes)
    if n_episodes < k:
        # Raise warning
        yield [], episodes
        return
    n_fold = n_episodes // k
    for i in range(k):
        train_episodes = episodes[: i * n_fold] + episodes[(i + 1) * n_fold :]
        if i != k - 1:
            test_episodes = episodes[i * n_fold : (i + 1) * n_fold]
        else:
            # Append remaining episodes onto the last test_episodes
            test_episodes = episodes[i * n_fold :]
        yield train_episodes, test_episodes
    return


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
        super().__init__(policy, gamma, config)
        assert isinstance(
            policy.action_space, Discrete
        ), "DM Estimator only supports discrete action spaces!"
        model_cls = QRegTorchModel if policy.framework == "torch" else QRegTFModel
        self.model = model_cls(
            policy=policy,
            gamma=gamma,
            config=config,
        )
        self.k = config.get("k", 5)

    @override(OffPolicyEstimator)
    def estimate(self, batch: SampleBatchType) -> OffPolicyEstimate:
        self.check_can_estimate_for(batch)
        estimates = []
        # Split data into train and test using k-fold cross validation
        for train_episodes, test_episodes in k_fold_cv(batch, self.k):
            # Reinitialize model
            self.model.reset()

            # Train Q-function
            if train_episodes:
                train_batch = train_episodes[0].concat_samples(train_episodes)
                # TODO (rohan): log the training losses somewhere
                losses = self.train(train_batch)  # noqa: F841

            # Calculate direct method OPE estimates
            for episode in test_episodes:
                rewards = episode["rewards"]
                V_prev, V_DM = 0.0, 0.0
                for t in range(episode.count):
                    V_prev += rewards[t] * self.gamma ** t

                init_step = episode[0:1]
                init_obs = np.array([init_step[SampleBatch.OBS]])
                all_actions = np.array(
                    [a for a in range(self.policy.action_space.n)], dtype=float
                )
                init_step[SampleBatch.ACTIONS] = all_actions
                action_probs = np.exp(self.compute_log_likelihoods(init_step))
                v_value = self.model.estimate_v(init_obs, action_probs)
                V_DM = convert_to_numpy(v_value).item()

                estimates.append(
                    OffPolicyEstimate(
                        "direct_method",
                        {
                            "V_prev": V_prev,
                            "V_DM": V_DM,
                            "V_gain_est": V_DM / max(1e-8, V_prev),
                        },
                    )
                )
        return estimates

    @override(OffPolicyEstimator)
    def train(self, batch: SampleBatchType) -> None:
        new_action_probs = []
        # TODO (rohan): This feels hacky, figure out a better way
        for episode in batch.split_by_episode():
            new_action_probs.append(np.exp(self.compute_log_likelihoods(episode)))
        self.model.train_q(batch, new_action_probs)
