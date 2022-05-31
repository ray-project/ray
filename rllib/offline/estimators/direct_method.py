from typing import Tuple, List, Generator
from ray.rllib.offline.estimators.off_policy_estimator import (
    OffPolicyEstimator,
    OffPolicyEstimate,
)
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.offline.estimators.fqe_torch_model import FQETorchModel
from ray.rllib.offline.estimators.qreg_torch_model import QRegTorchModel
from gym.spaces import Discrete
import numpy as np


# TODO (rohan): replace with AIR/parallel workers
# (And find a better name than `should_train`)
def k_fold_cv(
    batch: SampleBatchType, k: int, should_train: bool = True
) -> Generator[Tuple[List[SampleBatch]], None, None]:
    """Utility function that returns a k-fold cross validation generator
    over episodes from the given batch. If the number of episodes in the
    batch is less than `k` or `should_train` is set to False, yields an empty
    list for train_episodes and all the episodes in test_episodes.

    Args:
        batch: A SampleBatch of episodes to split
        k: Number of cross-validation splits
        should_train: True by default. If False, yield [], [episodes].

    Returns:
        A tuple with two lists of SampleBatches (train_episodes, test_episodes)
    """
    episodes = batch.split_by_episode()
    n_episodes = len(episodes)
    if n_episodes < k or not should_train:
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
    """The Direct Method estimator.

    q_model_type: Either "fqe" for Fitted Q-Evaluation or "qreg" for Q-Regression
    framework: One of "tf|tf2|tfe|torch", currently only "torch" is supported
    k: k-fold cross validation for training model and evaluating OPE
    q_model_kwargs: Optional arguments for the specified Q model

    DM estimator described in https://arxiv.org/pdf/1511.03722.pdf"""

    @override(OffPolicyEstimator)
    def __init__(
        self,
        name: str,
        policy: Policy,
        gamma: float,
        q_model_type: str = "fqe",
        framework: str = "torch",
        k: int = 5,
        **q_model_kwargs,
    ):
        super().__init__(name, policy, gamma)
        # TODO (rohan): Add support for continuous action spaces
        assert isinstance(
            policy.action_space, Discrete
        ), "DM Estimator only supports discrete action spaces!"
        assert (
            policy.config["batch_mode"] == "complete_episodes"
        ), "DM Estimator only supports batch_mode=`complete_episodes`"
        assert framework == "torch", "DM estimator only supports `framework`=`torch`"

        # TODO (rohan): Add support for QRegTF, FQETF, custom QModel types!
        if framework == "torch":
            if q_model_type == "qreg":
                model_cls = QRegTorchModel
            elif q_model_type == "fqe":
                model_cls = FQETorchModel
            else:
                raise ValueError(f"Unknown `q_model_type`= {q_model_type}")

        self.model = model_cls(
            policy=policy,
            gamma=gamma,
            **q_model_kwargs,
        )
        self.k = k
        self.losses = []

    @override(OffPolicyEstimator)
    def estimate(
        self, batch: SampleBatchType, should_train: bool = True
    ) -> OffPolicyEstimate:
        self.check_can_estimate_for(batch)
        estimates = []
        # Split data into train and test using k-fold cross validation
        for train_episodes, test_episodes in k_fold_cv(batch, self.k, should_train):

            # Train Q-function
            if train_episodes:
                # Reinitialize model
                self.model.reset()
                train_batch = SampleBatch.concat_samples(train_episodes)
                losses = self.train(train_batch)  # noqa: F841
                self.losses.append(losses)

            # Calculate direct method OPE estimates
            for episode in test_episodes:
                rewards = episode["rewards"]
                v_old = 0.0
                v_new = 0.0
                for t in range(episode.count):
                    v_old += rewards[t] * self.gamma ** t

                init_step = episode[0:1]
                init_obs = np.array([init_step[SampleBatch.OBS]])
                all_actions = np.array(
                    [a for a in range(self.policy.action_space.n)], dtype=float
                )
                init_step[SampleBatch.ACTIONS] = all_actions
                action_probs = np.exp(self.action_log_likelihood(init_step))
                v_value = self.model.estimate_v(init_obs, action_probs)
                v_new = convert_to_numpy(v_value).item()

                estimates.append(
                    OffPolicyEstimate(
                        self.name,
                        {
                            "v_old": v_old,
                            "v_new": v_new,
                            "v_gain": v_new / max(1e-8, v_old),
                        },
                    )
                )
        return estimates

    @override(OffPolicyEstimator)
    def train(self, batch: SampleBatchType):
        return self.model.train_q(batch)
