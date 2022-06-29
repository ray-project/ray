from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModelConfigDict, TensorType
from ray.rllib.offline.estimators.utils import action_log_likelihood
import numpy as np

torch, nn = try_import_torch()


@DeveloperAPI
class FQETorchModel:
    """Pytorch implementation of the Fitted Q-Evaluation (FQE) model from
    https://arxiv.org/pdf/1911.06854.pdf
    """

    def __init__(
        self,
        policy: Policy,
        gamma: float,
        model: ModelConfigDict = None,
        n_iters: int = 160,
        lr: float = 1e-3,
        delta: float = 1e-4,
        clip_grad_norm: float = 100.0,
        batch_size: int = 32,
        tau: float = 0.05,
    ) -> None:
        """
        Args:
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
            # The ModelConfigDict for self.q_model
            model = {
                        "fcnet_hiddens": [8, 8],
                        "fcnet_activation": "relu",
                        "vf_share_layers": True,
                    },
            # Maximum number of training iterations to run on the batch
            n_iters = 160,
            # Learning rate for Q-function optimizer
            lr = 1e-3,
            # Early stopping if the mean loss < delta
            delta = 1e-4,
            # Clip gradients to this maximum value
            clip_grad_norm = 100.0,
            # Minibatch size for training Q-function
            batch_size = 32,
            # Polyak averaging factor for target Q-function
            tau = 0.05
        """
        self.policy = policy
        self.gamma = gamma
        self.observation_space = policy.observation_space
        self.action_space = policy.action_space

        if model is None:
            model = {
                "fcnet_hiddens": [8, 8],
                "fcnet_activation": "relu",
                "vf_share_layers": True,
            }

        self.device = self.policy.device
        self.q_model: TorchModelV2 = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            self.action_space.n,
            model,
            framework="torch",
            name="TorchQModel",
        ).to(self.device)
        self.target_q_model: TorchModelV2 = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            self.action_space.n,
            model,
            framework="torch",
            name="TargetTorchQModel",
        ).to(self.device)
        self.n_iters = n_iters
        self.lr = lr
        self.delta = delta
        self.clip_grad_norm = clip_grad_norm
        self.batch_size = batch_size
        self.tau = tau
        self.optimizer = torch.optim.Adam(self.q_model.variables(), self.lr)
        initializer = get_initializer("xavier_uniform", framework="torch")
        # Hard update target
        self.update_target(tau=1.0)

        def f(m):
            if isinstance(m, nn.Linear):
                initializer(m.weight)

        self.initializer = f

    def reset(self) -> None:
        """Resets/Reinintializes the model weights."""
        self.q_model.apply(self.initializer)

    def train(self, batch: SampleBatch) -> TensorType:
        """Trains self.q_model using FQE loss on given batch.

        Args:
            batch: A SampleBatch of episodes to train on

        Returns:
            A list of losses for each training iteration
        """
        losses = []
        for _ in range(self.n_iters):
            minibatch_losses = []
            batch.shuffle()
            for idx in range(0, batch.count, self.batch_size):
                minibatch = batch[idx : idx + self.batch_size]
                obs = torch.tensor(minibatch[SampleBatch.OBS], device=self.device)
                actions = torch.tensor(
                    minibatch[SampleBatch.ACTIONS], device=self.device
                )
                rewards = torch.tensor(
                    minibatch[SampleBatch.REWARDS], device=self.device
                )
                next_obs = torch.tensor(
                    minibatch[SampleBatch.NEXT_OBS], device=self.device
                )
                dones = torch.tensor(minibatch[SampleBatch.DONES], device=self.device)

                # Neccessary if policy uses recurrent/attention model
                num_state_inputs = 0
                for k in batch.keys():
                    if k.startswith("state_in_"):
                        num_state_inputs += 1
                state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]

                # Compute action_probs for next_obs as in FQE
                all_actions = torch.zeros([minibatch.count, self.policy.action_space.n])
                all_actions[:] = torch.arange(self.policy.action_space.n)
                next_action_prob = self.policy.compute_log_likelihoods(
                    actions=all_actions.T,
                    obs_batch=next_obs,
                    state_batches=[minibatch[k] for k in state_keys],
                    prev_action_batch=minibatch[SampleBatch.ACTIONS],
                    prev_reward_batch=minibatch[SampleBatch.REWARDS],
                    actions_normalized=False,
                )
                next_action_prob = (
                    torch.exp(next_action_prob.T).to(self.device).detach()
                )

                q_values, _ = self.q_model({"obs": obs}, [], None)
                q_acts = torch.gather(q_values, -1, actions.unsqueeze(-1)).squeeze(-1)
                with torch.no_grad():
                    next_q_values, _ = self.target_q_model({"obs": next_obs}, [], None)
                next_v = torch.sum(next_q_values * next_action_prob, axis=-1)
                targets = rewards + ~dones * self.gamma * next_v
                loss = (targets - q_acts) ** 2
                loss = torch.mean(loss)
                self.optimizer.zero_grad()
                loss.backward()
                nn.utils.clip_grad.clip_grad_norm_(
                    self.q_model.variables(), self.clip_grad_norm
                )
                self.optimizer.step()
                minibatch_losses.append(loss.item())
            iter_loss = sum(minibatch_losses) / len(minibatch_losses)
            losses.append(iter_loss)
            if iter_loss < self.delta:
                break
            self.update_target()
        return losses

    def estimate_q(self, batch: SampleBatch) -> TensorType:
        obs = torch.tensor(batch[SampleBatch.OBS], device=self.device)
        with torch.no_grad():
            q_values, _ = self.q_model({"obs": obs}, [], None)
        actions = torch.tensor(batch[SampleBatch.ACTIONS], device=self.device)
        q_values = torch.gather(q_values, -1, actions.unsqueeze(-1)).squeeze(-1)
        return q_values

    def estimate_v(self, batch: SampleBatch) -> TensorType:
        obs = torch.tensor(batch[SampleBatch.OBS], device=self.device)
        with torch.no_grad():
            q_values, _ = self.q_model({"obs": obs}, [], None)
        action_probs = []
        tmp_batch = batch.copy()
        for i in range(self.policy.action_space.n):
            tmp_batch[SampleBatch.ACTIONS] = (
                np.zeros_like(batch[SampleBatch.ACTIONS]) + i
            )
            tmp_probs = np.exp(action_log_likelihood(self.policy, tmp_batch))
            action_probs.append(tmp_probs)
        action_probs = np.swapaxes(action_probs, 0, 1)
        action_probs = torch.tensor(action_probs, device=self.device)
        v_values = torch.sum(q_values * action_probs, axis=-1)
        return v_values

    def update_target(self, tau=None):
        # Update_target will be called periodically to copy Q network to
        # target Q network, using (soft) tau-synching.
        tau = tau or self.tau
        model_state_dict = self.q_model.state_dict()
        # Support partial (soft) synching.
        # If tau == 1.0: Full sync from Q-model to target Q-model.
        target_state_dict = self.target_q_model.state_dict()
        model_state_dict = {
            k: tau * model_state_dict[k] + (1 - tau) * v
            for k, v in target_state_dict.items()
        }

        self.target_q_model.load_state_dict(model_state_dict)
