from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy
from typing import List, Union
import numpy as np

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType, ModelConfigDict

torch, nn = try_import_torch()


@ExperimentalAPI
class QRegTorchModel:
    """Pytorch implementation of the Q-Reg model from
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
        self.n_iters = n_iters
        self.lr = lr
        self.delta = delta
        self.clip_grad_norm = clip_grad_norm
        self.batch_size = batch_size
        self.optimizer = torch.optim.Adam(self.q_model.variables(), self.lr)
        initializer = get_initializer("xavier_uniform", framework="torch")

        def f(m):
            if isinstance(m, nn.Linear):
                initializer(m.weight)

        self.initializer = f

    def reset(self) -> None:
        """Resets/Reinintializes the model weights."""
        self.q_model.apply(self.initializer)

    def train_q(self, batch: SampleBatch) -> TensorType:
        """Trains self.q_model using Q-Reg loss on given batch.

        Args:
            batch: A SampleBatch of episodes to train on

        Returns:
            A list of losses for each training iteration
        """
        losses = []
        obs = torch.tensor(batch[SampleBatch.OBS], device=self.device)
        actions = torch.tensor(batch[SampleBatch.ACTIONS], device=self.device)
        ps = torch.zeros([batch.count], device=self.device)
        returns = torch.zeros([batch.count], device=self.device)
        discounts = torch.zeros([batch.count], device=self.device)

        # Neccessary if policy uses recurrent/attention model
        num_state_inputs = 0
        for k in batch.keys():
            if k.startswith("state_in_"):
                num_state_inputs += 1
        state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]

        # get rewards, old_prob, new_prob
        rewards = batch[SampleBatch.REWARDS]
        old_log_prob = torch.tensor(batch[SampleBatch.ACTION_LOGP])
        new_log_prob = (
            self.policy.compute_log_likelihoods(
                actions=batch[SampleBatch.ACTIONS],
                obs_batch=batch[SampleBatch.OBS],
                state_batches=[batch[k] for k in state_keys],
                prev_action_batch=batch.get(SampleBatch.PREV_ACTIONS),
                prev_reward_batch=batch.get(SampleBatch.PREV_REWARDS),
                actions_normalized=False,
            )
            .detach()
            .cpu()
        )
        prob_ratio = torch.exp(new_log_prob - old_log_prob)

        eps_begin = 0
        for episode in batch.split_by_episode():
            eps_end = eps_begin + episode.count

            # calculate importance ratios and returns

            for t in range(episode.count):
                discounts[eps_begin + t] = self.gamma ** t
                if t == 0:
                    pt_prev = 1.0
                else:
                    pt_prev = ps[eps_begin + t - 1]
                ps[eps_begin + t] = pt_prev * prob_ratio[eps_begin + t]

                # O(n^3)
                # ret = 0
                # for t_prime in range(t, episode.count):
                #     gamma = self.gamma ** (t_prime - t)
                #     rho_t_1_t_prime = 1.0
                #     for k in range(t + 1, min(t_prime + 1, episode.count)):
                #         rho_t_1_t_prime = rho_t_1_t_prime * prob_ratio[eps_begin + k]
                #     r = rewards[eps_begin + t_prime]
                #     ret += gamma * rho_t_1_t_prime * r

                # O(n^2)
                ret = 0
                rho = 1
                for t_ in reversed(range(t, episode.count)):
                    ret = rewards[eps_begin + t_] + self.gamma * rho * ret
                    rho = prob_ratio[eps_begin + t_]

                returns[eps_begin + t] = ret

            # Update before next episode
            eps_begin = eps_end

        indices = np.arange(batch.count)
        for _ in range(self.n_iters):
            minibatch_losses = []
            np.random.shuffle(indices)
            for idx in range(0, batch.count, self.batch_size):
                idxs = indices[idx : idx + self.batch_size]
                q_values, _ = self.q_model({"obs": obs[idxs]}, [], None)
                q_acts = torch.gather(
                    q_values, -1, actions[idxs].unsqueeze(-1)
                ).squeeze(-1)
                loss = discounts[idxs] * ps[idxs] * (returns[idxs] - q_acts) ** 2
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
        return losses

    def estimate_q(
        self,
        obs: Union[TensorType, List[TensorType]],
        actions: Union[TensorType, List[TensorType]] = None,
    ) -> TensorType:
        """Given `obs`, a list or array or tensor of observations,
        compute the Q-values for `obs` for all actions in the action space.
        If `actions` is not None, return the Q-values for the actions provided,
        else return Q-values for all actions for each observation in `obs`.
        """
        obs = torch.tensor(obs, device=self.device)
        q_values, _ = self.q_model({"obs": obs}, [], None)
        if actions is not None:
            actions = torch.tensor(actions, device=self.device, dtype=int)
            q_values = torch.gather(q_values, -1, actions.unsqueeze(-1)).squeeze(-1)
        return q_values.detach()

    def estimate_v(
        self,
        obs: Union[TensorType, List[TensorType]],
        action_probs: Union[TensorType, List[TensorType]],
    ) -> TensorType:
        """Given `obs`, compute q-values for all actions in the action space
        for each observations s in `obs`, then multiply this by `action_probs`,
        the probability distribution over actions for each state s to give the
        state value V(s) = sum_A pi(a|s)Q(s,a).
        """
        q_values = self.estimate_q(obs)
        action_probs = torch.tensor(action_probs, device=self.device)
        v_values = torch.sum(q_values * action_probs, axis=-1)
        return v_values.detach()
