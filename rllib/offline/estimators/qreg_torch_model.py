from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy
from typing import Dict, List, Union

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class QRegTorchModel(TorchModelV2):
    """Pytorch implementation of the Q-Reg model from
    https://arxiv.org/pdf/1911.06854.pdf

    Arguments:
        policy: The Policy object correspodning to the target policy in OPE
        gamma: The discount factor for the environment
        config: Optional config settings for Q-Reg
        config = {
            # The ModelConfigDict for self.q_model
            "model": {"fcnet_hiddens": [32, 32], "fcnet_activation": "relu"},
            # Maximum number of training iterations to run on the batch
            "n_iters": 80,
            Learning rate for Q-function optimizer
            "lr": 1e-3,
            Early stopping if the mean loss < delta
            "delta": 1e-4,
        }
    """

    def __init__(self, policy: Policy, gamma: float, config: Dict) -> None:
        self.policy = policy
        self.gamma = gamma
        self.observation_space = policy.observation_space
        self.action_space = policy.action_space

        self.q_model: TorchModelV2 = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            self.action_space.n,
            config.get("model", {}),
            framework="torch",
            name="TorchQModel",
        )
        self.device = self.policy.device
        self.n_iters = config.get("n_iters", 80)
        self.lr = config.get("lr", 1e-3)
        self.delta = config.get("delta", 1e-4)
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
        obs = torch.tensor(batch[SampleBatch.OBS], device=self.device)
        actions = torch.tensor(batch[SampleBatch.ACTIONS], device=self.device)
        ps = torch.zeros([batch.count], device=self.device)
        returns = torch.zeros([batch.count], device=self.device)
        discounts = torch.zeros([batch.count], device=self.device)
        num_state_inputs = 0
        for k in batch.keys():
            if k.startswith("state_in_"):
                num_state_inputs += 1
        state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]
        eps_begin = 0
        for episode in batch.split_by_episode():
            eps_end = eps_begin + episode.count

            # get rewards, old_prob, new_prob
            rewards = episode[SampleBatch.REWARDS]
            old_prob = episode[SampleBatch.ACTION_PROB]
            new_prob = self.policy.compute_log_likelihoods(
                actions=episode[SampleBatch.ACTIONS],
                obs_batch=episode[SampleBatch.OBS],
                state_batches=[episode[k] for k in state_keys],
                prev_action_batch=episode.get(SampleBatch.PREV_ACTIONS),
                prev_reward_batch=episode.get(SampleBatch.PREV_REWARDS),
                actions_normalized=True,
            )

            # calculate importance ratios and returns
            for t in range(episode.count):
                discounts[eps_begin + t] = self.gamma ** t
                if t == 0:
                    pt_prev = 1.0
                    pt_next = 1.0
                    returns[eps_end - 1] = rewards[-1]
                else:
                    pt_prev = ps[eps_begin + t - 1]
                    pt_next = pt_next * new_prob[-t] / old_prob[-t]
                    returns[eps_end - t - 1] = (
                        rewards[-t - 1] + self.gamma * pt_next * returns[eps_end - t]
                    )
                ps[eps_begin + t] = pt_prev * new_prob[t] / old_prob[t]

            # Update before next episode
            eps_begin = eps_end

        losses = []
        for _ in range(self.n_iters):
            q_values, _ = self.q_model({"obs": obs}, [], None)
            q_acts = torch.gather(q_values, -1, actions.unsqueeze(-1))
            loss = discounts * ps * (returns - q_acts) ** 2
            loss = torch.mean(loss)
            self.optimizer.zero_grad()
            self.optimizer.step()
            losses.append(loss.item())
            if loss < self.delta:
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
            q_values = torch.gather(q_values, -1, actions.unsqueeze(-1))
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
        v_values = torch.sum(q_values * action_probs, axis=-1)
        return v_values.detach()
