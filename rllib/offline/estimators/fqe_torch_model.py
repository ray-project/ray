from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy
from typing import Dict, List, Union

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class FQETorchModel:
    """Pytorch implementation of the Fitted Q-Evaluation (FQE) model from
    https://arxiv.org/pdf/1911.06854.pdf

    Arguments:
        policy: The Policy object correspodning to the target policy in OPE
        gamma: The discount factor for the environment
        config: Optional config settings for FQE
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
        rewards = torch.tensor(batch[SampleBatch.REWARDS], device=self.device)
        next_obs = torch.tensor(batch[SampleBatch.NEXT_OBS], device=self.device)
        dones = torch.tensor(batch[SampleBatch.DONES], device=self.device)

        # Neccessary if policy uses recurrent/attention model
        num_state_inputs = 0
        for k in batch.keys():
            if k.startswith("state_in_"):
                num_state_inputs += 1
        state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]

        # Compute action_probs for next_obs as in FQE
        all_actions = torch.zeros([batch.count, self.policy.action_space.n])
        all_actions[:] = torch.arange(self.policy.action_space.n)
        next_action_prob = self.policy.compute_log_likelihoods(
            actions=all_actions.T,
            obs_batch=next_obs,
            state_batches=[batch[k] for k in state_keys],
            prev_action_batch=batch[SampleBatch.ACTIONS],
            prev_reward_batch=batch[SampleBatch.REWARDS],
            actions_normalized=False,
        )
        next_action_prob = torch.exp(next_action_prob.T).to(self.device)

        losses = []
        for _ in range(self.n_iters):
            q_acts = self.estimate_q(batch[SampleBatch.OBS], batch[SampleBatch.ACTIONS])
            next_v = self.estimate_v(batch[SampleBatch.NEXT_OBS], next_action_prob)
            targets = rewards + ~dones * self.gamma * next_v
            loss = (targets - q_acts) ** 2
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
