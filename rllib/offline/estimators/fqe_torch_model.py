from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.annotations import is_overridden
from ray.rllib.utils.typing import ModelConfigDict, TensorType
from gym.spaces import Discrete

torch, nn = try_import_torch()

# TODO: Create a config object for FQE and unify it with the RLModule API


@DeveloperAPI
class FQETorchModel:
    """Pytorch implementation of the Fitted Q-Evaluation (FQE) model from
    https://arxiv.org/abs/1911.06854
    """

    def __init__(
        self,
        policy: Policy,
        gamma: float,
        model: ModelConfigDict = None,
        n_iters: int = 1,
        lr: float = 1e-3,
        delta: float = 1e-4,
        clip_grad_norm: float = 100.0,
        minibatch_size: int = None,
        tau: float = 1.0,
    ) -> None:
        """
        Args:
            policy: Policy to evaluate.
            gamma: Discount factor of the environment.
            model: The ModelConfigDict for self.q_model, defaults to:
                {
                    "fcnet_hiddens": [8, 8],
                    "fcnet_activation": "relu",
                    "vf_share_layers": True,
                },
            n_iters: Number of gradient steps to run on batch, defaults to 1
            lr: Learning rate for Adam optimizer
            delta: Early stopping threshold if the mean loss < delta
            clip_grad_norm: Clip loss gradients to this maximum value
            minibatch_size: Minibatch size for training Q-function;
                if None, train on the whole batch
            tau: Polyak averaging factor for target Q-function
        """
        self.policy = policy
        assert isinstance(
            policy.action_space, Discrete
        ), f"{self.__class__.__name__} only supports discrete action spaces!"
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
        self.minibatch_size = minibatch_size
        self.tau = tau
        self.optimizer = torch.optim.Adam(self.q_model.variables(), self.lr)
        initializer = get_initializer("xavier_uniform", framework="torch")
        # Hard update target
        self.update_target(tau=1.0)

        def f(m):
            if isinstance(m, nn.Linear):
                initializer(m.weight)

        self.initializer = f

    def train(self, batch: SampleBatch) -> TensorType:
        """Trains self.q_model using FQE loss on given batch.

        Args:
            batch: A SampleBatch of episodes to train on

        Returns:
            A list of losses for each training iteration
        """
        losses = []
        minibatch_size = self.minibatch_size or batch.count
        # Copy batch for shuffling
        batch = batch.copy(shallow=True)
        for _ in range(self.n_iters):
            minibatch_losses = []
            batch.shuffle()
            for idx in range(0, batch.count, minibatch_size):
                minibatch = batch[idx : idx + minibatch_size]
                obs = torch.tensor(minibatch[SampleBatch.OBS], device=self.device)
                actions = torch.tensor(
                    minibatch[SampleBatch.ACTIONS],
                    device=self.device,
                    dtype=int,
                )
                rewards = torch.tensor(
                    minibatch[SampleBatch.REWARDS], device=self.device
                )
                next_obs = torch.tensor(
                    minibatch[SampleBatch.NEXT_OBS], device=self.device
                )
                dones = torch.tensor(
                    minibatch[SampleBatch.DONES], device=self.device, dtype=float
                )

                # Compute Q-values for current obs
                q_values, _ = self.q_model({"obs": obs}, [], None)
                q_acts = torch.gather(q_values, -1, actions.unsqueeze(-1)).squeeze(-1)

                next_action_probs = self._compute_action_probs(next_obs)

                # Compute Q-values for next obs
                with torch.no_grad():
                    next_q_values, _ = self.target_q_model({"obs": next_obs}, [], None)

                # Compute estimated state value next_v = E_{a ~ pi(s)} [Q(next_obs,a)]
                next_v = torch.sum(next_q_values * next_action_probs, axis=-1)
                targets = rewards + (1 - dones) * self.gamma * next_v
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
        actions = torch.tensor(
            batch[SampleBatch.ACTIONS], device=self.device, dtype=int
        )
        q_values = torch.gather(q_values, -1, actions.unsqueeze(-1)).squeeze(-1)
        return q_values

    def estimate_v(self, batch: SampleBatch) -> TensorType:
        obs = torch.tensor(batch[SampleBatch.OBS], device=self.device)
        with torch.no_grad():
            q_values, _ = self.q_model({"obs": obs}, [], None)
        # Compute pi(a | s) for each action a in policy.action_space
        action_probs = self._compute_action_probs(obs)
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

    def _compute_action_probs(self, obs: TensorType) -> TensorType:
        """Compute action distribution over the action space.

        Args:
            obs: A tensor of observations of shape (batch_size * obs_dim)

        Returns:
            action_probs: A tensor of action probabilities
            of shape (batch_size * action_dim)
        """
        input_dict = {SampleBatch.OBS: obs}
        seq_lens = torch.ones(len(obs), device=self.device, dtype=int)
        state_batches = []
        if is_overridden(self.policy.action_distribution_fn):
            try:
                # TorchPolicyV2 function signature
                dist_inputs, dist_class, _ = self.policy.action_distribution_fn(
                    self.policy.model,
                    obs_batch=input_dict,
                    state_batches=state_batches,
                    seq_lens=seq_lens,
                    explore=False,
                    is_training=False,
                )
            except TypeError:
                # TorchPolicyV1 function signature for compatibility with DQN
                # TODO: Remove this once DQNTorchPolicy is migrated to PolicyV2
                dist_inputs, dist_class, _ = self.policy.action_distribution_fn(
                    self.policy,
                    self.policy.model,
                    input_dict=input_dict,
                    state_batches=state_batches,
                    seq_lens=seq_lens,
                    explore=False,
                    is_training=False,
                )
        else:
            dist_class = self.policy.dist_class
            dist_inputs, _ = self.policy.model(input_dict, state_batches, seq_lens)
        action_dist = dist_class(dist_inputs, self.policy.model)
        assert isinstance(
            action_dist.dist, torch.distributions.categorical.Categorical
        ), "FQE only supports Categorical or MultiCategorical distributions!"
        action_probs = action_dist.dist.probs
        return action_probs
