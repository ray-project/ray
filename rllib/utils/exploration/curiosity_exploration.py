"""
Curiosity-driven Exploration by Self-supervised Prediction - Pathak, Agrawal,
Efros, and Darrell - UC Berkeley - ICML 2017.
https://arxiv.org/pdf/1705.05363.pdf
"""
from gym.spaces import Space
from typing import Union

# TODO alphabetize imports, remove unused, alphabetize method names, type annotations, linting at end
from ray.rllib.utils.framework import try_import_torch, TensorType
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.utils.exploration import EpsilonGreedy, GaussianNoise, Random
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2

torch, nn = try_import_torch()


class CuriosityExploration(Exploration):
    """Implements an exploration strategy for Policies.

    An Exploration takes model outputs, a distribution, and a timestep from
    the agent and computes an action to apply to the environment using an
    implemented exploration schema.
    """

    def __init__(self, action_space: Space, *, framework: str,
                 policy_config: dict, model: ModelV2, num_workers: int,
                 worker_index: int):
        """
        Args:
            action_space (Space): The action space in which to explore.
            framework (str): One of "tf" or "torch".
            policy_config (dict): The Policy's config dict.
            model (ModelV2): The Policy's model.
            num_workers (int): The overall number of workers used.
            worker_index (int): The index of the worker using this class.
        """
        self.action_space = action_space
        self.policy_config = policy_config
        self.model = model
        self.num_workers = num_workers
        self.worker_index = worker_index
        self.framework = framework
        # The device on which the Model has been placed.
        # This Exploration will be on the same device.
        self.device = None
        if isinstance(self.model, nn.Module):
            params = list(self.model.parameters())
            if params:
                self.device = params[0].device

        # TODO (tanay): get from the config
        self.obs_space_dim = 16
        self.embedding_dim = 6
        self.action_space_dim = 1

        # List of dimension of each layer
        features_dims = [self.obs_space_dim, 3, self.embedding_dim]
        inverse_dims = [2 * self.obs_space_dim, 4, self.action_space]
        forwards_dims = [
            self.embedding_dim + self.action_space, 5, self.embedding_dim
        ]

        # Pass in activation_fn in model config
        # Two layer relu nets
        self.forwards_model = nn.Sequential(
            SlimFC(
                in_size=forwards_dims[0],
                out_size=forwards_dims[1],
                use_bias=False,
                activation_fn=nn.ReLU),
            SlimFC(
                in_size=forwards_dims[1],
                out_size=forwards_dims[2],
                use_bias=False,
                activation_fn=nn.ReLU))

        self.inverse_model = nn.Sequential(
            SlimFC(
                in_size=inverse_dims[0],
                out_size=inverse_dims[1],
                use_bias=False,
                activation_fn=nn.ReLU),
            SlimFC(
                in_size=inverse_dims[1],
                out_size=inverse_dims[2],
                use_bias=False,
                activation_fn=nn.ReLU))

        # TODO add Conv2D option later
        self.features_model = nn.Sequential(
            SlimFC(
                in_size=features_dims[0],
                out_size=features_dims[1],
                use_bias=False,
                activation_fn=nn.ReLU),
            SlimFC(
                in_size=features_dims[1],
                out_size=features_dims[2],
                use_bias=False,
                activation_fn=nn.ReLU))

        self.criterion = torch.nn.MSELoss(reduction="sum")
        self.optimizer = torch.optim.Adam(
            torch.cat(self.forwards_model.parameters(),
                      self.inverse_model.parameters()),
            lr=1e-3)
        # TODO lr=config["lr"]

        submodule_type = "EpsilonGreedy"
        framework = "torch"

        # TODO get from policy config
        if submodule_type == "EpsilonGreedy":
            self.exploration_submodule = EpsilonGreedy(
                action_space=action_space, framework=framework)
        elif submodule_type != "":
            raise NotImplementedError("Called with a sub-exploration module "
                                      "we don't support!")
        else:  # what's the correct default?
            self.exploration_submodule = EpsilonGreedy(
                action_space=action_space, framework=framework)

    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution, timestep=timestep)

    def get_intrinsic_loss(self):
        # TODO vectorize loss or not?
        return 0

    def get_latent_vector(self, obs):
        return self.features_model(obs)

    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        # np arrays from sample_batch
        obs_list = sample_batch["obs"]
        next_obs_list = sample_batch["new_obs"]

        emb_obs_list = self.get_latent_vector(obs_list)
        emb_next_obs_list = self.get_latent_vector(next_obs_list)

        actions_list = sample_batch["actions"]
        rewards_list = sample_batch["rewards"]

        # ask sven about gpu/cpu and which operations should happen on both
        # also when do we convert from numpy array to tensor

        # TODO check which state vectors I'm clipping out (depends on how batching is formatted)

        # Equations (2) and (3) in paper.
        actions_pred = self.predict_action(obs_list, next_obs_list)
        embedding_pred = self.predict_next_obs(obs_list, actions_list)

        # As a vector
        loss = self.criterion(emb_obs_list[:, 1:], embedding_pred[:, :-1]) \
            + self.criterion(actions_pred, actions_list)

        # does MSE loss turn it to a scalar first
        sample_batch["rewards"] = sample_batch["rewards"] - loss

        loss = torch.sum(loss)
        # Normally the gradient update is batched.
        # Here sample_batch is also batched so we backprop over everything.
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()

        return sample_batch

    def predict_action(self, obs, next_obs):
        return self.inverse_model(
            torch.cat(
                (self.get_latent_vector(obs),
                 self.get_latent_vector(next_obs)),
                axis=-1))

    def predict_next_obs(self, obs, action):
        return self.forwards_model(
            torch.cat((self.get_latent_vector(obs), action), axis=-1))
