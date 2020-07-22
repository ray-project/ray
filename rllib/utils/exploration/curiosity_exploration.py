"""
Curiosity-driven Exploration by Self-supervised Prediction - Pathak, Agrawal,
Efros, and Darrell - UC Berkeley - ICML 2017.
https://arxiv.org/pdf/1705.05363.pdf
"""
from gym.spaces import Space
from typing import Union

#TODO initialization
# TODO alphabetize imports, check type annotations, lint
from ray.rllib.utils.framework import try_import_torch, TensorType
from ray.rllib.utils.types import TensorType, TensorStructType
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.utils.exploration import EpsilonGreedy, GaussianNoise, Random
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2

torch, nn = try_import_torch()


class Curiosity(Exploration):
    """Implements an exploration strategy for Policies.

    An Exploration takes model outputs, a distribution, and a timestep from
    the agent and computes an action to apply to the environment using an
    implemented exploration schema.
    """

    def __init__(self,
                 action_space: Space,
                 *,
                 framework: str,
                 **kwargs):
        """
        Args:
            action_space (Space): The action space in which to explore.
            framework (str): One of "tf" or "torch". Currently only torch is
                supported.
        """
        if framework != "torch":
            raise NotImplementedError("only torch is currently supported for "
                                      "curiosity")

        super().__init__(
            action_space=action_space,
            framework=framework,
            **kwargs)


        # TODO (tanay): get from the config
        self.obs_space_dim = 4 #self.model.obs_space.shape[0]
        self.embedding_dim = 6
        self.action_space_dim = 1

        # List of dimension of each layer
        features_dims = [self.obs_space_dim, 3, self.embedding_dim]
        inverse_dims = [2 * self.embedding_dim, 4, self.action_space_dim]
        forwards_dims = [
            self.embedding_dim + self.action_space_dim, 5, self.embedding_dim
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

        forward_params = list(self.forwards_model.parameters())
        inverse_params = list(self.inverse_model.parameters())
        feature_params = list(self.features_model.parameters())

        self.criterion = torch.nn.MSELoss(reduction="none")
        self.optimizer = torch.optim.Adam(
            forward_params + inverse_params + feature_params,
            lr=1e-3)
        # TODO lr=config["lr"]

        submodule_type = "EpsilonGreedy"
        framework = "torch"

        # TODO get from policy config
        if submodule_type == "EpsilonGreedy":
            self.exploration_submodule = EpsilonGreedy(
                action_space=action_space,
                framework=framework,
                policy_config=self.policy_config,
                model=self.model,
                num_workers=self.num_workers,
                worker_index=self.worker_index
            )
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

    def _get_latent_vector(self, obs: TensorType):
        return self.features_model(obs)

    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        # np arrays from sample_batch

        obs_list = torch.from_numpy(sample_batch["obs"]).float()
        next_obs_list = torch.from_numpy(sample_batch["new_obs"]).float()
        emb_next_obs_list = self._get_latent_vector(next_obs_list).float()

        actions_list = torch.from_numpy(sample_batch["actions"]).float()
        rewards_list = torch.from_numpy(sample_batch["rewards"]).float()

        # Equations (2) and (3) in paper.
        actions_pred = self._predict_action(obs_list, next_obs_list)
        embedding_pred = self._predict_next_obs(obs_list, actions_list)



        # Gives a vector of losses, of L2 losses corresponding to each observation
        embedding_loss = torch.sum(
            self.criterion(emb_next_obs_list, embedding_pred),
            dim=-1)
        actions_loss = self.criterion(actions_pred.squeeze(1), actions_list)

        # todo ask sven about gpu/cpu and which operations should happen on both
        # also when do we convert from numpy array to tensor

        # does MSE loss turn it to a scalar first
        sample_batch["rewards"] = sample_batch["rewards"] \
                                  - embedding_loss.clone().detach().numpy() \
                                  - actions_loss.clone().detach().numpy()

        loss = torch.sum(embedding_loss) + torch.sum(actions_loss)

        # Normally the gradient update is batched.
        # sample_batch is also batched so we backprop over everything.
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()

        return sample_batch

    # raw observation from the environment as a tensor
    def _predict_action(self, obs: TensorType, next_obs: TensorType):
        return self.inverse_model(
            torch.cat(
                (self._get_latent_vector(obs),
                 self._get_latent_vector(next_obs)),
                axis=-1))

    # raw obs (not embedded)
    def _predict_next_obs(self, obs: TensorType, action: TensorType):
        return self.forwards_model(
            torch.cat(
                (self._get_latent_vector(obs), action.unsqueeze(1)),
                axis=-1))
