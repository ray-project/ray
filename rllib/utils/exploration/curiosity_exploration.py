from gym.spaces import Space
from typing import Union

# TODO alphabetize imports, remove unused, alphabetize method names
from ray.rllib.utils.framework import try_import_torch, TensorType
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.utils.exploration import EpsilonGreedy, GaussianNoise, OrnsteinUhlenbeckNoise
torch, nn = try_import_torch()


# TODO should it also subcless nn.Module? how should i order the inits?
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
        self.embedding_dim = 16
        self.action_space_dim = 1

        # List of dimension of each layer
        features_dims = [self.obs_space_dim, 64, self.embedding_dim]
        inverse_dims = [2 * self.obs_space_dim, 32, self.action_space]
        forwards_dims = [self.embedding_dim + self.action_space, 123,
                         self.embedding_dim]

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

        # TODO vae?
        self.features_model = None

        self.optimizer = torch.optim.Adam(
            torch.cat(
                self.forwards_model.parameters(),
                self.inverse_model.parameters()
            ), lr=10e-3)
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
        else: # what's the correct default?
            self.exploration_submodule = EpsilonGreedy(
                action_space=action_space, framework=framework)

    def get_latent_vector(self, obs):
        return self.features_model(obs)

    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        # get from sample batch
        obs = {}
        next_obs = {} # torch tensor
        actions = {}
        rewards = {}


        intrinsic_rewards = np.randn(10)
        for i, o in enumerate(obs):
            # calculate losses
            # explicitly backprop through all three networks

            # have to handle gradient minibatching: we can just run
            # over everything since sample_batch is trivially batched
        return sample_batch


    def predict_action(self, obs, next_obs):
        return self.inverse_model(torch.cat((obs, next_obs), axis=0))

    def predict_next_obs(self, obs, action):
        return self.forwards_model(torch.cat((obs, action), axis=0))
