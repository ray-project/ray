"""
Curiosity-driven Exploration by Self-supervised Prediction - Pathak, Agrawal,
Efros, and Darrell - UC Berkeley - ICML 2017.

This implements the curiosty-based loss function from
https://arxiv.org/pdf/1705.05363.pdf. We learn a simplified model of the
environment based on three networks:
    1) embedding states into latent space (the "features" network)
    2) predicting the next embedded state, given a state and action (the
        "forwards" network)
    3) predicting the action, given two consecutive embedded state (the
        "inverse" network)

If the agent was unable to successfully predict the state-action-next_state
sequence, we modify the standard reward with a penalty. Therefore, if a state
transition was unexpected, the agent becomes "curious" and further explores
this transition.

This is tailored for sparse reward environments, as it generates an intrinsic
reward.
"""
from gym.spaces import Space
from typing import Union, Optional



#from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch

# TODO alphabetize imports, lint
# TODO how to test if action space is discrete
# TODO should i use the default docstring format. also check the types with sven
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.framework import try_import_torch, TensorType
from ray.rllib.utils.types import TensorType, TensorStructType, SampleBatchType
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.models.torch.misc import SlimFC
# from ray.rllib.utils.exploration import EpsilonGreedy, GaussianNoise, Random
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2

from ray.rllib.utils.framework import get_activation_fn

torch, nn = try_import_torch()

"""
Example: give a full example config

TODO
"""
class Curiosity(Exploration):

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

        # Parse the curiosity-specific arguments
        submodule_type = kwargs["submodule"]
        if "submodule_type" in kwargs:
            submodule_type = kwargs["submodule_type"]
            del kwargs["submodule_type"]
        else:
            submodule_type = "StochasticSampling"


        if "forward_activation" in kwargs:
            forward_activation = kwargs["forward_activation"]
            del kwargs["forward_activation"]
        else:
            forward_activation = nn.ReLU

        if "inverse_activation" in kwargs:
            inverse_activation = kwargs["inverse_activation"]
            del kwargs["inverse_activation"]
        else:
            inverse_activation = nn.ReLU

        if "feature_activation" in kwargs:
            feature_activation = kwargs["feature_activation"]
            del kwargs["feature_activation"]
        else:
            feature_activation = nn.ReLU

        if "feature_net_hiddens" in kwargs:
            feature_net_hiddens = kwargs["feature_net_hiddens"]
            del kwargs["feature_net_hiddens"]
        else:
            feature_net_hiddens = [64]

        if "inverse_net_hiddens" in kwargs:
            inverse_net_hiddens = kwargs["inverse_net_hiddens"]
            del kwargs["inverse_net_hiddens"]
        else:
            inverse_net_hiddens = [64]

        if "forward_net_hiddens" in kwargs:
            forward_net_hiddens = kwargs["forward_net_hiddens"]
            del kwargs["forward_net_hiddens"]
        else:
            forward_net_hiddens = [64]

        if "features_dim" in kwargs:
            self.embedding_dim = kwargs["features_dim"]
            del kwargs["features_dim"]
        else:
            self.embedding_dim = [32]

        super().__init__(
            action_space=action_space,
            framework=framework,
            **kwargs)


        # TODO: what should this look like for multidimensional obs spaces
        self.obs_space_dim = kwargs["model"].obs_space.shape[0]
        self.action_space_dim = 1 # TODO can we always assume 1?

        # List of dimension of each layer
        feature_dims = [self.obs_space_dim] + feature_net_hiddens + [self.embedding_dim]
        inverse_dims = [2 * self.embedding_dim] + inverse_net_hiddens + [self.action_space_dim]
        forward_dims = [self.embedding_dim + self.action_space_dim] + forward_net_hiddens + [self.embedding_dim]

        print(forwards_dims)

        # Given a list of layer dimensions, create a FC ReLU net.
        # If layer_dims is [4,8,6] we'll have a two layer net: 4->8 and 8->6
        def create_fc_net(layer_dims, activation):
            layers = []
            for i in range(len(layer_dims) - 1):
                layers.append(
                    SlimFC(
                        in_size=layer_dims[i],
                        out_size=layer_dims[i + 1],
                        use_bias=False,
                        activation_fn=activation)
                )
            return nn.Sequential(*layers)

        self.feature_model = create_fc_net(feature_dims, feature_activation)
        self.inverse_model = create_fc_net(inverse_dims, inverse_activation)
        self.forward_model = create_fc_net(forward_dims, forward_activation)


        self.criterion = torch.nn.MSELoss(reduction="none")
        self.criterion_reduced = torch.nn.MSELoss(reduction="sum")

        #submodule = from_config( subconfig from main config )
        self.exploration_submodule = from_config({
                'type': submodule_type,
                'action_space': action_space,
                'framework': framework,
                'policy_config': self.policy_config,
                'model': self.model,
                'num_workers': self.num_workers,
                'worker_index': self.worker_index
                }
            )
#        elif submodule_type != "":
#            raise NotImplementedError("Called with a sub-exploration module "
#                                      "we don't support!")


    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        """
        Returns the action to take next

        Args:
            action_distribution (ActionDistribution): The probabilistic
                distribution we sample actions from
            timestep (Union[int, TensorType]):
            explore (bool): If true, uses the submodule strategy to select the
                next action
        """
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution, timestep=timestep)

    # TODO add a hook in the main exploration class. this returns scalar loss
    def get_exploration_loss(self,
                             policy_loss,
                             sample_batch: SampleBatchType):
        next_obs_list = torch.from_numpy(sample_batch["new_obs"]).float()
        emb_next_obs_list = self._get_latent_vector(next_obs_list).float()
        actions_list = torch.from_numpy(sample_batch["actions"]).float()

        actions_pred = self._predict_action(obs_list, next_obs_list)
        embedding_pred = self._predict_next_obs(obs_list, actions_list)

        # L2 losses for predicted action and next state
        embedding_loss = self.criterion_reduced(
            emb_next_obs_list, embedding_pred)
        actions_loss = self.criterion_reduced(
            actions_pred.squeeze(1), actions_list)
        return policy_loss.append(embedding_loss + actions_loss)

    def _get_latent_vector(self, obs: TensorType) -> TensorType:
        """
        Returns the embedded vector phi(state)
            obs (TensorType): a batch of states
        """
        return self.features_model(obs)

    def get_optimizer(self):
        """
        Returns an optimizer for environmental dynamics networks, which will
        then be handled by the Policy
        """
        forward_params = list(self.forwards_model.parameters())
        inverse_params = list(self.inverse_model.parameters())
        feature_params = list(self.features_model.parameters())

        return torch.optim.Adam(
            forward_params + inverse_params + feature_params,
            lr=1e-3)

    def postprocess_trajectory(self,
                               policy,
                               sample_batch: SampleBatchType,
                               tf_sess: Optional["tf.Session"] = None):
        # push sample batch through curiosity model
        # change rewards inside sample batch
        # don't return anything

        """
        Calculates the intrinsic curiosity based loss
        policy (TODO what type should this be): The model policy
        sample_batch (SampleBatch): a SampleBatch object containing
            data from worker environmental rollouts
        tf_sess (TODO what type): If tensorflow was supported, this would be
            the execution session
        """

        # Extract the relevant data from the SampleBatch, and cast to Tensors
        obs_list = torch.from_numpy(sample_batch["obs"]).float()
        next_obs_list = torch.from_numpy(sample_batch["new_obs"]).float()
        emb_next_obs_list = self._get_latent_vector(next_obs_list).float()
        actions_list = torch.from_numpy(sample_batch["actions"]).float()

        # Equation (2) in paper.
        actions_pred = self._predict_action(obs_list, next_obs_list)
        embedding_pred = self._predict_next_obs(obs_list, actions_list)

        # A vector of L2 losses corresponding to each observation, Equation (7) in paper
        embedding_loss = torch.sum(
            self.criterion(emb_next_obs_list, embedding_pred),
            dim=-1)

        # Equation (3) in paper. TODO discrete action space
        actions_loss = self.criterion(actions_pred.squeeze(1), actions_list)

        # todo ask sven about gpu/cpu and which operations should happen on both
        # also when do we convert from numpy array to tensor

        # Modifies environment rewards by subtracting intrinsic rewards
        sample_batch["rewards"] = sample_batch["rewards"] \
                                  - embedding_loss.clone().detach().numpy() \
                                  - actions_loss.clone().detach().numpy()

        # sample_batch is already batched so we backprop over everything.
        # self.optimizer.zero_grad()
        loss = torch.sum(embedding_loss) + torch.sum(actions_loss)
        # loss.backward()
        # self.optimizer.step()


    def _predict_action(self, obs: TensorType, next_obs: TensorType):
        """
        Returns the predicted action, given two states. This is the inverse
        dynamics model.

        obs (TensorType): Observed state at time t.
        next_obs (TensorType): Observed state at time t+1
        """
        return self.inverse_model(
            torch.cat(
                (self._get_latent_vector(obs),
                 self._get_latent_vector(next_obs)),
                axis=-1))

    # raw obs (not embedded)
    def _predict_next_obs(self, obs: TensorType, action: TensorType):
        """
        Returns the predicted next state, given an action and state.

        obs (TensorType): Observed state at time t.
        action (TensorType): Action taken at time t
        """
        return self.forwards_model(
            torch.cat(
                (self._get_latent_vector(obs), action.unsqueeze(1)),
                axis=-1))
