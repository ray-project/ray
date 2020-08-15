from gym.spaces import Discrete, Space
from typing import Union, Optional

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_torch, TensorType
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.types import FromConfigSpec, SampleBatchType, \
    TrainerConfigDict

torch, nn = try_import_torch()
F = None
if nn is not None:
    F = nn.functional

"""
Example Configuration

config = ppo.DEFAULT_CONFIG
env = "CartPole-v0"
config["framework"] = "torch"
config["exploration_config"] = {
    "type": "ray.rllib.utils.exploration.curiosity_exploration.Curiosity",
    "forward_net_hiddens": [64],
    "inverse_net_hiddens": [32,4],
    "feature_net_hiddens": [16,8],
    "feature_dim": 8,
    "eta": 1.0,
    "forward_activation": "relu",
    "inverse_activation": "relu",
    "feature_activation": "relu",
    "submodule": "EpsilonGreedy",
}
trainer = ppo.PPOTrainer(config=config, env=env)
trainer.train()
"""


class Curiosity(Exploration):
    """Implementation of:
    Curiosity-driven Exploration by Self-supervised Prediction
    Pathak, Agrawal, Efros, and Darrell - UC Berkeley - ICML 2017.
    https://arxiv.org/pdf/1705.05363.pdf

    Learns a simplified model of the environment based on three networks:
    1) Embedding observations into latent space ("features" network).
    2) Predicting the next embedded obs, given an obs and action
        ("forwards" network).
    3) Predicting the action, given two consecutive embedded observations
        ("inverse" network).

    The less the agent is able to predict the actually observed next_obs,
    given obs and action (through the forwards network), the larger the
    "intrinsic reward", which will modify the standard reward with a penalty.
    Therefore, if a state transition was unexpected, the agent becomes
    "curious" and will further explores this transition.
    """

    def __init__(self,
                 action_space: Space,
                 *,
                 framework: str,
                 model: ModelV2,
                 eta: float = 1.0,
                 sub_exploration: FromConfigSpec,
                 **kwargs):
        """Initializes a Curiosity object.

        Args:
            action_space (Space): The action space in which to explore.
            framework (str): One of "tf" or "torch". Currently only torch is
                supported.
        """
        if framework != "torch":
            raise ValueError("Only torch is currently supported for Curiosity")
        elif not isinstance(action_space, Discrete):
            raise ValueError(
                "Only Discrete action spaces supported for Curiosity so far.")

        super().__init__(
            action_space, model=model, framework=framework, **kwargs)

        ## Parse the curiosity-specific arguments
        ## If it was not specified in the config, assign the given default
        #def extract_from_kwargs(key, default):
        #    if key in kwargs:
        #        temp = kwargs[key]
        #        del kwargs[key]
        #        return temp
        #    else:
        #        return default

        # Casts a single int to a list, else leaves it unchanged
        #def cast_to_list(l):
        #    if type(l) == int:
        #        return [l]
        #    else:
        #        return l

        submodule_type = extract_from_kwargs("submodule", "StochasticSampling")
        self.feature_dim = extract_from_kwargs("feature_dim", 32)

        forward_activation = extract_from_kwargs("forward_activation", nn.ReLU)
        inverse_activation = extract_from_kwargs("inverse_activation", nn.ReLU)
        feature_activation = extract_from_kwargs("feature_activation", nn.ReLU)

        feature_net_hiddens = extract_from_kwargs("feature_net_hiddens", [64])
        inverse_net_hiddens = extract_from_kwargs("inverse_net_hiddens", [64])
        forward_net_hiddens = extract_from_kwargs("forward_net_hiddens", [64])

        super().__init__(
            action_space=action_space, framework=framework, **kwargs)

        # TODO: what should this look like for multidimensional obs spaces
        self.obs_space_dim = kwargs["model"].obs_space.shape[0]
        # TODO can we always assume 1
        self.action_space_dim = 1

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
                        activation_fn=activation))
            return nn.Sequential(*layers)

        # List of dimension of each layer. Appends the hidden dims.
        feature_dims = [self.obs_space_dim
                        ] + feature_net_hiddens + [self.feature_dim]
        inverse_dims = [2 * self.feature_dim
                        ] + inverse_net_hiddens + [self.action_space_dim]
        forward_dims = [self.feature_dim + self.action_space_dim] + \
            forward_net_hiddens + [self.feature_dim]

        # Creates actual models
        self.feature_model = create_fc_net(feature_dims, feature_activation)
        self.inverse_model = create_fc_net(inverse_dims, inverse_activation)
        self.forward_model = create_fc_net(forward_dims, forward_activation)

        # Convenient reductions
        #self.criterion = torch.nn.MSELoss(reduction="none")
        #self.criterion_reduced = torch.nn.MSELoss(reduction="sum")

        # This is only used to select the correct action
        self.exploration_submodule = from_config(
            cls=Exploration,
            config={
                "type": submodule_type,
                "action_space": action_space,
                "framework": framework,
                "policy_config": self.policy_config,
                "model": self.model,
                "num_workers": self.num_workers,
                "worker_index": self.worker_index
            })

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution,
            timestep=timestep,
            explore=explore)

    def get_exploration_loss(self, policy_loss, sample_batch: SampleBatchType):
        """
        Returns the intrinsic reward associated to the explorations strategy
            policy_loss (TensorType): The loss from the policy, not associated
                to the exploration strategy, which we will modify
            sample_batch (SampleBatchType): The SampleBatch of observations, to
                which we will associate an intrinsic loss.
        """

        # Cast to torch tensors, to be fed into the model
        obs_list = sample_batch["obs"].float()
        next_obs_list = sample_batch["new_obs"].float()
        emb_next_obs_list = self._get_latent_vector(next_obs_list).float()
        actions_list = sample_batch["actions"].float()

        actions_pred = self._predict_action(obs_list, next_obs_list)
        embedding_pred = self._predict_next_obs(obs_list, actions_list)

        # L2 losses for predicted action and next state
        embedding_loss = self.criterion_reduced(emb_next_obs_list,
                                                embedding_pred)
        actions_loss = self.criterion_reduced(
            actions_pred.squeeze(1), actions_list)
        return policy_loss + [embedding_loss + actions_loss]

    def _get_latent_vector(self, obs: TensorType) -> TensorType:
        """
        Returns the embedded vector phi(state)
            obs (TensorType): a batch of states
        """
        return self.feature_model(obs)

    def get_exploration_optimizers(self, config: TrainerConfigDict):
        """Returns optimizer (or list) for environmental dynamics networks.
        """
        forward_params = list(self.forward_model.parameters())
        inverse_params = list(self.inverse_model.parameters())
        feature_params = list(self.feature_model.parameters())

        return torch.optim.Adam(
            forward_params + inverse_params + feature_params, lr=1e-3)

    def postprocess_trajectory(self,
                               policy,
                               sample_batch: SampleBatchType,
                               tf_sess: Optional["tf.Session"] = None):
        """Calculates intrinsic rewards and adds them to "rewards" in batch.

        Calculations are based on difference between predicted and actually
        observed next states (predicted phi' vs observed phi').
        """

        # Extract the relevant data from the SampleBatch, and cast to Tensors
        obs = torch.from_numpy(sample_batch[SampleBatch.OBS]).float()
        next_obs = torch.from_numpy(sample_batch[SampleBatch.NEXT_OBS]).float()
        next_state = self._get_latent_vector(next_obs).float()
        actions = torch.from_numpy(sample_batch[SampleBatch.ACTIONS]).float()

        predicted_next_state = self._predict_next_obs(obs, actions)

        # Calculate the L2 difference between the actually observed next_state
        # (phi') and the predicted one.
        intrinsic_reward = torch.sum(
            torch.pow(next_state - predicted_next_state, 2.0), dim=-1)

        # Modifies environment rewards by subtracting intrinsic rewards
        sample_batch["rewards"] = sample_batch["rewards"] - \
            self.eta * intrinsic_reward.detach().numpy()

    def _predict_action(self, obs: TensorType, next_obs: TensorType):
        """Returns the predicted action, given two states.

        Uses the inverse dynamics model.

        obs (TensorType): Observation at time t.
        next_obs (TensorType): Observation at time t+1
        """
        state = self._get_latent_vector(obs)
        next_state = self._get_latent_vector(next_obs)
        return self.inverse_model(torch.cat(state, next_state, dim=-1))

    def _predict_next_state(self, obs: TensorType, action: TensorType):
        """Returns the predicted next state, given an action and current state.

        Args:
            obs (TensorType): Observation at time t.
            action (TensorType): Action taken at time t

        Returns:
            TensorType: The predicted next state (feature vector phi').
        """
        state = self._get_latent_vector(obs)
        inputs = torch.cat(
            state,
            F.one_hot(action, num_classes=self.action_space.n), dim=-1)
        return self.forward_model(inputs)
