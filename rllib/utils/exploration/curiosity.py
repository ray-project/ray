from gym.spaces import Discrete, Space
from typing import Optional, Tuple, Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_torch, TensorType
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.typing import FromConfigSpec, ModelConfigDict, \
    SampleBatchType

torch, nn = try_import_torch()
F = None
if nn is not None:
    F = nn.functional


class Curiosity(Exploration):
    """Implementation of:
    [1] Curiosity-driven Exploration by Self-supervised Prediction
    Pathak, Agrawal, Efros, and Darrell - UC Berkeley - ICML 2017.
    https://arxiv.org/pdf/1705.05363.pdf

    Learns a simplified model of the environment based on three networks:
    1) Embedding observations into latent space ("feature" network).
    2) Predicting the action, given two consecutive embedded observations
        ("inverse" network).
    3) Predicting the next embedded obs, given an obs and action
        ("forward" network).

    The less the agent is able to predict the actually observed next feature
    vector, given obs and action (through the forwards network), the larger the
    "intrinsic reward", which will be added to the extrinsic reward.
    Therefore, if a state transition was unexpected, the agent becomes
    "curious" and will further explore this transition leading to better
    exploration in sparse rewards environments.
    """

    def __init__(self,
                 action_space: Space,
                 *,
                 framework: str,
                 model: ModelV2,
                 feature_dim: int = 288,
                 feature_net_config: Optional[ModelConfigDict] = None,
                 inverse_net_hiddens: Tuple[int] = (256, ),
                 inverse_net_activation: str = "relu",
                 forward_net_hiddens: Tuple[int] = (256, ),
                 forward_net_activation: str = "relu",
                 beta: float = 0.2,
                 eta: float = 1.0,
                 lr: float = 1e-3,
                 sub_exploration: Optional[FromConfigSpec] = None,
                 **kwargs):
        """Initializes a Curiosity object.

        Uses as defaults the hyperparameters described in [1].

        Args:
             feature_dim (int): The dimensionality of the feature (phi)
                vectors.
             feature_net_config (Optional[ModelConfigDict]): Optional model
                configuration for the feature network, producing feature
                vectors (phi) from observations. This can be used to configure
                fcnet- or conv_net setups to properly process any observation
                space.
             inverse_net_hiddens (Tuple[int]): Tuple of the layer sizes of the
                inverse (action predicting) NN head (on top of the feature
                outputs for phi and phi').
             inverse_net_activation (str): Activation specifier for the inverse
                net.
             forward_net_hiddens (Tuple[int]): Tuple of the layer sizes of the
                forward (phi' predicting) NN head.
             forward_net_activation (str): Activation specifier for the forward
                net.
             beta (float): Weight for the forward loss (over the inverse loss,
                which gets weight=1.0-beta) in the common loss term.
             eta (float): Weight for intrinsic rewards before being added to
                extrinsic ones.
             lr (float): The learning rate for the curiosity-specific
                optimizer, optimizing feature-, inverse-, and forward nets.
             sub_exploration (Optional[FromConfigSpec]): The config dict for
                the underlying Exploration to use (e.g. epsilon-greedy for
                DQN). If None, uses the FromSpecDict provided in the Policy's
                default config.
        """
        if framework != "torch":
            raise ValueError("Only torch is currently supported for Curiosity")
        elif not isinstance(action_space, Discrete):
            raise ValueError(
                "Only Discrete action spaces supported for Curiosity so far.")

        super().__init__(
            action_space, model=model, framework=framework, **kwargs)

        self.feature_dim = feature_dim
        if feature_net_config is None:
            feature_net_config = self.policy_config["model"].copy()
        self.feature_net_config = feature_net_config
        self.inverse_net_hiddens = inverse_net_hiddens
        self.inverse_net_activation = inverse_net_activation
        self.forward_net_hiddens = forward_net_hiddens
        self.forward_net_activation = forward_net_activation

        self.beta = beta
        self.eta = eta
        self.lr = lr
        # TODO: (sven) if sub_exploration is None, use Trainer's default
        #  Exploration config.
        if sub_exploration is None:
            raise NotImplementedError
        self.sub_exploration = sub_exploration

        # Creates modules/layers inside the actual ModelV2.
        self._curiosity_feature_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.feature_dim,
            model_config=self.feature_net_config,
            framework=self.framework,
            name="feature_net",
        )

        self._curiosity_inverse_fcnet = self._create_fc_net(
            [2 * self.feature_dim] + list(self.inverse_net_hiddens) +
            [self.action_space.n], self.inverse_net_activation)

        self._curiosity_forward_fcnet = self._create_fc_net(
            [self.feature_dim + self.action_space.n
             ] + list(forward_net_hiddens) + [self.feature_dim],
            self.forward_net_activation)

        # This is only used to select the correct action
        self.exploration_submodule = from_config(
            cls=Exploration,
            config=self.sub_exploration,
            action_space=self.action_space,
            framework=self.framework,
            policy_config=self.policy_config,
            model=self.model,
            num_workers=self.num_workers,
            worker_index=self.worker_index,
        )

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution: ActionDistribution,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        # Simply delegate to sub-Exploration module.
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution,
            timestep=timestep,
            explore=explore)

    @override(Exploration)
    def get_exploration_optimizer(self, optimizers):
        feature_params = list(self._curiosity_feature_net.parameters())
        inverse_params = list(self._curiosity_inverse_fcnet.parameters())
        forward_params = list(self._curiosity_forward_fcnet.parameters())

        # Now that the Policy's own optimizer(s) have been created (from
        # the Model parameters (IMPORTANT: w/o(!) the curiosity params),
        # we can add our curiosity sub-modules to the Policy's Model.
        self.model._curiosity_feature_net = \
            self._curiosity_feature_net.to(self.device)
        self.model._curiosity_inverse_fcnet = \
            self._curiosity_inverse_fcnet.to(self.device)
        self.model._curiosity_forward_fcnet = \
            self._curiosity_forward_fcnet.to(self.device)

        # Add the Adam for curiosity NN updating to the Policy's optimizers.
        return optimizers + [
            torch.optim.Adam(
                forward_params + inverse_params + feature_params, lr=self.lr)
        ]

    @override(Exploration)
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        """Calculates phi values (obs, obs', and predicted obs') and ri.

        Stores calculated phi, phi' and predicted phi' as well as the intrinsic
        rewards in the batch for loss processing by the policy.
        """
        batch_size = sample_batch[SampleBatch.OBS].shape[0]
        phis, _ = self.model._curiosity_feature_net({
            SampleBatch.OBS: torch.cat([
                torch.from_numpy(sample_batch[SampleBatch.OBS]),
                torch.from_numpy(sample_batch[SampleBatch.NEXT_OBS])
            ])
        })
        phi, next_phi = phis[:batch_size], phis[batch_size:]

        # Detach phi from graph (should not backpropagate through feature net
        # for forward-loss).
        predicted_next_phi = self.model._curiosity_forward_fcnet(
            torch.cat(
                [
                    phi.detach(),
                    F.one_hot(
                        torch.from_numpy(
                            sample_batch[SampleBatch.ACTIONS]).long(),
                        num_classes=self.action_space.n).float()
                ],
                dim=-1))

        # Forward loss term (predicted phi', given phi and action vs actually
        # observed phi').
        forward_l2_norm_sqared = 0.5 * torch.sum(
            torch.pow(predicted_next_phi - next_phi, 2.0), dim=-1)
        # Scale forward loss by eta hyper-parameter.
        sample_batch[SampleBatch.REWARDS] = \
            sample_batch[SampleBatch.REWARDS] + \
            self.eta * forward_l2_norm_sqared.detach().cpu().numpy()
        return sample_batch

    @override(Exploration)
    def get_exploration_loss(self, policy_loss, train_batch: SampleBatchType):
        """Adds the loss for the inverse and forward models to policy_loss.
        """
        batch_size = train_batch[SampleBatch.OBS].shape[0]
        phis, _ = self.model._curiosity_feature_net({
            SampleBatch.OBS: torch.cat(
                [
                    train_batch[SampleBatch.OBS],
                    train_batch[SampleBatch.NEXT_OBS]
                ],
                dim=0)
        })
        phi, next_phi = phis[:batch_size], phis[batch_size:]
        # Inverse loss term (prediced action that led from phi to phi' vs
        # actual action taken).
        phi_next_phi = torch.cat([phi, next_phi], dim=-1)
        dist_inputs = self.model._curiosity_inverse_fcnet(phi_next_phi)
        action_dist = TorchCategorical(dist_inputs, self.model)
        # Neg log(p); p=probability of observed action given the inverse-NN
        # predicted action distribution.
        inverse_loss = -action_dist.logp(train_batch[SampleBatch.ACTIONS])
        inverse_loss = torch.mean(inverse_loss)

        # Forward loss term has already been calculated during train batch pre-
        # processing (just have to weight with beta here).
        predicted_next_phi = self.model._curiosity_forward_fcnet(
            torch.cat(
                [
                    phi,
                    F.one_hot(
                        train_batch[SampleBatch.ACTIONS].long(),
                        num_classes=self.action_space.n).float()
                ],
                dim=-1))
        forward_loss = torch.mean(0.5 * torch.sum(
            torch.pow(predicted_next_phi - next_phi, 2.0), dim=-1))

        # Append our loss to the policy loss(es).
        return policy_loss + [
            (1.0 - self.beta) * inverse_loss + self.beta * forward_loss
        ]

    def _create_fc_net(self, layer_dims, activation):
        """Given a list of layer dimensions (incl. input-dim), creates FC-net.

        Args:
            layer_dims (Tuple[int]): Tuple of layer dims, including the input
                dimension.
            activation (str): An activation specifier string (e.g. "relu").


        Examples:
            If layer_dims is [4,8,6] we'll have a two layer net: 4->8 and 8->6.
        """
        layers = []
        for i in range(len(layer_dims) - 1):
            act = activation if i < len(layer_dims) - 2 else None
            layers.append(
                SlimFC(
                    in_size=layer_dims[i],
                    out_size=layer_dims[i + 1],
                    activation_fn=act))
        return nn.Sequential(*layers)
