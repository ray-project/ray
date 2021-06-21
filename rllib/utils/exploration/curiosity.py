from gym.spaces import Discrete, MultiDiscrete, Space
import numpy as np
from typing import Optional, Tuple, Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical, MultiCategorical
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_action_dist import TorchCategorical, \
    TorchMultiCategorical
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import NullContextManager
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, \
    try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.tf_ops import get_placeholder, one_hot as tf_one_hot
from ray.rllib.utils.torch_ops import one_hot
from ray.rllib.utils.typing import FromConfigSpec, ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()
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
        if not isinstance(action_space, (Discrete, MultiDiscrete)):
            raise ValueError(
                "Only (Multi)Discrete action spaces supported for Curiosity "
                "so far!")

        super().__init__(
            action_space, model=model, framework=framework, **kwargs)

        if self.policy_config["num_workers"] != 0:
            raise ValueError(
                "Curiosity exploration currently does not support parallelism."
                " `num_workers` must be 0!")

        self.feature_dim = feature_dim
        if feature_net_config is None:
            feature_net_config = self.policy_config["model"].copy()
        self.feature_net_config = feature_net_config
        self.inverse_net_hiddens = inverse_net_hiddens
        self.inverse_net_activation = inverse_net_activation
        self.forward_net_hiddens = forward_net_hiddens
        self.forward_net_activation = forward_net_activation

        self.action_dim = self.action_space.n if isinstance(
            self.action_space, Discrete) else np.sum(self.action_space.nvec)

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
            [self.action_dim],
            self.inverse_net_activation,
            name="inverse_net")

        self._curiosity_forward_fcnet = self._create_fc_net(
            [self.feature_dim + self.action_dim] + list(
                self.forward_net_hiddens) + [self.feature_dim],
            self.forward_net_activation,
            name="forward_net")

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
        # Create, but don't add Adam for curiosity NN updating to the policy.
        # If we added and returned it here, it would be used in the policy's
        # update loop, which we don't want (curiosity updating happens inside
        # `postprocess_trajectory`).
        if self.framework == "torch":
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
            self._optimizer = torch.optim.Adam(
                forward_params + inverse_params + feature_params, lr=self.lr)
        else:
            self.model._curiosity_feature_net = self._curiosity_feature_net
            self.model._curiosity_inverse_fcnet = self._curiosity_inverse_fcnet
            self.model._curiosity_forward_fcnet = self._curiosity_forward_fcnet
            # Feature net is a RLlib ModelV2, the other 2 are keras Models.
            self._optimizer_var_list = \
                self._curiosity_feature_net.base_model.variables + \
                self._curiosity_inverse_fcnet.variables + \
                self._curiosity_forward_fcnet.variables
            self._optimizer = tf1.train.AdamOptimizer(learning_rate=self.lr)
            # Create placeholders and initialize the loss.
            if self.framework == "tf":
                self._obs_ph = get_placeholder(
                    space=self.model.obs_space, name="_curiosity_obs")
                self._next_obs_ph = get_placeholder(
                    space=self.model.obs_space, name="_curiosity_next_obs")
                self._action_ph = get_placeholder(
                    space=self.model.action_space, name="_curiosity_action")
                self._forward_l2_norm_sqared, self._update_op = \
                    self._postprocess_helper_tf(
                        self._obs_ph, self._next_obs_ph, self._action_ph)

        return optimizers

    @override(Exploration)
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        """Calculates phi values (obs, obs', and predicted obs') and ri.

        Also calculates forward and inverse losses and updates the curiosity
        module on the provided batch using our optimizer.
        """
        if self.framework != "torch":
            self._postprocess_tf(policy, sample_batch, tf_sess)
        else:
            self._postprocess_torch(policy, sample_batch)

    def _postprocess_tf(self, policy, sample_batch, tf_sess):
        # tf1 static-graph: Perform session call on our loss and update ops.
        if self.framework == "tf":
            forward_l2_norm_sqared, _ = tf_sess.run(
                [self._forward_l2_norm_sqared, self._update_op],
                feed_dict={
                    self._obs_ph: sample_batch[SampleBatch.OBS],
                    self._next_obs_ph: sample_batch[SampleBatch.NEXT_OBS],
                    self._action_ph: sample_batch[SampleBatch.ACTIONS],
                })
        # tf-eager: Perform model calls, loss calculations, and optimizer
        # stepping on the fly.
        else:
            forward_l2_norm_sqared, _ = self._postprocess_helper_tf(
                sample_batch[SampleBatch.OBS],
                sample_batch[SampleBatch.NEXT_OBS],
                sample_batch[SampleBatch.ACTIONS],
            )
        # Scale intrinsic reward by eta hyper-parameter.
        sample_batch[SampleBatch.REWARDS] = \
            sample_batch[SampleBatch.REWARDS] + \
            self.eta * forward_l2_norm_sqared

        return sample_batch

    def _postprocess_helper_tf(self, obs, next_obs, actions):
        with (tf.GradientTape()
              if self.framework != "tf" else NullContextManager()) as tape:
            # Push both observations through feature net to get both phis.
            phis, _ = self.model._curiosity_feature_net({
                SampleBatch.OBS: tf.concat([obs, next_obs], axis=0)
            })
            phi, next_phi = tf.split(phis, 2)

            # Predict next phi with forward model.
            predicted_next_phi = self.model._curiosity_forward_fcnet(
                tf.concat(
                    [phi, tf_one_hot(actions, self.action_space)], axis=-1))

            # Forward loss term (predicted phi', given phi and action vs
            # actually observed phi').
            forward_l2_norm_sqared = 0.5 * tf.reduce_sum(
                tf.square(predicted_next_phi - next_phi), axis=-1)
            forward_loss = tf.reduce_mean(forward_l2_norm_sqared)

            # Inverse loss term (prediced action that led from phi to phi' vs
            # actual action taken).
            phi_cat_next_phi = tf.concat([phi, next_phi], axis=-1)
            dist_inputs = self.model._curiosity_inverse_fcnet(phi_cat_next_phi)
            action_dist = Categorical(dist_inputs, self.model) if \
                isinstance(self.action_space, Discrete) else \
                MultiCategorical(
                    dist_inputs, self.model, self.action_space.nvec)
            # Neg log(p); p=probability of observed action given the inverse-NN
            # predicted action distribution.
            inverse_loss = -action_dist.logp(actions)
            inverse_loss = tf.reduce_mean(inverse_loss)

            # Calculate the ICM loss.
            loss = (1.0 - self.beta) * inverse_loss + self.beta * forward_loss

        # Step the optimizer.
        if self.framework != "tf":
            grads = tape.gradient(loss, self._optimizer_var_list)
            grads_and_vars = [(g, v)
                              for g, v in zip(grads, self._optimizer_var_list)
                              if g is not None]
            update_op = self._optimizer.apply_gradients(grads_and_vars)
        else:
            update_op = self._optimizer.minimize(
                loss, var_list=self._optimizer_var_list)

        # Return the squared l2 norm and the optimizer update op.
        return forward_l2_norm_sqared, update_op

    def _postprocess_torch(self, policy, sample_batch):
        # Push both observations through feature net to get both phis.
        phis, _ = self.model._curiosity_feature_net({
            SampleBatch.OBS: torch.cat([
                torch.from_numpy(sample_batch[SampleBatch.OBS]),
                torch.from_numpy(sample_batch[SampleBatch.NEXT_OBS])
            ])
        })
        phi, next_phi = torch.chunk(phis, 2)
        actions_tensor = torch.from_numpy(
            sample_batch[SampleBatch.ACTIONS]).long().to(policy.device)

        # Predict next phi with forward model.
        predicted_next_phi = self.model._curiosity_forward_fcnet(
            torch.cat(
                [phi, one_hot(actions_tensor, self.action_space).float()],
                dim=-1))

        # Forward loss term (predicted phi', given phi and action vs actually
        # observed phi').
        forward_l2_norm_sqared = 0.5 * torch.sum(
            torch.pow(predicted_next_phi - next_phi, 2.0), dim=-1)
        forward_loss = torch.mean(forward_l2_norm_sqared)

        # Scale intrinsic reward by eta hyper-parameter.
        sample_batch[SampleBatch.REWARDS] = \
            sample_batch[SampleBatch.REWARDS] + \
            self.eta * forward_l2_norm_sqared.detach().cpu().numpy()

        # Inverse loss term (prediced action that led from phi to phi' vs
        # actual action taken).
        phi_cat_next_phi = torch.cat([phi, next_phi], dim=-1)
        dist_inputs = self.model._curiosity_inverse_fcnet(phi_cat_next_phi)
        action_dist = TorchCategorical(dist_inputs, self.model) if \
            isinstance(self.action_space, Discrete) else \
            TorchMultiCategorical(
                dist_inputs, self.model, self.action_space.nvec)
        # Neg log(p); p=probability of observed action given the inverse-NN
        # predicted action distribution.
        inverse_loss = -action_dist.logp(actions_tensor)
        inverse_loss = torch.mean(inverse_loss)

        # Calculate the ICM loss.
        loss = (1.0 - self.beta) * inverse_loss + self.beta * forward_loss
        # Perform an optimizer step.
        self._optimizer.zero_grad()
        loss.backward()
        self._optimizer.step()

        # Return the postprocessed sample batch (with the corrected rewards).
        return sample_batch

    def _create_fc_net(self, layer_dims, activation, name=None):
        """Given a list of layer dimensions (incl. input-dim), creates FC-net.

        Args:
            layer_dims (Tuple[int]): Tuple of layer dims, including the input
                dimension.
            activation (str): An activation specifier string (e.g. "relu").

        Examples:
            If layer_dims is [4,8,6] we'll have a two layer net: 4->8 (8 nodes)
            and 8->6 (6 nodes), where the second layer (6 nodes) does not have
            an activation anymore. 4 is the input dimension.
        """
        layers = [
            tf.keras.layers.Input(
                shape=(layer_dims[0], ), name="{}_in".format(name))
        ] if self.framework != "torch" else []

        for i in range(len(layer_dims) - 1):
            act = activation if i < len(layer_dims) - 2 else None
            if self.framework == "torch":
                layers.append(
                    SlimFC(
                        in_size=layer_dims[i],
                        out_size=layer_dims[i + 1],
                        initializer=torch.nn.init.xavier_uniform_,
                        activation_fn=act))
            else:
                layers.append(
                    tf.keras.layers.Dense(
                        units=layer_dims[i + 1],
                        activation=get_activation_fn(act),
                        name="{}_{}".format(name, i)))

        if self.framework == "torch":
            return nn.Sequential(*layers)
        else:
            return tf.keras.Sequential(layers)
