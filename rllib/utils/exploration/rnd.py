from gym.spaces import Space
import logging
from typing import Dict, Optional, Union
import numpy as np

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.misc import normc_initializer as tf_normc_initializer
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.misc import normc_initializer as torch_normc_initializer

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import NullContextManager
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.tf_utils import get_placeholder, make_tf_callable
from ray.rllib.utils.typing import (
    FromConfigSpec,
    List,
    ModelConfigDict,
    ModelWeights,
    TensorType,
)
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


logger = logging.getLogger(__name__)

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()
F = None
if nn is not None:
    F = nn.functional


class RND(Exploration):
    """Implements RND exploration criterion.
    Implementation of:
    [2] Exploration By Random Network Distillation.
    Burda, Edwards, Storkey & Klimov (2018).
    7th International Conference on Learning Representations (ICLR 2019)
    Computes an exploration bonus of a state by a distilled network approach
    This exploration bonus (also called novelty) defines the intrinsic reward
    in this exploration module and is added to the extrinsic reward.
    Novelty is actually a very general approach and any novelty measure
    could be used. Here the distillation error is used.
    RND has been shown to work well in both, deterministic and stochastic
    environments and to resolve the hard-exploration problem.
    """

    def __init__(
        self,
        action_space: Space,
        *,
        framework: str,
        model: ModelV2,
        tf_sess,
        embed_dim: int = 128,
        distill_net_config: Optional[ModelConfigDict] = None,
        lr: float = 1e-3,
        intrinsic_reward_coeff: float = 5e-3,
        nonepisodic_returns: bool = False,
        gamma: float = 0.99,
        lambda_: float = 0.95,
        vf_loss_coeff: float = None,
        adv_int_coeff: float = 1.0,
        adv_ext_coeff: float = 2.0,
        normalize: bool = True,
        random_timesteps: int = 10000,
        sub_exploration: Optional[FromConfigSpec] = None,
        **kwargs,
    ):
        """Initializes an RND exploration scheme.
        Args:
            action_space: The action space of the environment.
            framework: The ml framework used to train the model.
                Can be either one of ["tf", "tf2", "torch"].
                tf: TensorFlow (static-graph); tf2: TensorFlow 2.x
                (eager or traced, if eager_tracing=True); torch: PyTorch.
                This should be the same framework as used in the Trainer.
            embed_dim: The embedding dimension of the distillation networks
                used to compute the novelty of a state. This is the output
                size of the distillation networks. A larger embedding size
                will generalize less and therefore states have to be very
                similar for the intrinsic reward to shrink to zero. Note
                that large embedding sizes will necessarily result in slower
                training times for the agent as the distillation network is
                trained for one iteration after each single rollout.
            distill_net_config: An optional model configuration for the
                distillation networks. If None, the configuration of the
                Policy model is used.
            lr: The learning rate of the distillation network optimizer. The
                optimizer used is `Adam`. Note the network usually approaches
                its target easily. Too high learning rates will result in
                the intrinsic rewards vanishing faster and exploring similar
                states results in smaller rewards. Learning rates too small
                cause the opposite effect: intrinsic rewards are getting still
                paid for highly similar states even though they are not that
                novel anymore.
            intrinsic_reward_coeff: Scaling factor of the next state's intrinsic
                reward. The default value appears to be a good choice. Values
                too high might be contraproductive leading to no signal from the
                original (sparse) rewards. Values too low make exploration less
                efficient as the agent has less incentive to do so.
            normalize: Indicates, if intrinsic rewards should be normalized. In
                experiments with distillation networks a normalization of intrinsic
                rewards results in more stable exploration (after a burn-in).
                Rewards are normalized by a logarithmic scaling using the intrinsic
                rewards moving standard deviation.
            random_timesteps: The number of timesteps to act fully random when the
                default sub-exploration is used (`subexploration=None`).
            subexploration: The config dict for the underlying Exploration
                to use (e.g. epsilon-greedy for DQN). If None, `EpsilonGreedy` is
                used with a `PiecewiseSchedule`, i.e. using the `random_timesteps`.
        """

        super().__init__(action_space, framework=framework, model=model, **kwargs)

        # Check for parallel execution.
        if self.policy_config["num_workers"] != 0:
            self.global_update = True
            logger.warning(
                "RND uses a global update for the exploration model. "
                "This could lead to less frequent updates than needed. "
                "If more frequent updates are needed consider either "
                "decreasing the `train_batch_size` or setting the "
                "`num_workers` to zero."
            )

        self.embed_dim = embed_dim
        # In case no configuration is passed in, use the Policy's model config.
        # it has the same input dimensions as needed for the distill network.
        if distill_net_config is None:
            distill_net_config = self.policy_config["model"].copy()
        self.distill_net_config = distill_net_config
        self.lr = lr
        self.intrinsic_reward_coeff = intrinsic_reward_coeff

        # TODO: Check which algorithms could use non-episodic returns.
        # TODO: Use also de-central update for this. Everything should work:
        #   Just add the loss and you are done.
        # Include non-episodic returns.
        self.nonepisodic_returns = nonepisodic_returns
        # ------------ ENcapsulate ----------------------------
        if self.nonepisodic_returns:
            if self.policy_config["algo_class"].__name__ != "PPO":
                raise ValueError(
                    "RND does support non-episodic returns only for `'PPO'`. "
                    "Either change your training algorithm to `'PPO'` or "
                    "switch off non-episodic returns by setting "
                    "`nonepisodic_returns=False`."
                )
            if not self.global_update:
                # Set `global_update` to `True` to override the policy's loss
                # function and add the non-episodic value loss to the episodic
                # one.
                # TODO: This should actually also run with de-centralized updates.
                self.global_update = True
                logger.info(
                    "RND uses a global update to include non-episodic returns."
                    "This could lead to less frequent updates than needed. "
                    "If more frequent updates are needed consider either "
                    "decreasing the `train_batch_size` or set "
                    "`nonepisodic_returns=False`."
                )
            # Set hyper-parameters.
            self.gamma = gamma
            self.lambda_ = lambda_
            # Set the coefficient for the intrinsic value loss to the
            # coefficient used by the Policy if no value was provided.
            self.vf_loss_coeff = vf_loss_coeff
            if self.vf_loss_coeff is None:
                self.vf_loss_coeff = self.policy_config["vf_loss_coeff"]
            self.adv_int_coeff = adv_int_coeff
            self.adv_ext_coeff = adv_ext_coeff

            # TODO: Ensure that any model can use the value head (CNN, RNN, etc.)
            # Set up a second value head for non-episodic returns.
            # TODO: Ensure also non-separate case
            if self.framework == "torch":
                prev_layer_size = (
                    model._value_branch_separate[-1]._model[-2].out_features
                )
                model._exploration_value_branch = SlimFC(
                    in_size=prev_layer_size,
                    out_size=1,
                    initializer=torch_normc_initializer(0.01),
                    activation_fn=None,
                )
            else:
                # TensorFlow
                prev_layer = model.base_model.get_layer("value_out").input
                exploration_value_out = tf.keras.layers.Dense(
                    1,
                    name="exploration_value_out",
                    activation=None,
                    kernel_initializer=tf_normc_initializer(0.01),
                )(prev_layer)

                model.base_model = tf.keras.Model(
                    inputs=model.base_model.inputs,
                    outputs=model.base_model.output + [exploration_value_out],
                )

                def forward(
                    model,
                    input_dict: Dict[str, TensorType],
                    state: List[TensorType],
                    seq_lens: TensorType,
                ) -> (TensorType, List[TensorType]):
                    (
                        model_out,
                        model._value_out,
                        model._exploration_value_out,
                    ) = model.base_model(input_dict["obs_flat"])
                    return model_out, state

                model.forward = forward.__get__(model, type(model))

                def _exploration_value_branch(model) -> TensorType:
                    return tf.reshape(model._exploration_value_out, [-1])

                model._exploration_value_branch = _exploration_value_branch.__get__(
                    model, type(model)
                )
                self.model = model
                self._sess = tf_sess

                @make_tf_callable(self._sess)
                def _value_function(**input_dict):
                    input_dict = SampleBatch(input_dict)
                    model_out, _ = self.model(input_dict)
                    return self.model._exploration_value_branch()[0]

                self._value_function = _value_function
            # -------------------------------------------------------
            # TODO: Check, if this is really working on a reference.
            self.model = model

        self.normalize = normalize
        self._moving_mean_std = None

        if self.normalize:
            # Use the `_MovingMeanStd` class to normalize the intrinsic rewards.
            from ray.rllib.utils.exploration.random_encoder import _MovingMeanStd

            self._moving_mean_std = _MovingMeanStd()

        if sub_exploration is None:
            sub_exploration = {
                "type": "EpsilonGreedy",
                "epsilon_schedule": {
                    "type": "PiecewiseSchedule",
                    # Step function.
                    "endpoints": [
                        (0, 1.0),
                        (random_timesteps + 1, 1.0),
                        (random_timesteps + 2, 0.01),
                    ],
                    "outside_value": 0.01,
                },
            }
        self.sub_exploration = sub_exploration

        # Creates modules/layers inside the actual ModelV2 object.
        self._distill_predictor_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.embed_dim,
            model_config=self.distill_net_config,
            framework=self.framework,
            name="_distill_predictor_net",
        )
        self._distill_target_net = ModelCatalog.get_model_v2(
            self.model.obs_space,
            self.action_space,
            self.embed_dim,
            model_config=self.distill_net_config,
            framework=self.framework,
            name="_distill_target_net",
        )

        # This is only used to select the correct action.
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
    def get_exploration_action(
        self,
        *,
        action_distribution: ActionDistribution,
        timestep: Union[int, TensorType],
        explore: bool = True,
    ):
        # Simply delegate to sub-Exploration module.
        return self.exploration_submodule.get_exploration_action(
            action_distribution=action_distribution,
            timestep=timestep,
            explore=explore,
        )

    @override(Exploration)
    def get_exploration_optimizer(self, optimizers):
        """Prepares the optimizer for the distillation network."""

        if self.framework == "torch":
            # We do not train the target network.
            distill_params = list(self._distill_predictor_net.parameters())
            # TODO: Does this assign the preidctor net to the same
            # device as the policy model?
            self.model._distill_predictor_net = self._distill_predictor_net.to(
                self.device
            )
            self._optimizer = torch.optim.Adam(
                distill_params,
                lr=self.lr,
            )
        else:
            self.model._distill_predictor_net = self._distill_predictor_net

            # We do not train the target network.
            self._optimizer_var_list = self._distill_predictor_net.base_model.variables
            self._optimizer = tf1.train.AdamOptimizer(learning_rate=self.lr)

            # Create placeholders and initialize the loss.
            if self.framework == "tf":
                self._obs_ph = get_placeholder(
                    space=self.model.obs_space, name="_distill_obs"
                )
                # TODO: @simonsays1980: Check how it works with actions.
                (
                    self._novelty,
                    self._update_op,
                ) = self._postprocess_helper_tf(self._obs_ph)

        return optimizers

    @override(Exploration)
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        """Calculates phi values for the novelty and intrinsic reward.
        Also calculates distillation loss and updates the RND
        module on the provided batch using the optimizer.
        """
        if self.framework != "torch":
            self._postprocess_tf(policy, sample_batch, tf_sess)
        else:
            self._postprocess_torch(policy, sample_batch)

    # TODO: Make this dependent on nonepisodic_returns.
    @override(Exploration)
    def get_state(self, sess: Optional["tf.Session"] = None):
        """Returns the main variables of RND.
        This can be used for metrics. See the `RNDMetricsCallbacks`.
        """
        if self.nonepisodic_returns:
            return (
                self._intrinsic_reward_np,
                self._distill_loss_np,
                self._vf_intrinsic_loss_np,
            )
        else:
            return (
                self._intrinsic_reward_np,
                self._distill_loss_np,
            )

    @override(Exploration)
    def get_weights(self) -> ModelWeights:
        # TODO: Create the same for TF in base branch
        if self.framework == "torch":
            return {
                k: v.cpu().detach().numpy()
                for k, v in self._distill_predictor_net.state_dict().items()
            }
        else:
            return self._optimizer_var_list.get_weights()

    @override(Exploration)
    def set_weights(self, weights: ModelWeights):
        if self.framework == "torch":
            weights = convert_to_torch_tensor(weights, device=self.device)
            self._distill_predictor_net.load_state_dict(weights)
        else:
            self._optimizer_var_list.set_weights(weights)

    # TODO: Add typing
    @override(Exploration)
    def extra_action_out_fn(self, policy):
        extra_action_out = super().extra_action_out_fn(policy)

        if isinstance(self.model, tf.keras.Model):
            return extra_action_out

        extra_action_out.update(
            {"exploration_vf_preds": self.model._exploration_value_branch()}
        )
        return extra_action_out

    def _postprocess_tf(self, policy, sample_batch, tf_sess):
        """Calculates the intrinsic reward and updates the parameters."""
        # tf1 static-graph: Perform session call on our loss and update ops.
        if self.framework == "tf":
            if self.global_update:
                # In case of a global update only compute the novelty
                # but do not update here.
                self._novelty_np = tf_sess.run(
                    self._novelty,
                    feed_dict={
                        self._obs_ph: sample_batch[SampleBatch.OBS],
                    },
                )
            else:
                # Update the weights of the predictor network only, if the
                # update should happen locally.
                self._novelty_np, _ = tf_sess.run(
                    [self._novelty, self._update_op],
                    feed_dict={
                        self._obs_ph: sample_batch[SampleBatch.OBS],
                    },
                )
        # tf-eager: Perform model calls, loss calculation, and optimizer
        # stepping on the fly.
        else:
            # Update the weights of the predictor network only, if the
            # update should happen locally.
            novelty, _ = self._postprocess_helper_tf(
                sample_batch[SampleBatch.OBS],
                not self.global_update,
            )
            self._novelty_np = novelty.numpy()
        self._compute_intrinsic_reward(sample_batch)
        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)

        if not self.nonepisodic_returns:
            # Add intrinsic reward to extrinsic one.
            sample_batch[SampleBatch.REWARDS] = (
                sample_batch[SampleBatch.REWARDS] + self._intrinsic_reward_np
            )
        else:

            # TODO: this also can be merged together with Torch.
            # Calculate non-episodic returns and estimate value targets.
            last_r = self._predict_nonepisodic_value(sample_batch, policy, tf_sess)

            # TODO: This part can be merged together with Torch
            # Compute advantages and value targets.
            return self._compute_advantages(sample_batch, np.array([last_r]))
            # -------------------------- Encapsulate -------------------------

            # -------------------------------------------------------------------

        return sample_batch

    def _postprocess_helper_tf(self, obs, optimize: bool = True):
        """Computes novelty and gradients."""
        with (
            tf.GradientTape() if self.framework != "tf" else NullContextManager()
        ) as tape:

            novelty = self._compute_novelty(obs)
            # # Push observations through the distillation networks.
            # phi, _ = self.model._distill_predictor_net({SampleBatch.OBS: obs})
            # phi_target, _ = self._distill_target_net({SampleBatch.OBS: obs})
            # # Avoid dividing by zero in the gradient by adding a small epsilon.
            # novelty = tf.norm(phi - phi_target + 1e-12, axis=1)
            # TODO: @simonsays1980: Should be probably mean over dim=1 and then
            # sum over batches.
            distill_loss = tf.reduce_mean(novelty, axis=-1)

            update_op = None
            if optimize:
                # Step the optimizer
                if self.framework != "tf":
                    grads = tape.gradient(distill_loss, self._optimizer_var_list)
                    grads_and_vars = [
                        (g, v)
                        for g, v in zip(grads, self._optimizer_var_list)
                        if g is not None
                    ]
                    update_op = self._optimizer.apply_gradients(grads_and_vars)
                else:
                    update_op = self._optimizer.minimize(
                        distill_loss, var_list=self._optimizer_var_list
                    )

        return novelty, update_op

    def _postprocess_torch(self, policy, sample_batch):
        """Calculates the intrinsic reward and updates the parameters."""

        novelty = self._compute_novelty(
            torch.from_numpy(sample_batch[SampleBatch.OBS]).to(policy.device)
        )
        self._novelty_np = novelty.detach().cpu().numpy()
        # Calculate the intrinsic reward.
        self._compute_intrinsic_reward(sample_batch)

        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)

        if not self.nonepisodic_returns:
            # Add intrinsic reward to extrinsic one.
            sample_batch[SampleBatch.REWARDS] = (
                sample_batch[SampleBatch.REWARDS] + self._intrinsic_reward_np
            )
        else:
            # Calculate non-episodic returns and estimate value targets.
            last_r = (
                self._predict_nonepisodic_value(sample_batch, policy)
                .detach()
                .numpy()
                .reshape(-1)
            )
            # Get the non-episodic value predictions for all observations
            # in the trajectory.
            sample_batch["exploration_vf_preds"] = (
                policy.model._exploration_value_branch(
                    policy.model._value_branch_separate(
                        torch.from_numpy(sample_batch[SampleBatch.OBS]).to(
                            policy.device
                        )
                    )
                )
                .detach()
                .numpy()
                .reshape(-1)
            )
            # Compute advantages and value targets.
            sample_batch = self._compute_advantages(sample_batch, last_r)

        # When no global update is chosen, perform an optimization
        # step after each trajectory is collected.
        if not self.global_update:
            # Perform an optimizer step.
            distill_loss = torch.mean(novelty)
            self._optimizer.zero_grad()
            distill_loss.backward()
            self._optimizer.step()
            self._distill_loss_np = distill_loss.detach().numpy()

        return sample_batch

    @override(Exploration)
    def compute_loss_and_update(self, sample_batch, policy):

        if self.framework == "torch":
            novelty = self._compute_novelty(
                sample_batch[SampleBatch.OBS].to(policy.device)
            )
            # TODO: Policy is LSTM: Padded sequence mean_valid(novelty) from PPO
            distill_loss = torch.mean(novelty)
            self._optimizer.zero_grad()
            distill_loss.backward()
            self._optimizer.step()
            self._distill_loss_np = distill_loss.detach().numpy()

            if self.nonepisodic_returns:
                from ray.rllib.evaluation.postprocessing import Postprocessing

                value_fn_out = self._value_function(sample_batch, policy)
                # Compute the intrinsic value function loss to add to the
                # total PPO loss.
                vf_intrinsic_loss = (
                    torch.mean(
                        torch.pow(
                            value_fn_out - sample_batch["exploration_value_targets"],
                            2.0,
                        )
                    )
                    * self.vf_loss_coeff
                )
                self._vf_intrinsic_loss_np = vf_intrinsic_loss.detach().numpy()
                # Add intrinsic advantages to the extrinsic advantages.
                sample_batch[Postprocessing.ADVANTAGES] = (
                    self.adv_ext_coeff * sample_batch[Postprocessing.ADVANTAGES]
                    + self.adv_int_coeff * sample_batch["exploration_advantages"]
                )
            else:
                # Return zero, if non-episodic returns are not used.
                vf_intrinsic_loss = np.array([0.0])
        else:
            # Update the predictor network.
            self.novelty, _ = self._postprocess_helper_tf(
                sample_batch[SampleBatch.OBS], True
            )

            # Add non-episodic returns if needed.
            if self.nonepisodic_returns:
                from ray.rllib.evaluation.postprocessing import Postprocessing

                # Attach the RNDBatchCallbacks to compute non-episodic advantages.
                self._attach_rnd_batch_callbacks(policy)
                # TODO: Check if this cannot be optimized away with a cached fetch.
                model_out, _ = self.model(sample_batch)
                value_fn_out = self.model._exploration_value_branch()[0]
                _ = sample_batch["exploration_advantages"] * 2.0
                # value_fn_out = self._value_function(sample_batch, policy)
                # Compute the intrinsic value function loss to add to the
                # total PPO loss.
                # TODO: Check, if this has to be initialized at the beginning
                # and only calculated here.
                vf_intrinsic_loss = (
                    tf.reduce_mean(
                        tf.pow(
                            value_fn_out - sample_batch["exploration_value_targets"],
                            2.0,
                        ),
                        axis=-1,
                    )
                    * tf.constant(self.vf_loss_coeff)
                )
                if self.framework == "tf":
                    self._vf_intrinsic_loss_np = self._sess.run(vf_intrinsic_loss)
                else:
                    if not self.policy_config["eager_tracing"]:
                        self._vf_intrinsic_loss_np = vf_intrinsic_loss.numpy()
            else:
                # Else, return zero loss.
                vf_intrinsic_loss = tf.constant(0.0)

        return vf_intrinsic_loss

    def _compute_novelty(self, obs):
        # TODO: Include TF
        # Push observations through the distillation networks.
        phi, _ = self.model._distill_predictor_net(
            {
                SampleBatch.OBS: obs,
            }
        )
        phi_target, _ = self._distill_target_net(
            {
                SampleBatch.OBS: obs,
            }
        )
        # Avoid dividing by zero in the gradient by adding a small epsilon.
        if self.framework == "torch":
            novelty = torch.norm(phi - phi_target + 1e-12, dim=1)
        else:
            novelty = tf.norm(phi - phi_target + 1e-12, axis=1)

        return novelty

    def _compute_intrinsic_reward(self, sample_batch):
        """Computes the intrinsic reward."""
        self._intrinsic_reward_np = self._novelty_np * self.intrinsic_reward_coeff

    # TODO: Check if this holds for both TF and Torch
    def _predict_nonepisodic_value(self, sample_batch, policy, tf_sess=None):
        """Uses the non-episodic value head to predict a value.

        Note, this is done only for the next observation.
        """
        input_dict = sample_batch.get_single_step_input_dict(
            self.model.view_requirements, index="last"
        )
        if self.framework == "torch":
            return self._value_function(input_dict, policy)
        else:
            return self._value_function(**input_dict)

    # @make_tf_callable(self.get_session())
    def _value_function(self, input_dict, policy):
        """Calls the non-episodic value head."""
        if self.framework == "torch":
            input_dict = SampleBatch(input_dict)
            input_dict = policy._lazy_tensor_dict(input_dict)
            # TODO: Ensure it also runs with no separate value head.
            return self.model._exploration_value_branch(
                self.model._value_branch_separate(
                    input_dict[SampleBatch.OBS].to(policy.device)
                )
            )
        else:
            # For TensorFlow.
            model_out, _ = policy.model(input_dict)
            return self.model._exploration_value_branch()[0]

    def _compute_advantages(self, sample_batch, last_r):
        """Compute advantages for the non-episodic returns."""
        from ray.rllib.evaluation.postprocessing import discount_cumsum

        assert (
            "exploration_vf_preds" in sample_batch
        ), "Cannot use GAE in RND without value predictions."

        # Compare this computation with the computation of advantages in
        # `postprocessing.compute_advantages()`.
        vpred_t = np.concatenate([sample_batch["exploration_vf_preds"], last_r])
        delta_t = self._intrinsic_reward_np + self.gamma * vpred_t[1:] - vpred_t[:-1]
        sample_batch["exploration_advantages"] = discount_cumsum(
            delta_t, self.gamma * self.lambda_
        )
        sample_batch["exploration_value_targets"] = (
            sample_batch["exploration_advantages"]
            + sample_batch["exploration_vf_preds"]
        ).astype(np.float32)
        sample_batch["exploration_advantages"] = sample_batch[
            "exploration_advantages"
        ].astype(np.float32)
        sample_batch["exploration_vf_preds"].astype(np.float32)
        return sample_batch

    def _attach_rnd_batch_callbacks(self, policy):
        """Attaches the RNDBatchCallbacks to add non-episodic advantages.

        For TensorFlow 1.x this is needed as otherwise the `"advantages"`'
        placeholder is overwritten and cannot be used for feeding anymore.
        Using a callback solves this problem as in òn_learn_on_batch()` we
        work on the numpy training batch and not the placeholders.
        """
        from ray.rllib.utils.exploration.callbacks import RNDBatchCallbacks
        from ray.rllib.algorithms.callbacks import MultiCallbacks

        # Three cases can occur within the Policy's callbacks:
        #   1. Only the DefaultCallbacks.
        #   2. Only another single Callback.
        #   3. A MultiCallbacks object.
        if not isinstance(policy.callbacks, MultiCallbacks):
            if not isinstance(policy.callbacks, RNDBatchCallbacks):
                policy.callbacks = MultiCallbacks([policy.callbacks, RNDBatchCallbacks])
                logger.info(
                    "Attached RNDBatchCallbacks to policy callbacks. This enables "
                    "advantages computation with non-episodic returns."
                )
        else:
            if not issubclass(
                policy.callbacks._callback_class_list[-1], RNDBatchCallbacks
            ):
                policy.callbacks = MultiCallbacks(
                    [*policy.callbacks._callback_class_list, RNDBatchCallbacks]
                )
                logger.info(
                    "Attached RNDBatchCallbacks to policy callbacks. This enables "
                    "advantages computation with non-episodic returns."
                )
