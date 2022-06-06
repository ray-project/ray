from gym.spaces import Space
import logging
from typing import Optional, Union

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import NullContextManager
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.tf_utils import get_placeholder
from ray.rllib.utils.typing import FromConfigSpec, ModelConfigDict, TensorType

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
        embed_dim: int = 128,
        distill_net_config: Optional[ModelConfigDict] = None,
        lr: float = 1e-3,
        intrinsic_reward_coeff: float = 5e-3,
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

        if self.policy_config["num_workers"] != 0:
            raise ValueError(
                "RND exploration currently does not support parallelism."
                " `num_workers` must be 0!"
            )

        self.embed_dim = embed_dim
        # In case no configuration is passed in, use the Policy's model config.
        # it has the same input dimensions as needed for the distill network.
        if distill_net_config is None:
            distill_net_config = self.policy_config["model"].copy()
        self.distill_net_config = distill_net_config
        self.lr = lr
        self.intrinsic_reward_coeff = intrinsic_reward_coeff
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

    @override(Exploration)
    def get_state(self, sess: Optional["tf.Session"] = None):
        """Returns the main variables of RND.

        This can be used for metrics. See the `RNDMetricsCallbacks`.
        """
        return (self._intrinsic_reward_np,)

    def _postprocess_tf(self, policy, sample_batch, tf_sess):
        """Calculates the intrinsic reward and updates the parameters."""
        # tf1 static-graph: Perform session call on our loss and update ops.
        if self.framework == "tf":
            self._novelty_np, _ = tf_sess.run(
                [self._novelty, self._update_op],
                feed_dict={
                    self._obs_ph: sample_batch[SampleBatch.OBS],
                },
            )
        # tf-eager: Perform model calls, loss calculation, and optimizer
        # stepping on the fly.
        else:
            self._novelty_np, _ = self._postprocess_helper_tf(
                sample_batch[SampleBatch.OBS],
            )

        self._compute_intrinsic_reward(sample_batch)
        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)

        # Add intrinsic reward to extrinsic one.
        sample_batch[SampleBatch.REWARDS] = (
            sample_batch[SampleBatch.REWARDS]
            + self._intrinsic_reward_np * self.intrinsic_reward_coeff
        )

        return sample_batch

    def _postprocess_helper_tf(
        self,
        obs,
    ):
        """Computes novelty and gradients."""
        with (
            tf.GradientTape() if self.framework != "tf" else NullContextManager()
        ) as tape:
            # Push observations through the distillation networks.
            phi, _ = self.model._distill_predictor_net({SampleBatch.OBS: obs})
            phi_target, _ = self._distill_target_net({SampleBatch.OBS: obs})
            # Avoid dividing by zero in the gradient by adding a small epsilon.
            novelty = tf.norm(phi - phi_target + 1e-12, axis=1)
            # TODO: @simonsays1980: Should be probably mean over dim=1 and then
            # sum over batches.
            distill_loss = tf.reduce_mean(novelty, axis=-1)

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
        # Push observations through the distillation networks.
        phi, _ = self.model._distill_predictor_net(
            {
                SampleBatch.OBS: torch.from_numpy(sample_batch[SampleBatch.OBS]),
            }
        )
        phi_target, _ = self._distill_target_net(
            {
                SampleBatch.OBS: torch.from_numpy(sample_batch[SampleBatch.OBS]),
            }
        )
        # Avoid dividing by zero in the gradient by adding a small epsilon.
        novelty = torch.norm(phi - phi_target + 1e-12, dim=1)
        self._novelty_np = novelty.detach().cpu().numpy()

        # Calculate the intrinsic reward.
        self._compute_intrinsic_reward(sample_batch)

        if self.normalize:
            self._intrinsic_reward_np = self._moving_mean_std(self._intrinsic_reward_np)

        # Add intrinsic reward to extrinsic one.
        sample_batch[SampleBatch.REWARDS] = (
            sample_batch[SampleBatch.REWARDS]
            + self._intrinsic_reward_np * self.intrinsic_reward_coeff
        )

        # Perform an optimizer step.
        distill_loss = torch.mean(novelty)
        self._optimizer.zero_grad()
        distill_loss.backward()
        self._optimizer.step()

        return sample_batch

    def _compute_intrinsic_reward(self, sample_batch):
        """Computes the intrinsic reward."""
        self._intrinsic_reward_np = self._novelty_np
