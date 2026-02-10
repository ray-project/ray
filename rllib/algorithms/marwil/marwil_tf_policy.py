import logging
from typing import Any, Dict, List, Optional, Type, Union

from ray.rllib.evaluation.postprocessing import Postprocessing, compute_advantages
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import (
    ValueNetworkMixin,
    compute_gradients,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import get_variable, try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorType,
)

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class PostprocessAdvantages:
    """Marwil's custom trajectory post-processing mixin."""

    def __init__(self):
        pass

    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[Dict[Any, SampleBatch]] = None,
        episode=None,
    ):
        sample_batch = super().postprocess_trajectory(
            sample_batch, other_agent_batches, episode
        )

        # Trajectory is actually complete -> last r=0.0.
        if sample_batch[SampleBatch.TERMINATEDS][-1]:
            last_r = 0.0
        # Trajectory has been truncated -> last r=VF estimate of last obs.
        else:
            # Input dict is provided to us automatically via the Model's
            # requirements. It's a single-timestep (last one in trajectory)
            # input_dict.
            # Create an input dict according to the Model's requirements.
            index = "last" if SampleBatch.NEXT_OBS in sample_batch else -1
            input_dict = sample_batch.get_single_step_input_dict(
                self.view_requirements, index=index
            )
            last_r = self._value(**input_dict)

        # Adds the "advantages" (which in the case of MARWIL are simply the
        # discounted cumulative rewards) to the SampleBatch.
        return compute_advantages(
            sample_batch,
            last_r,
            self.config["gamma"],
            # We just want the discounted cumulative rewards, so we won't need
            # GAE nor critic (use_critic=True: Subtract vf-estimates from returns).
            use_gae=False,
            use_critic=False,
        )


class MARWILLoss:
    def __init__(
        self,
        policy: Policy,
        value_estimates: TensorType,
        action_dist: ActionDistribution,
        train_batch: SampleBatch,
        vf_loss_coeff: float,
        beta: float,
    ):
        # L = - A * log\pi_\theta(a|s)
        logprobs = action_dist.logp(train_batch[SampleBatch.ACTIONS])
        if beta != 0.0:
            cumulative_rewards = train_batch[Postprocessing.ADVANTAGES]
            # Advantage Estimation.
            adv = cumulative_rewards - value_estimates
            adv_squared = tf.reduce_mean(tf.math.square(adv))
            # Value function's loss term (MSE).
            self.v_loss = 0.5 * adv_squared

            # Perform moving averaging of advantage^2.
            rate = policy.config["moving_average_sqd_adv_norm_update_rate"]

            # Update averaged advantage norm.
            # Eager.
            if policy.config["framework"] == "tf2":
                update_term = adv_squared - policy._moving_average_sqd_adv_norm
                policy._moving_average_sqd_adv_norm.assign_add(rate * update_term)

                # Exponentially weighted advantages.
                c = tf.math.sqrt(policy._moving_average_sqd_adv_norm)
                exp_advs = tf.math.exp(beta * (adv / (1e-8 + c)))
            # Static graph.
            else:
                update_adv_norm = tf1.assign_add(
                    ref=policy._moving_average_sqd_adv_norm,
                    value=rate * (adv_squared - policy._moving_average_sqd_adv_norm),
                )

                # Exponentially weighted advantages.
                with tf1.control_dependencies([update_adv_norm]):
                    exp_advs = tf.math.exp(
                        beta
                        * tf.math.divide(
                            adv,
                            1e-8 + tf.math.sqrt(policy._moving_average_sqd_adv_norm),
                        )
                    )
            exp_advs = tf.stop_gradient(exp_advs)

            self.explained_variance = tf.reduce_mean(
                explained_variance(cumulative_rewards, value_estimates)
            )

        else:
            # Value function's loss term (MSE).
            self.v_loss = tf.constant(0.0)
            exp_advs = 1.0

        # logprob loss alone tends to push action distributions to
        # have very low entropy, resulting in worse performance for
        # unfamiliar situations.
        # A scaled logstd loss term encourages stochasticity, thus
        # alleviate the problem to some extent.
        logstd_coeff = policy.config["bc_logstd_coeff"]
        if logstd_coeff > 0.0:
            logstds = tf.reduce_sum(action_dist.log_std, axis=1)
        else:
            logstds = 0.0

        self.p_loss = -1.0 * tf.reduce_mean(
            exp_advs * (logprobs + logstd_coeff * logstds)
        )

        self.total_loss = self.p_loss + vf_loss_coeff * self.v_loss


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_marwil_tf_policy(name: str, base: type) -> type:
    """Construct a MARWILTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with MAML.
    """

    class MARWILTFPolicy(ValueNetworkMixin, PostprocessAdvantages, base):
        def __init__(
            self,
            observation_space,
            action_space,
            config,
            existing_model=None,
            existing_inputs=None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            # Initialize base class.
            base.__init__(
                self,
                observation_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

            ValueNetworkMixin.__init__(self, config)
            PostprocessAdvantages.__init__(self)

            # Not needed for pure BC.
            if config["beta"] != 0.0:
                # Set up a tf-var for the moving avg (do this here to make it work
                # with eager mode); "c^2" in the paper.
                self._moving_average_sqd_adv_norm = get_variable(
                    config["moving_average_sqd_adv_norm_start"],
                    framework="tf",
                    tf_name="moving_average_of_advantage_norm",
                    trainable=False,
                )

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            model_out, _ = model(train_batch)
            action_dist = dist_class(model_out, model)
            value_estimates = model.value_function()

            self._marwil_loss = MARWILLoss(
                self,
                value_estimates,
                action_dist,
                train_batch,
                self.config["vf_coeff"],
                self.config["beta"],
            )

            return self._marwil_loss.total_loss

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            stats = {
                "policy_loss": self._marwil_loss.p_loss,
                "total_loss": self._marwil_loss.total_loss,
            }
            if self.config["beta"] != 0.0:
                stats["moving_average_sqd_adv_norm"] = self._moving_average_sqd_adv_norm
                stats["vf_explained_var"] = self._marwil_loss.explained_variance
                stats["vf_loss"] = self._marwil_loss.v_loss

            return stats

        @override(base)
        def compute_gradients_fn(
            self, optimizer: LocalOptimizer, loss: TensorType
        ) -> ModelGradients:
            return compute_gradients(self, optimizer, loss)

    MARWILTFPolicy.__name__ = name
    MARWILTFPolicy.__qualname__ = name

    return MARWILTFPolicy


MARWILTF1Policy = get_marwil_tf_policy("MARWILTF1Policy", DynamicTFPolicyV2)
MARWILTF2Policy = get_marwil_tf_policy("MARWILTF2Policy", EagerTFPolicyV2)
