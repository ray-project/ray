import logging
import gym
from typing import Optional, Dict

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, Postprocessing
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import ValueNetworkMixin
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.framework import try_import_tf, get_variable
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.typing import TrainerConfigDict, TensorType, PolicyID
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorType,
)

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


# TODO(jungong) : Temporarily copied from ppo_tf_policy.
# Should simply use ComputeAndClipGradsMixIn once we migrate
# Marwil to extend DynamicTFPolicyV2.
def compute_and_clip_gradients(
    policy: Policy, optimizer: LocalOptimizer, loss: TensorType
) -> ModelGradients:
    """Gradients computing function (from loss tensor, using local optimizer).
    Args:
        policy (Policy): The Policy object that generated the loss tensor and
            that holds the given local optimizer.
        optimizer (LocalOptimizer): The tf (local) optimizer object to
            calculate the gradients with.
        loss (TensorType): The loss tensor for which gradients should be
            calculated.
    Returns:
        ModelGradients: List of the possibly clipped gradients- and variable
            tuples.
    """
    # Compute the gradients.
    variables = policy.model.trainable_variables
    if isinstance(policy.model, ModelV2):
        variables = variables()
    grads_and_vars = optimizer.compute_gradients(loss, variables)

    # Clip by global norm, if necessary.
    if policy.config["grad_clip"] is not None:
        # Defuse inf gradients (due to super large losses).
        grads = [g for (g, v) in grads_and_vars]
        grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
        # If the global_norm is inf -> All grads will be NaN. Stabilize this
        # here by setting them to 0.0. This will simply ignore destructive loss
        # calculations.
        policy.grads = [tf.where(tf.math.is_nan(g), tf.zeros_like(g), g) for g in grads]
        clipped_grads_and_vars = list(zip(policy.grads, variables))
        return clipped_grads_and_vars
    else:
        return grads_and_vars


def postprocess_advantages(
    policy: Policy,
    sample_batch: SampleBatch,
    other_agent_batches: Optional[Dict[PolicyID, SampleBatch]] = None,
    episode=None,
) -> SampleBatch:
    """Postprocesses a trajectory and returns the processed trajectory.

    The trajectory contains only data from one episode and from one agent.
    - If  `config.batch_mode=truncate_episodes` (default), sample_batch may
    contain a truncated (at-the-end) episode, in case the
    `config.rollout_fragment_length` was reached by the sampler.
    - If `config.batch_mode=complete_episodes`, sample_batch will contain
    exactly one episode (no matter how long).
    New columns can be added to sample_batch and existing ones may be altered.

    Args:
        policy (Policy): The Policy used to generate the trajectory
            (`sample_batch`)
        sample_batch (SampleBatch): The SampleBatch to postprocess.
        other_agent_batches (Optional[Dict[PolicyID, SampleBatch]]): Optional
            dict of AgentIDs mapping to other agents' trajectory data (from the
            same episode). NOTE: The other agents use the same policy.
        episode (Optional[Episode]): Optional multi-agent episode
            object in which the agents operated.

    Returns:
        SampleBatch: The postprocessed, modified SampleBatch (or a new one).
    """

    # Trajectory is actually complete -> last r=0.0.
    if sample_batch[SampleBatch.DONES][-1]:
        last_r = 0.0
    # Trajectory has been truncated -> last r=VF estimate of last obs.
    else:
        # Input dict is provided to us automatically via the Model's
        # requirements. It's a single-timestep (last one in trajectory)
        # input_dict.
        # Create an input dict according to the Model's requirements.
        index = "last" if SampleBatch.NEXT_OBS in sample_batch else -1
        input_dict = sample_batch.get_single_step_input_dict(
            policy.model.view_requirements, index=index
        )
        last_r = policy._value(**input_dict)

    # Adds the "advantages" (which in the case of MARWIL are simply the
    # discounted cummulative rewards) to the SampleBatch.
    return compute_advantages(
        sample_batch,
        last_r,
        policy.config["gamma"],
        # We just want the discounted cummulative rewards, so we won't need
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
            if policy.config["framework"] in ["tf2", "tfe"]:
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


def marwil_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: ActionDistribution,
    train_batch: SampleBatch,
) -> TensorType:
    model_out, _ = model(train_batch)
    action_dist = dist_class(model_out, model)
    value_estimates = model.value_function()

    policy.loss = MARWILLoss(
        policy,
        value_estimates,
        action_dist,
        train_batch,
        policy.config["vf_coeff"],
        policy.config["beta"],
    )

    return policy.loss.total_loss


def stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    stats = {
        "policy_loss": policy.loss.p_loss,
        "total_loss": policy.loss.total_loss,
    }
    if policy.config["beta"] != 0.0:
        stats["moving_average_sqd_adv_norm"] = policy._moving_average_sqd_adv_norm
        stats["vf_explained_var"] = policy.loss.explained_variance
        stats["vf_loss"] = policy.loss.v_loss

    return stats


def setup_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    # Setup Value branch of our NN.
    ValueNetworkMixin.__init__(policy, config)

    # Not needed for pure BC.
    if policy.config["beta"] != 0.0:
        # Set up a tf-var for the moving avg (do this here to make it work
        # with eager mode); "c^2" in the paper.
        policy._moving_average_sqd_adv_norm = get_variable(
            policy.config["moving_average_sqd_adv_norm_start"],
            framework="tf",
            tf_name="moving_average_of_advantage_norm",
            trainable=False,
        )


MARWILTFPolicy = build_tf_policy(
    name="MARWILTFPolicy",
    get_default_config=lambda: ray.rllib.agents.marwil.marwil.DEFAULT_CONFIG,
    loss_fn=marwil_loss,
    stats_fn=stats,
    postprocess_fn=postprocess_advantages,
    before_loss_init=setup_mixins,
    compute_gradients_fn=compute_and_clip_gradients,
    mixins=[ValueNetworkMixin],
)
