import ray
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.framework import try_import_tf, get_variable
from ray.rllib.utils.tf_ops import explained_variance, make_tf_callable

tf1, tf, tfv = try_import_tf()


class ValueNetworkMixin:
    def __init__(self, obs_space, action_space, config):

        # Input dict is provided to us automatically via the Model's
        # requirements. It's a single-timestep (last one in trajectory)
        # input_dict.
        if config["_use_trajectory_view_api"]:

            @make_tf_callable(self.get_session())
            def value(**input_dict):
                model_out, _ = self.model.from_batch(
                    input_dict, is_training=False)
                # [0] = remove the batch dim.
                return self.model.value_function()[0]

        # TODO: (sven) Remove once trajectory view API is all-algo default.
        else:

            @make_tf_callable(self.get_session())
            def value(ob, prev_action, prev_reward, *state):
                model_out, _ = self.model({
                    SampleBatch.CUR_OBS: tf.convert_to_tensor([ob]),
                    SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                        [prev_action]),
                    SampleBatch.PREV_REWARDS: tf.convert_to_tensor(
                        [prev_reward]),
                    "is_training": tf.convert_to_tensor(False),
                }, [tf.convert_to_tensor([s]) for s in state],
                                          tf.convert_to_tensor([1]))
                return self.model.value_function()[0]

        self._value = value


class ValueLoss:
    def __init__(self, state_values, cumulative_rewards):
        self.loss = 0.5 * tf.reduce_mean(
            tf.math.square(state_values - cumulative_rewards))


class ReweightedImitationLoss:
    def __init__(self, policy, state_values, cumulative_rewards, actions,
                 action_dist, beta):
        if beta != 0.0:
            # Advantage Estimation.
            adv = cumulative_rewards - state_values

            # Update averaged advantage norm.
            # Eager.
            if policy.config["framework"] in ["tf2", "tfe"]:
                policy._ma_adv_norm.assign_add(1e-6 * (
                    tf.reduce_mean(tf.math.square(adv)) - policy._ma_adv_norm))
                # Exponentially weighted advantages.
                exp_advs = tf.math.exp(beta * tf.math.divide(
                    adv, 1e-8 + tf.math.sqrt(policy._ma_adv_norm)))
            # Static graph.
            else:
                update_adv_norm = tf1.assign_add(
                    ref=policy._ma_adv_norm,
                    value=1e-6 * (tf.reduce_mean(tf.math.square(adv)) -
                                  policy._ma_adv_norm))

                # Exponentially weighted advantages.
                with tf1.control_dependencies([update_adv_norm]):
                    exp_advs = tf.math.exp(beta * tf.math.divide(
                        adv, 1e-8 + tf.math.sqrt(policy._ma_adv_norm)))
            exp_advs = tf.stop_gradient(exp_advs)
        else:
            exp_advs = 1.0

        # L = - A * log\pi_\theta(a|s)
        logprobs = action_dist.logp(actions)
        self.loss = -1.0 * tf.reduce_mean(exp_advs * logprobs)


def postprocess_advantages(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
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
        episode (Optional[MultiAgentEpisode]): Optional multi-agent episode
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
        if policy.config["_use_trajectory_view_api"]:
            # Create an input dict according to the Model's requirements.
            index = "last" if SampleBatch.NEXT_OBS in sample_batch.data else -1
            input_dict = policy.model.get_input_dict(sample_batch, index=index)
            last_r = policy._value(**input_dict)
        # TODO: (sven) Remove once trajectory view API is all-algo default.
        else:
            next_state = []
            for i in range(policy.num_state_tensors()):
                next_state.append(sample_batch["state_out_{}".format(i)][-1])
            last_r = policy._value(sample_batch[SampleBatch.NEXT_OBS][-1],
                                   sample_batch[SampleBatch.ACTIONS][-1],
                                   sample_batch[SampleBatch.REWARDS][-1],
                                   *next_state)

    # Adds the policy logits, VF preds, and advantages to the batch,
    # using GAE ("generalized advantage estimation") or not.
    return compute_advantages(
        sample_batch,
        last_r,
        policy.config["gamma"],
        use_gae=False,
        use_critic=False)


class MARWILLoss:
    def __init__(self, policy, state_values, action_dist, actions, advantages,
                 vf_loss_coeff, beta):

        self.v_loss = self._build_value_loss(state_values, advantages)
        self.p_loss = self._build_policy_loss(policy, state_values, advantages,
                                              actions, action_dist, beta)

        self.total_loss = self.p_loss.loss + vf_loss_coeff * self.v_loss.loss
        explained_var = explained_variance(advantages, state_values)
        self.explained_variance = tf.reduce_mean(explained_var)

    def _build_value_loss(self, state_values, cum_rwds):
        return ValueLoss(state_values, cum_rwds)

    def _build_policy_loss(self, policy, state_values, cum_rwds, actions,
                           action_dist, beta):
        return ReweightedImitationLoss(policy, state_values, cum_rwds, actions,
                                       action_dist, beta)


def marwil_loss(policy, model, dist_class, train_batch):
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)
    state_values = model.value_function()

    policy.loss = MARWILLoss(policy, state_values, action_dist,
                             train_batch[SampleBatch.ACTIONS],
                             train_batch[Postprocessing.ADVANTAGES],
                             policy.config["vf_coeff"], policy.config["beta"])

    return policy.loss.total_loss


def stats(policy, train_batch):
    return {
        "policy_loss": policy.loss.p_loss.loss,
        "vf_loss": policy.loss.v_loss.loss,
        "total_loss": policy.loss.total_loss,
        "vf_explained_var": policy.loss.explained_variance,
    }


def setup_mixins(policy, obs_space, action_space, config):
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    # Set up a tf-var for the moving avg (do this here to make it work with
    # eager mode).
    policy._ma_adv_norm = get_variable(
        100.0,
        framework="tf",
        tf_name="moving_average_of_advantage_norm",
        trainable=False)


MARWILTFPolicy = build_tf_policy(
    name="MARWILTFPolicy",
    get_default_config=lambda: ray.rllib.agents.marwil.marwil.DEFAULT_CONFIG,
    loss_fn=marwil_loss,
    stats_fn=stats,
    postprocess_fn=postprocess_advantages,
    before_loss_init=setup_mixins,
    mixins=[ValueNetworkMixin])
