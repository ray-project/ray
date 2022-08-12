"""TensorFlow policy class used for R2D2."""

from typing import Dict, List, Optional, Tuple

import gym
import ray
from ray.rllib.algorithms.dqn.dqn_tf_policy import (
    clip_gradients,
    compute_q_values,
    PRIO_WEIGHTS,
    postprocess_nstep_and_prio,
)
from ray.rllib.algorithms.dqn.dqn_tf_policy import build_q_model
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import (
    LearningRateSchedule,
    TargetNetworkMixin,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import huber_loss
from ray.rllib.utils.typing import ModelInputDict, TensorType, AlgorithmConfigDict

tf1, tf, tfv = try_import_tf()


def build_r2d2_model(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> Tuple[ModelV2, ActionDistribution]:
    """Build q_model and target_model for DQN

    Args:
        policy: The policy, which will use the model for optimization.
        obs_space (gym.spaces.Space): The policy's observation space.
        action_space (gym.spaces.Space): The policy's action space.
        config (AlgorithmConfigDict):

    Returns:
        q_model
            Note: The target q model will not be returned, just assigned to
            `policy.target_model`.
    """

    # Create the policy's models.
    model = build_q_model(policy, obs_space, action_space, config)

    # Assert correct model type by checking the init state to be present.
    # For attention nets: These don't necessarily publish their init state via
    # Model.get_initial_state, but may only use the trajectory view API
    # (view_requirements).
    assert (
        model.get_initial_state() != []
        or model.view_requirements.get("state_in_0") is not None
    ), (
        "R2D2 requires its model to be a recurrent one! Try using "
        "`model.use_lstm` or `model.use_attention` in your config "
        "to auto-wrap your model with an LSTM- or attention net."
    )

    return model


def r2d2_loss(policy: Policy, model, _, train_batch: SampleBatch) -> TensorType:
    """Constructs the loss for R2D2TFPolicy.

    Args:
        policy: The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        train_batch: The training data.

    Returns:
        TensorType: A single loss tensor.
    """
    config = policy.config

    # Construct internal state inputs.
    i = 0
    state_batches = []
    while "state_in_{}".format(i) in train_batch:
        state_batches.append(train_batch["state_in_{}".format(i)])
        i += 1
    assert state_batches

    # Q-network evaluation (at t).
    q, _, _, _ = compute_q_values(
        policy,
        model,
        train_batch,
        state_batches=state_batches,
        seq_lens=train_batch.get(SampleBatch.SEQ_LENS),
        explore=False,
        is_training=True,
    )

    # Target Q-network evaluation (at t+1).
    q_target, _, _, _ = compute_q_values(
        policy,
        policy.target_model,
        train_batch,
        state_batches=state_batches,
        seq_lens=train_batch.get(SampleBatch.SEQ_LENS),
        explore=False,
        is_training=True,
    )

    if not hasattr(policy, "target_q_func_vars"):
        policy.target_q_func_vars = policy.target_model.variables()

    actions = tf.cast(train_batch[SampleBatch.ACTIONS], tf.int64)
    dones = tf.cast(train_batch[SampleBatch.DONES], tf.float32)
    rewards = train_batch[SampleBatch.REWARDS]
    weights = tf.cast(train_batch[PRIO_WEIGHTS], tf.float32)

    B = tf.shape(state_batches[0])[0]
    T = tf.shape(q)[0] // B

    # Q scores for actions which we know were selected in the given state.
    one_hot_selection = tf.one_hot(actions, policy.action_space.n)
    q_selected = tf.reduce_sum(
        tf.where(q > tf.float32.min, q, tf.zeros_like(q)) * one_hot_selection, axis=1
    )

    if config["double_q"]:
        best_actions = tf.argmax(q, axis=1)
    else:
        best_actions = tf.argmax(q_target, axis=1)

    best_actions_one_hot = tf.one_hot(best_actions, policy.action_space.n)
    q_target_best = tf.reduce_sum(
        tf.where(q_target > tf.float32.min, q_target, tf.zeros_like(q_target))
        * best_actions_one_hot,
        axis=1,
    )

    if config["num_atoms"] > 1:
        raise ValueError("Distributional R2D2 not supported yet!")
    else:
        q_target_best_masked_tp1 = (1.0 - dones) * tf.concat(
            [q_target_best[1:], tf.constant([0.0])], axis=0
        )

        if config["use_h_function"]:
            h_inv = h_inverse(q_target_best_masked_tp1, config["h_function_epsilon"])
            target = h_function(
                rewards + config["gamma"] ** config["n_step"] * h_inv,
                config["h_function_epsilon"],
            )
        else:
            target = (
                rewards + config["gamma"] ** config["n_step"] * q_target_best_masked_tp1
            )

        # Seq-mask all loss-related terms.
        seq_mask = tf.sequence_mask(train_batch[SampleBatch.SEQ_LENS], T)[:, :-1]
        # Mask away also the burn-in sequence at the beginning.
        burn_in = policy.config["replay_buffer_config"]["replay_burn_in"]
        # Making sure, this works for both static graph and eager.
        if burn_in > 0:
            seq_mask = tf.cond(
                pred=tf.convert_to_tensor(burn_in, tf.int32) < T,
                true_fn=lambda: tf.concat(
                    [tf.fill([B, burn_in], False), seq_mask[:, burn_in:]], 1
                ),
                false_fn=lambda: seq_mask,
            )

        def reduce_mean_valid(t):
            return tf.reduce_mean(tf.boolean_mask(t, seq_mask))

        # Make sure to use the correct time indices:
        # Q(t) - [gamma * r + Q^(t+1)]
        q_selected = tf.reshape(q_selected, [B, T])[:, :-1]
        td_error = q_selected - tf.stop_gradient(tf.reshape(target, [B, T])[:, :-1])
        td_error = td_error * tf.cast(seq_mask, tf.float32)
        weights = tf.reshape(weights, [B, T])[:, :-1]
        policy._total_loss = reduce_mean_valid(weights * huber_loss(td_error))
        # Store the TD-error per time chunk (b/c we need only one mean
        # prioritized replay weight per stored sequence).
        policy._td_error = tf.reduce_mean(td_error, axis=-1)
        policy._loss_stats = {
            "mean_q": reduce_mean_valid(q_selected),
            "min_q": tf.reduce_min(q_selected),
            "max_q": tf.reduce_max(q_selected),
            "mean_td_error": reduce_mean_valid(td_error),
        }

    return policy._total_loss


def h_function(x, epsilon=1.0):
    """h-function to normalize target Qs, described in the paper [1].

    h(x) = sign(x) * [sqrt(abs(x) + 1) - 1] + epsilon * x

    Used in [1] in combination with h_inverse:
      targets = h(r + gamma * h_inverse(Q^))
    """
    return tf.sign(x) * (tf.sqrt(tf.abs(x) + 1.0) - 1.0) + epsilon * x


def h_inverse(x, epsilon=1.0):
    """Inverse if the above h-function, described in the paper [1].

    If x > 0.0:
    h-1(x) = [2eps * x + (2eps + 1) - sqrt(4eps x + (2eps + 1)^2)] /
        (2 * eps^2)

    If x < 0.0:
    h-1(x) = [2eps * x + (2eps + 1) + sqrt(-4eps x + (2eps + 1)^2)] /
        (2 * eps^2)
    """
    two_epsilon = epsilon * 2
    if_x_pos = (
        two_epsilon * x
        + (two_epsilon + 1.0)
        - tf.sqrt(4.0 * epsilon * x + (two_epsilon + 1.0) ** 2)
    ) / (2.0 * epsilon ** 2)
    if_x_neg = (
        two_epsilon * x
        - (two_epsilon + 1.0)
        + tf.sqrt(-4.0 * epsilon * x + (two_epsilon + 1.0) ** 2)
    ) / (2.0 * epsilon ** 2)
    return tf.where(x < 0.0, if_x_neg, if_x_pos)


class ComputeTDErrorMixin:
    """Assign the `compute_td_error` method to the R2D2TFPolicy

    This allows us to prioritize on the worker side.
    """

    def __init__(self):
        def compute_td_error(
            obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights
        ):
            input_dict = self._lazy_tensor_dict({SampleBatch.CUR_OBS: obs_t})
            input_dict[SampleBatch.ACTIONS] = act_t
            input_dict[SampleBatch.REWARDS] = rew_t
            input_dict[SampleBatch.NEXT_OBS] = obs_tp1
            input_dict[SampleBatch.DONES] = done_mask
            input_dict[PRIO_WEIGHTS] = importance_weights

            # Do forward pass on loss to update td error attribute
            r2d2_loss(self, self.model, None, input_dict)

            return self._td_error

        self.compute_td_error = compute_td_error


def get_distribution_inputs_and_class(
    policy: Policy,
    model: ModelV2,
    *,
    input_dict: ModelInputDict,
    state_batches: Optional[List[TensorType]] = None,
    seq_lens: Optional[TensorType] = None,
    explore: bool = True,
    is_training: bool = False,
    **kwargs
) -> Tuple[TensorType, type, List[TensorType]]:

    if policy.config["framework"] == "torch":
        from ray.rllib.algorithms.r2d2.r2d2_torch_policy import (
            compute_q_values as torch_compute_q_values,
        )

        func = torch_compute_q_values
    else:
        func = compute_q_values

    q_vals, logits, probs_or_logits, state_out = func(
        policy, model, input_dict, state_batches, seq_lens, explore, is_training
    )

    policy.q_values = q_vals
    if not hasattr(policy, "q_func_vars"):
        policy.q_func_vars = model.variables()

    action_dist_class = (
        TorchCategorical if policy.config["framework"] == "torch" else Categorical
    )

    return policy.q_values, action_dist_class, state_out


def adam_optimizer(
    policy: Policy, config: AlgorithmConfigDict
) -> "tf.keras.optimizers.Optimizer":
    return tf1.train.AdamOptimizer(
        learning_rate=policy.cur_lr, epsilon=config["adam_epsilon"]
    )


def build_q_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    return dict(
        {
            "cur_lr": policy.cur_lr,
        },
        **policy._loss_stats
    )


def setup_early_mixins(
    policy: Policy, obs_space, action_space, config: AlgorithmConfigDict
) -> None:
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def before_loss_init(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    ComputeTDErrorMixin.__init__(policy)
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


R2D2TFPolicy = build_tf_policy(
    name="R2D2TFPolicy",
    loss_fn=r2d2_loss,
    get_default_config=lambda: ray.rllib.algorithms.r2d2.r2d2.R2D2_DEFAULT_CONFIG,
    postprocess_fn=postprocess_nstep_and_prio,
    stats_fn=build_q_stats,
    make_model=build_r2d2_model,
    action_distribution_fn=get_distribution_inputs_and_class,
    optimizer_fn=adam_optimizer,
    extra_action_out_fn=lambda policy: {"q_values": policy.q_values},
    compute_gradients_fn=clip_gradients,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy._td_error},
    before_init=setup_early_mixins,
    before_loss_init=before_loss_init,
    mixins=[
        TargetNetworkMixin,
        ComputeTDErrorMixin,
        LearningRateSchedule,
    ],
)
