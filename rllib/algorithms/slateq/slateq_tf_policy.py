"""TensorFlow policy class used for SlateQ."""

import functools
import gym
import logging
import numpy as np
from typing import Dict

import ray
from ray.rllib.algorithms.dqn.dqn_tf_policy import clip_gradients
from ray.rllib.algorithms.sac.sac_tf_policy import TargetNetworkMixin
from ray.rllib.algorithms.slateq.slateq_tf_model import SlateQTFModel
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import SlateMultiCategorical
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_mixins import LearningRateSchedule
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import huber_loss
from ray.rllib.utils.typing import TensorType, AlgorithmConfigDict

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


def build_slateq_model(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> SlateQTFModel:
    """Build models for the SlateQTFPolicy.

    Args:
        policy: The policy, which will use the model for optimization.
        obs_space: The policy's observation space.
        action_space: The policy's action space.
        config: The Algorithm's config dict.

    Returns:
        The slate-Q specific Q-model instance.
    """
    model = SlateQTFModel(
        obs_space,
        action_space,
        num_outputs=action_space.nvec[0],
        model_config=config["model"],
        name="slateq_model",
        fcnet_hiddens_per_candidate=config["fcnet_hiddens_per_candidate"],
    )

    policy.target_model = SlateQTFModel(
        obs_space,
        action_space,
        num_outputs=action_space.nvec[0],
        model_config=config["model"],
        name="target_slateq_model",
        fcnet_hiddens_per_candidate=config["fcnet_hiddens_per_candidate"],
    )

    return model


def build_slateq_losses(
    policy: Policy,
    model: ModelV2,
    _,
    train_batch: SampleBatch,
) -> TensorType:
    """Constructs the choice- and Q-value losses for the SlateQTorchPolicy.

    Args:
        policy: The Policy to calculate the loss for.
        model: The Model to calculate the loss for.
        train_batch: The training data.

    Returns:
        The Q-value loss tensor.
    """

    # B=batch size
    # S=slate size
    # C=num candidates
    # E=embedding size
    # A=number of all possible slates

    # Q-value computations.
    # ---------------------
    observation = train_batch[SampleBatch.OBS]
    # user.shape: [B, E]
    user_obs = observation["user"]
    batch_size = tf.shape(user_obs)[0]
    # doc.shape: [B, C, E]
    doc_obs = list(observation["doc"].values())
    # action.shape: [B, S]
    actions = train_batch[SampleBatch.ACTIONS]

    # click_indicator.shape: [B, S]
    click_indicator = tf.cast(
        tf.stack([k["click"] for k in observation["response"]], 1), tf.float32
    )
    # item_reward.shape: [B, S]
    item_reward = tf.stack([k["watch_time"] for k in observation["response"]], 1)
    # q_values.shape: [B, C]
    q_values = model.get_q_values(user_obs, doc_obs)
    # slate_q_values.shape: [B, S]
    slate_q_values = tf.gather(
        q_values, tf.cast(actions, dtype=tf.int32), batch_dims=-1
    )
    # Only get the Q from the clicked document.
    # replay_click_q.shape: [B]
    replay_click_q = tf.reduce_sum(
        input_tensor=slate_q_values * click_indicator, axis=1, name="replay_click_q"
    )

    # Target computations.
    # --------------------
    next_obs = train_batch[SampleBatch.NEXT_OBS]

    # user.shape: [B, E]
    user_next_obs = next_obs["user"]
    # doc.shape: [B, C, E]
    doc_next_obs = list(next_obs["doc"].values())
    # Only compute the watch time reward of the clicked item.
    reward = tf.reduce_sum(input_tensor=item_reward * click_indicator, axis=1)

    # TODO: Find out, whether it's correct here to use obs, not next_obs!
    # Dopamine uses obs, then next_obs only for the score.
    # next_q_values = policy.target_model.get_q_values(user_next_obs, doc_next_obs)
    next_q_values = policy.target_model.get_q_values(user_obs, doc_obs)
    scores, score_no_click = score_documents(user_next_obs, doc_next_obs)

    # next_q_values_slate.shape: [B, A, S]
    next_q_values_slate = tf.gather(next_q_values, policy.slates, axis=1)
    # scores_slate.shape [B, A, S]
    scores_slate = tf.gather(scores, policy.slates, axis=1)
    # score_no_click_slate.shape: [B, A]
    score_no_click_slate = tf.reshape(
        tf.tile(score_no_click, tf.shape(input=policy.slates)[:1]), [batch_size, -1]
    )

    # next_q_target_slate.shape: [B, A]
    next_q_target_slate = tf.reduce_sum(
        input_tensor=next_q_values_slate * scores_slate, axis=2
    ) / (tf.reduce_sum(input_tensor=scores_slate, axis=2) + score_no_click_slate)
    next_q_target_max = tf.reduce_max(input_tensor=next_q_target_slate, axis=1)

    target = reward + policy.config["gamma"] * next_q_target_max * (
        1.0 - tf.cast(train_batch["dones"], tf.float32)
    )
    target = tf.stop_gradient(target)

    clicked = tf.reduce_sum(input_tensor=click_indicator, axis=1)
    clicked_indices = tf.squeeze(tf.where(tf.equal(clicked, 1)), axis=1)
    # Clicked_indices is a vector and tf.gather selects the batch dimension.
    q_clicked = tf.gather(replay_click_q, clicked_indices)
    target_clicked = tf.gather(target, clicked_indices)

    td_error = tf.where(
        tf.cast(clicked, tf.bool),
        replay_click_q - target,
        tf.zeros_like(train_batch[SampleBatch.REWARDS]),
    )
    if policy.config["use_huber"]:
        loss = huber_loss(td_error, delta=policy.config["huber_threshold"])
    else:
        loss = tf.math.square(td_error)
    loss = tf.reduce_mean(loss)
    td_error = tf.abs(td_error)
    mean_td_error = tf.reduce_mean(td_error)

    policy._q_values = tf.reduce_mean(q_values)
    policy._q_clicked = tf.reduce_mean(q_clicked)
    policy._scores = tf.reduce_mean(scores)
    policy._score_no_click = tf.reduce_mean(score_no_click)
    policy._slate_q_values = tf.reduce_mean(slate_q_values)
    policy._replay_click_q = tf.reduce_mean(replay_click_q)
    policy._bellman_reward = tf.reduce_mean(reward)
    policy._next_q_values = tf.reduce_mean(next_q_values)
    policy._target = tf.reduce_mean(target)
    policy._next_q_target_slate = tf.reduce_mean(next_q_target_slate)
    policy._next_q_target_max = tf.reduce_mean(next_q_target_max)
    policy._target_clicked = tf.reduce_mean(target_clicked)
    policy._q_loss = loss
    policy._td_error = td_error
    policy._mean_td_error = mean_td_error
    policy._mean_actions = tf.reduce_mean(actions)

    return loss


def build_slateq_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    stats = {
        "q_values": policy._q_values,
        "q_clicked": policy._q_clicked,
        "scores": policy._scores,
        "score_no_click": policy._score_no_click,
        "slate_q_values": policy._slate_q_values,
        "replay_click_q": policy._replay_click_q,
        "bellman_reward": policy._bellman_reward,
        "next_q_values": policy._next_q_values,
        "target": policy._target,
        "next_q_target_slate": policy._next_q_target_slate,
        "next_q_target_max": policy._next_q_target_max,
        "target_clicked": policy._target_clicked,
        "mean_td_error": policy._mean_td_error,
        "q_loss": policy._q_loss,
        "mean_actions": policy._mean_actions,
    }
    # if hasattr(policy, "_mean_grads_0"):
    #    stats.update({"mean_grads_0": policy._mean_grads_0})
    #    stats.update({"mean_grads_1": policy._mean_grads_1})
    #    stats.update({"mean_grads_2": policy._mean_grads_2})
    #    stats.update({"mean_grads_3": policy._mean_grads_3})
    #    stats.update({"mean_grads_4": policy._mean_grads_4})
    #    stats.update({"mean_grads_5": policy._mean_grads_5})
    #    stats.update({"mean_grads_6": policy._mean_grads_6})
    #    stats.update({"mean_grads_7": policy._mean_grads_7})
    return stats


def action_distribution_fn(
    policy: Policy, model: SlateQTFModel, input_dict, *, explore, is_training, **kwargs
):
    """Determine which action to take."""

    # First, we transform the observation into its unflattened form.
    observation = input_dict[SampleBatch.OBS]
    # user.shape: [B, E]
    user_obs = observation["user"]
    doc_obs = list(observation["doc"].values())

    # Compute scores per candidate.
    scores, score_no_click = score_documents(user_obs, doc_obs)
    # Compute Q-values per candidate.
    q_values = model.get_q_values(user_obs, doc_obs)

    with tf.name_scope("select_slate"):
        per_slate_q_values = get_per_slate_q_values(
            policy.slates, score_no_click, scores, q_values
        )
    return (
        per_slate_q_values,
        functools.partial(
            SlateMultiCategorical,
            action_space=policy.action_space,
            all_slates=policy.slates,
        ),
        [],
    )


def get_per_slate_q_values(slates, s_no_click, s, q):
    slate_q_values = tf.gather(s * q, slates, axis=1)
    slate_scores = tf.gather(s, slates, axis=1)
    slate_normalizer = tf.reduce_sum(
        input_tensor=slate_scores, axis=2
    ) + tf.expand_dims(s_no_click, 1)

    slate_q_values = slate_q_values / tf.expand_dims(slate_normalizer, 2)
    slate_sum_q_values = tf.reduce_sum(input_tensor=slate_q_values, axis=2)
    return slate_sum_q_values


def score_documents(
    user_obs, doc_obs, no_click_score=1.0, multinomial_logits=False, min_normalizer=-1.0
):
    """Computes dot-product scores for user vs doc (plus no-click) feature vectors."""

    # Dot product between used and each document feature vector.
    scores_per_candidate = tf.reduce_sum(
        tf.multiply(tf.expand_dims(user_obs, 1), tf.stack(doc_obs, axis=1)), 2
    )
    # Compile a constant no-click score tensor.
    score_no_click = tf.fill([tf.shape(user_obs)[0], 1], no_click_score)
    # Concatenate click and no-click scores.
    all_scores = tf.concat([scores_per_candidate, score_no_click], axis=1)

    # Logits: Softmax to yield probabilities.
    if multinomial_logits:
        all_scores = tf.nn.softmax(all_scores)
    # Multinomial proportional model: Shift to `[0.0,..[`.
    else:
        all_scores = all_scores - min_normalizer

    # Return click (per candidate document) and no-click scores.
    return all_scores[:, :-1], all_scores[:, -1]


def setup_early(policy, obs_space, action_space, config):
    """Obtain all possible slates given current docs in the candidate set."""

    num_candidates = action_space.nvec[0]
    slate_size = len(action_space.nvec)
    num_all_slates = np.prod([(num_candidates - i) for i in range(slate_size)])

    mesh_args = [list(range(num_candidates))] * slate_size
    slates = tf.stack(tf.meshgrid(*mesh_args), axis=-1)
    slates = tf.reshape(slates, shape=(-1, slate_size))
    # Filter slates that include duplicates to ensure each document is picked
    # at most once.
    unique_mask = tf.map_fn(
        lambda x: tf.equal(tf.size(input=x), tf.size(input=tf.unique(x)[0])),
        slates,
        dtype=tf.bool,
    )
    # slates.shape: [A, S]
    slates = tf.boolean_mask(tensor=slates, mask=unique_mask)
    slates.set_shape([num_all_slates, slate_size])

    # Store all possible slates only once in policy object.
    policy.slates = slates


def setup_mid_mixins(policy: Policy, obs_space, action_space, config) -> None:
    """Call mixin classes' constructors before SlateQTorchPolicy loss initialization.

    Args:
        policy: The Policy object.
        obs_space: The Policy's observation space.
        action_space: The Policy's action space.
        config: The Policy's config.
    """
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    """Call mixin classes' constructors after SlateQTorchPolicy loss initialization.

    Args:
        policy: The Policy object.
        obs_space: The Policy's observation space.
        action_space: The Policy's action space.
        config: The Policy's config.
    """
    TargetNetworkMixin.__init__(policy, config)


def rmsprop_optimizer(
    policy: Policy, config: AlgorithmConfigDict
) -> "tf.keras.optimizers.Optimizer":
    if policy.config["framework"] in ["tf2", "tfe"]:
        return tf.keras.optimizers.RMSprop(
            learning_rate=policy.cur_lr,
            epsilon=config["rmsprop_epsilon"],
            decay=0.95,
            momentum=0.0,
            centered=True,
        )
    else:
        return tf1.train.RMSPropOptimizer(
            learning_rate=policy.cur_lr,
            epsilon=config["rmsprop_epsilon"],
            decay=0.95,
            momentum=0.0,
            centered=True,
        )


SlateQTFPolicy = build_tf_policy(
    name="SlateQTFPolicy",
    get_default_config=lambda: ray.rllib.algorithms.slateq.slateq.DEFAULT_CONFIG,
    # Build model, loss functions, and optimizers
    make_model=build_slateq_model,
    loss_fn=build_slateq_losses,
    stats_fn=build_slateq_stats,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy._td_error},
    optimizer_fn=rmsprop_optimizer,
    # Define how to act.
    action_distribution_fn=action_distribution_fn,
    compute_gradients_fn=clip_gradients,
    before_init=setup_early,
    before_loss_init=setup_mid_mixins,
    after_init=setup_late_mixins,
    mixins=[LearningRateSchedule, TargetNetworkMixin],
)
