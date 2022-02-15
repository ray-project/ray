"""TensorFlow policy class used for SlateQ."""

import gym
import logging
import numpy as np
import time
from typing import Dict, List, Tuple, Type

import ray
from ray.rllib.agents.dqn.dqn_tf_policy import adam_optimizer, clip_gradients
from ray.rllib.agents.sac.sac_tf_policy import TargetNetworkMixin
from ray.rllib.agents.slateq.slateq_tf_model import SlateQTFModel
from ray.rllib.models.modelv2 import ModelV2, restore_original_dimensions
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import LearningRateSchedule
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import huber_loss
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


def build_slateq_model(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
):
    """Build models for SlateQ

    Args:
        policy: The policy, which will use the model for optimization.
        obs_space: The policy's observation space.
        action_space: The policy's action space.
        config: The Trainer's config dict.

    Returns:
        Tuple consisting of 1) Q-model and 2) an action distribution class.
    """

    #self._validate_states(states)

    #with tf.name_scope('network'):
    #    # Since we decompose the slate optimization into an item-level
    #    # optimization problem, the observation space is the user state
    #    # observation plus all documents' observations. In the Dopamine DQN agent
    #    # implementation, there is one head for each possible action value, which
    #    # is designed for computing the argmax operation in the action space.
    #    # In our implementation, we generate one output for each document.
    #    q_value_list = []
    #    for i in range(self._num_candidates):
    #        user = tf.squeeze(states[:, 0, :, :], axis=2)
    #        doc = tf.squeeze(states[:, i + 1, :, :], axis=2)
    #        q_value_list.append(self.network(user, doc, scope))
    #    q_values = tf.concat(q_value_list, axis=1)

    #return dqn_agent.DQNNetworkType(q_values)

    model = SlateQTFModel(
        obs_space,
        action_space,
        num_outputs=action_space.nvec[0],
        model_config=config["model"],
        name="slateq_model",
        embedding_size=obs_space.original_space["user"].shape[0],
        num_candidates=len(obs_space.original_space["doc"].spaces),
        fcnet_hiddens_per_candidate=config["fcnet_hiddens_per_candidate"],
    )

    policy.target_model = SlateQTFModel(
        obs_space,
        action_space,
        num_outputs=action_space.nvec[0],
        model_config=config["model"],
        name="target_slateq_model",
        embedding_size=obs_space.original_space["user"].shape[0],
        num_candidates=len(obs_space.original_space["doc"].spaces),
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
        Tuple consisting of 1) the choice loss- and 2) the Q-value loss tensors.
    """
    observation = restore_original_dimensions(
        train_batch[SampleBatch.OBS], policy.observation_space, tensorlib=tf
    )
    # user.shape: [batch_size, embedding_size]
    user_obs = observation["user"]
    # doc.shape: [batch_size, num_docs, embedding_size]
    doc_obs = list(observation["doc"].values())
    # action.shape: [batch_size, slate_size]
    actions = train_batch[SampleBatch.ACTIONS]

    slate_size = len(observation["response"])

    # click_indicator: [B, S]
    click_indicator = tf.stack([k["click"][:, 1] for k in observation["response"]], 1) #self._replay.rewards[:, :, self._click_response_index]
    # item_reward: [B, S]
    item_reward = tf.stack([k["watch_time"] for k in observation["response"]], 1)
    # q_values: [B, A]
    q_values = model.get_q_values(user_obs, doc_obs)
    # slate_q_values: [B, S]
    slate_q_values = tf.gather(
        q_values,
        tf.cast(actions, dtype=tf.int32), batch_dims=-1)

    # Only get the Q from the clicked document.
    # replay_click_q: [B]
    replay_click_q = tf.reduce_sum(
        input_tensor=slate_q_values * click_indicator,
        axis=1,
        name="replay_click_q")

    next_obs = restore_original_dimensions(
        train_batch[SampleBatch.NEXT_OBS], policy.observation_space, tensorlib=tf
    )
    # user.shape: [batch_size, embedding_size]
    user_next_obs = next_obs["user"]
    # doc.shape: [batch_size, num_docs, embedding_size]
    doc_next_obs = list(next_obs["doc"].values())
    #target = tf.stop_gradient(self._build_target_q_op())

    #click_indicator = self._replay.rewards[:, :, self._click_response_index]

    # Only compute the watch time reward of the clicked item.
    reward = tf.reduce_sum(input_tensor=item_reward * click_indicator, axis=1)


    next_q_values = policy.target_model.get_q_values(user_next_obs, doc_next_obs)
    scores, score_no_click = score_documents(user_next_obs, doc_next_obs)

    # Obtain all possible slates given current docs in the candidate set.
    #slate_size = next_actions.get_shape().as_list()[1]
    #num_candidates = next_q_values.get_shape().as_list()[1]
    #mesh_args = [list(range(num_candidates))] * slate_size
    #slates = tf.stack(tf.meshgrid(*mesh_args), axis=-1)
    #slates = tf.reshape(slates, shape=(-1, slate_size))
    # Filter slates that include duplicates to ensure each document is picked
    # at most once.
    #unique_mask = tf.map_fn(
    #    lambda x: tf.equal(tf.size(input=x), tf.size(input=tf.unique(x)[0])),
    #    slates,
    #    dtype=tf.bool)
    # [num_of_slates, slate_size]
    #slates = tf.boolean_mask(tensor=slates, mask=unique_mask)

    # [batch_size, num_of_slates, slate_size]
    next_q_values_slate = tf.gather(next_q_values, policy.slates, axis=1)
    # [batch_size, num_of_slates, slate_size]
    scores_slate = tf.gather(scores, policy.slates, axis=1)
    # [batch_size, num_of_slates]
    batch_size = user_next_obs.get_shape().as_list()[0]
    score_no_click_slate = tf.reshape(
      tf.tile(score_no_click,
              tf.shape(input=policy.slates)[:1]), [batch_size, -1])

    # [batch_size, num_of_slates]
    next_q_target_slate = tf.reduce_sum(
      input_tensor=next_q_values_slate * scores_slate, axis=2) / (
          tf.reduce_sum(input_tensor=scores_slate, axis=2) +
          score_no_click_slate)

    next_q_target_max = tf.reduce_max(input_tensor=next_q_target_slate, axis=1)

    target = reward + policy.config["gamma"] * next_q_target_max * (1. -
                                                 tf.cast(train_batch["dones"], tf.float32))






    #target = compute_target_optimal_q(
    #    reward=reward,
    #    gamma=policy.config["gamma"],
    #    slates=policy.slates,
    #    slate_size=slate_size,
    #    next_q_values=policy.target_model.get_q_values(user_next_obs, doc_next_obs),
    #    user_obs=user_next_obs,
    #    doc_obs=doc_next_obs,
    #    terminals=train_batch["dones"],
    #)
    target = tf.stop_gradient(target)

    clicked = tf.reduce_sum(input_tensor=click_indicator, axis=1)
    clicked_indices = tf.squeeze(tf.where(tf.equal(clicked, 1)), axis=1)
    # clicked_indices is a vector and tf.gather selects the batch dimension.
    q_clicked = tf.gather(replay_click_q, clicked_indices)
    target_clicked = tf.gather(target, clicked_indices)

    def get_train_op():
        td_error = q_clicked - target_clicked
        loss = huber_loss(td_error)
        loss = tf.reduce_mean(loss)
        return loss
      #if self.summary_writer is not None:
      #  with tf.variable_scope('Losses'):
      #    tf.summary.scalar('Loss', loss)

    #  return loss

    loss = tf.cond(
        pred=tf.greater(tf.reduce_sum(input_tensor=clicked), 0),
        true_fn=get_train_op,
        false_fn=lambda: tf.constant(0.),
        name='')

    policy._q_loss = loss
    policy._target = tf.reduce_mean(target)
    policy._q_clicked = tf.reduce_mean(q_clicked)
    policy._target_clicked = tf.reduce_mean(target_clicked)

    return loss


def build_slateq_stats(policy: Policy, batch) -> Dict[str, TensorType]:
    stats = {
        "q_loss": policy._q_loss,
        "target": policy._target,
        "q_clicked": policy._q_clicked,
        "target_clicked": policy._target_clicked,
        #"per_slate_q_values": policy._per_slate_q_values,
    }
        #"q_values": torch.mean(torch.stack(policy.get_tower_stats("q_values"))),
        #"next_q_values": torch.mean(
        #    torch.stack(policy.get_tower_stats("next_q_values"))
        #),
        #"next_q_minus_q": torch.mean(
        #    torch.stack(policy.get_tower_stats("next_q_minus_q"))
        #),
        #"target_q_values": torch.mean(
        #    torch.stack(policy.get_tower_stats("target_q_values"))
        #),
        #"td_error": torch.mean(torch.stack(policy.get_tower_stats("td_error"))),
        #"choice_loss": torch.mean(torch.stack(policy.get_tower_stats("choice_loss"))),
        #"raw_scores": torch.mean(torch.stack(policy.get_tower_stats("raw_scores"))),
        #"choice_beta": torch.mean(torch.stack(policy.get_tower_stats("choice_beta"))),
        #"choice_score_no_click": torch.mean(torch.stack(policy.get_tower_stats("choice_score_no_click"))),
    #}
    #model_stats = {
    #    k: torch.mean(var)
    #    for k, var in policy.model.trainable_variables(as_dict=True).items()
    #}
    #stats.update(model_stats)
    return stats


def action_distribution_fn(
    policy: Policy, model: SlateQTFModel, input_dict, *, explore, is_training, **kwargs
):
    """Determine which action to take"""

    # First, we transform the observation into its unflattened form.
    observation = restore_original_dimensions(
        input_dict[SampleBatch.CUR_OBS], policy.observation_space, tensorlib=tf
    )

    # user.shape: [batch_size(=1), embedding_size]
    user_obs = observation["user"]

    slate_size = len(policy.action_space.nvec)

    #if self.eval_mode:
    #    epsilon = self.epsilon_eval
    #else:
    #    epsilon = self.epsilon_fn(self.epsilon_decay_period, self.training_steps,
    #                              self.min_replay_history, self.epsilon_train)
    #    self._add_summary('epsilon', epsilon)

    doc_obs = list(observation["doc"].values())

    scores, score_no_click = score_documents(user_obs, doc_obs)

    q_values = model.get_q_values(user_obs, doc_obs)

    with tf.name_scope('select_slate'):
        per_slate_q_values = get_per_slate_q_values(
            policy.slates, slate_size, score_no_click, scores, q_values)
    #output_slate = select_slate_optimal(
    #      slate_size, score_no_click, scores, q_values)
    #self._output_slate = tf.Print(
    #    self._output_slate, [tf.constant('cp 1'), self._output_slate, p, q],
    #    summarize=10000)
    #output_slate = tf.reshape(output_slate, (slate_size,))
    model.slates = policy.slates
    #policy._per_slate_q_values = per_slate_q_values
    return per_slate_q_values, Categorical, []




    #output_slate, _ = self._sess.run(
    #    [self._output_slate, self._select_action_update_op], {
    #        #self.state_ph: self.state,
    #        #self._doc_affinity_scores_ph: scores,
    #        #self._prob_no_click_ph: score_no_click,
    #    })

    #return output_slate


#def select_slate_optimal(slates, slate_size, s_no_click, s, q):
#    """Selects the slate using exhaustive search.
#    This algorithm corresponds to the method "OS" in
#    Ie et al. https://arxiv.org/abs/1905.12767.
#    Args:
#    slate_size: int, the size of the recommendation slate.
#    s_no_click: float tensor, the score for not clicking any document.
#    s: [num_of_documents] tensor, the scores for clicking documents.
#    q: [num_of_documents] tensor, the predicted q values for documents.
#    Returns:
#    [slate_size] tensor, the selected slate.
#    """
#    per_slate_q_values, slates = get_per_slate_q_values(slates, slate_size, s_no_click, s, q)
#    max_q_slate_index = tf.argmax(input=per_slate_q_values)
#    return tf.gather(slates, max_q_slate_index, axis=0)


def get_per_slate_q_values(slates, slate_size, s_no_click, s, q):
    #num_candidates = s.shape.as_list()[1]

    ## Obtain all possible slates given current docs in the candidate set.
    #mesh_args = [list(range(num_candidates))] * slate_size
    #slates = tf.stack(tf.meshgrid(*mesh_args), axis=-1)
    #slates = tf.reshape(slates, shape=(-1, slate_size))

    ## Filter slates that include duplicates to ensure each document is picked
    ## at most once.
    #unique_mask = tf.map_fn(
    #    lambda x: tf.equal(tf.size(input=x), tf.size(input=tf.unique(x)[0])),
    #    slates,
    #    dtype=tf.bool)
    #slates = tf.boolean_mask(tensor=slates, mask=unique_mask)

    #repeat_slates = tf.tile(tf.expand_dims(slates, 0), [tf.shape(s)[0], 1, 1])

    slate_q_values = tf.gather(s * q, slates, axis=1)
    slate_scores = tf.gather(s, slates, axis=1)
    slate_normalizer = tf.reduce_sum(
        input_tensor=slate_scores, axis=2) + tf.expand_dims(s_no_click, 1)

    slate_q_values = slate_q_values / tf.expand_dims(slate_normalizer, 2)
    slate_sum_q_values = tf.reduce_sum(input_tensor=slate_q_values, axis=2)
    return slate_sum_q_values


def compute_target_optimal_q(reward, gamma, slates, slate_size, next_q_values,
                             user_obs, doc_obs, terminals):
    """Builds an op used as a target for the Q-value.

    This algorithm corresponds to the method "OT" in
    Ie et al. https://arxiv.org/abs/1905.12767..

    Args:
        reward: [batch_size] tensor, the immediate reward.
        gamma: float, discount factor with the usual RL meaning.
        next_q_values: [batch_size, num_of_documents] tensor, the q values of the
          documents in the next step.
        next_states: [batch_size, 1 + num_of_documents] tensor, the features for the
          user and the docuemnts in the next step.
        terminals: [batch_size] tensor, indicating if this is a terminal step.
    Returns:
        [batch_size] tensor, the target q values.
    """
    scores, score_no_click = score_documents(user_obs, doc_obs)

    # Obtain all possible slates given current docs in the candidate set.
    #slate_size = next_actions.get_shape().as_list()[1]
    #num_candidates = next_q_values.get_shape().as_list()[1]
    #mesh_args = [list(range(num_candidates))] * slate_size
    #slates = tf.stack(tf.meshgrid(*mesh_args), axis=-1)
    #slates = tf.reshape(slates, shape=(-1, slate_size))
    # Filter slates that include duplicates to ensure each document is picked
    # at most once.
    #unique_mask = tf.map_fn(
    #    lambda x: tf.equal(tf.size(input=x), tf.size(input=tf.unique(x)[0])),
    #    slates,
    #    dtype=tf.bool)
    # [num_of_slates, slate_size]
    #slates = tf.boolean_mask(tensor=slates, mask=unique_mask)

    # [batch_size, num_of_slates, slate_size]
    next_q_values_slate = tf.gather(next_q_values, slates, axis=1)
    # [batch_size, num_of_slates, slate_size]
    scores_slate = tf.gather(scores, slates, axis=1)
    # [batch_size, num_of_slates]
    batch_size = user_obs.get_shape().as_list()[0]
    score_no_click_slate = tf.reshape(
      tf.tile(score_no_click,
              tf.shape(input=slates)[:1]), [batch_size, -1])

    # [batch_size, num_of_slates]
    next_q_target_slate = tf.reduce_sum(
      input_tensor=next_q_values_slate * scores_slate, axis=2) / (
          tf.reduce_sum(input_tensor=scores_slate, axis=2) +
          score_no_click_slate)

    next_q_target_max = tf.reduce_max(input_tensor=next_q_target_slate, axis=1)

    return reward + gamma * next_q_target_max * (1. -
                                                 tf.cast(terminals, tf.float32))


def score_documents(user_obs,
                    doc_obs,
                    no_click_mass=1.0,
                    is_mnl=False,
                    min_normalizer=-1.0):
    """Computes unnormalized scores given both user and document observations.
    This implements both multinomial proportional model and multinormial logit
    model given some parameters. We also assume scores are based on inner
    products of user_obs and doc_obs.
    Args:
    user_obs: An instance of AbstractUserState.
    doc_obs: A numpy array that represents the observation of all documents in
      the candidate set.
    no_click_mass: a float indicating the mass given to a no click option
    is_mnl: whether to use a multinomial logit model instead of a multinomial
      proportional model.
    min_normalizer: A float (<= 0) used to offset the scores to be positive when
      using multinomial proportional model.
    Returns:
    A float tensor that stores unnormalzied scores of all candidate documents and a float
      tensor that represents the score for the action of picking no document.
    """
    #user_obs = tf.reshape(user_obs, [1, -1])
    # Dot product.
    scores_per_candidate = tf.reduce_sum(tf.multiply(tf.expand_dims(user_obs, 1), tf.stack(doc_obs, axis=1)),
                  2)
    #scores = tf.reduce_sum(input_tensor=tf.multiply(user_obs, doc_obs), axis=1)
    score_no_click = tf.fill([tf.shape(user_obs)[0], 1], no_click_mass)
    all_scores = tf.concat([scores_per_candidate, score_no_click], axis=1)
    # Logits: Softmax to yield probabilities.
    if is_mnl:
        all_scores = tf.nn.softmax(all_scores)
    # Multinomial proportional model: Shift to `[0.0,..[`.
    else:
        all_scores = all_scores - min_normalizer
    return all_scores[:, :-1], all_scores[:, -1]


def setup_early(policy, obs_space, action_space, config):
    # Obtain all possible slates given current docs in the candidate set.
    #slate_size = next_actions.get_shape().as_list()[1]
    num_candidates = action_space.nvec[0]
    slate_size = len(action_space.nvec)

    mesh_args = [list(range(num_candidates))] * slate_size
    slates = tf.stack(tf.meshgrid(*mesh_args), axis=-1)
    slates = tf.reshape(slates, shape=(-1, slate_size))
    # Filter slates that include duplicates to ensure each document is picked
    # at most once.
    unique_mask = tf.map_fn(
        lambda x: tf.equal(tf.size(input=x), tf.size(input=tf.unique(x)[0])),
        slates,
        dtype=tf.bool)
    # [num_of_slates, slate_size]
    slates = tf.boolean_mask(tensor=slates, mask=unique_mask)

    policy.slates = slates


def setup_mid_mixins(policy: Policy, obs_space, action_space, config) -> None:
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    """Call all mixin classes' constructors before SlateQTorchPolicy initialization.

    Args:
        policy: The Policy object.
        obs_space: The Policy's observation space.
        action_space: The Policy's action space.
        config: The Policy's config.
    """
    TargetNetworkMixin.__init__(policy, config)



def rmsprop_optimizer(
    policy: Policy, config: TrainerConfigDict
) -> "tf.keras.optimizers.Optimizer":
    if policy.config["framework"] in ["tf2", "tfe"]:
        return tf.keras.optimizers.RMSprop(
            learning_rate=policy.cur_lr,
            epsilon=0.00001,
            decay=0.95,
            momentum=0.0,
            centered=True,
        )
    else:
        return tf1.train.RMSPropOptimizer(
            learning_rate=policy.cur_lr,
            epsilon=0.00001,
            decay=0.95,
            momentum=0.0,
            centered=True,
        )


SlateQTFPolicy = build_tf_policy(
    name="SlateQTFPolicy",
    get_default_config=lambda: ray.rllib.agents.slateq.slateq.DEFAULT_CONFIG,
    after_init=setup_late_mixins,
    # Build model, loss functions, and optimizers
    make_model=build_slateq_model,
    loss_fn=build_slateq_losses,
    stats_fn=build_slateq_stats,
    optimizer_fn=rmsprop_optimizer,
    # Define how to act.
    action_distribution_fn=action_distribution_fn,
    compute_gradients_fn=clip_gradients,

    before_init=setup_early,
    before_loss_init=setup_mid_mixins,
    mixins=[LearningRateSchedule, TargetNetworkMixin],
)
