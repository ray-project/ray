from typing import Any, Mapping

from ray.rllib.algorithms.dqn.dqn_learner import QF_PREDS, QF_TARGET_PREDS
from ray.rllib.algorithms.dqn.dqn_rl_module import DQNRLModule
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

_, tf, _ = try_import_tf()


class DQNTfRLModule(TfRLModule, DQNRLModule):
    @override
    def setup(self) -> None:
        super().setup(self)

        # Primary update of the target network's weigths.
        self.update_target_network()

    @override(TfRLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # State encoding.
        encoder_outs = self.q_encoder(batch)

        # TODO (simon): include RNN fucntionality.
        # if STATE_OUT in encoder_outs:

        # Q head.
        q_outs = self.q(encoder_outs[ENCODER_OUT])
        output[SampleBatch.ACTIONS] = tf.argmax(q_outs, axis=1)

        return output

    @override(TfRLModule)
    def _forward_exploration(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        output = {}

        # State encoding.
        encoder_outs = self.q_encoder(batch)

        # TODO (simon): include RNN functionality.
        # if STATE_OUT in encoder_outs:

        # Q head.
        q_outs = self.q(encoder_outs[ENCODER_OUT])

        # Include epsilon-greedy exploration.
        exploitation_actions = tf.argmax(q_outs, axis=1)
        epsilon = kwargs.get("epsilon", 0.3)

        # Draw random actions from categorical distributions
        # based on the q logits.
        random_actions = tf.squeeze(tf.random.categorical(q_outs, 1))
        B = tf.shape(q_outs)
        randomize = (
            tf.random.uniform(tf.stack([B]), minval=0.0, maxval=1.0, dtype=tf.float32)
            < epsilon
        )
        actions = tf.where(randomize, random_actions, exploitation_actions)
        output[SampleBatch.ACTIONS] = actions

        return output

    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # State encodings.
        q_encoder_outs = self.q_encoder(batch)
        target_encoder_outs = self.target_encoder(batch)

        # Q head.
        q_outs = self.q(q_encoder_outs[ENCODER_OUT])

        # For the q function we need to consider the actions taken.
        one_hot_selection = tf.one_hot(
            tf.cast(batch[SampleBatch.ACTIONS], tf.int32), self.config.action_space.n
        )
        q_t_selected = tf.reduce_sum(q_outs * one_hot_selection, 1)
        output[QF_PREDS] = q_t_selected

        # Target head.
        target_outs = self.target(target_encoder_outs[ENCODER_OUT])
        # For bootstrapping we assume the optimal action is taken.
        target_outs = tf.max(target_outs, axis=1)
        output[QF_TARGET_PREDS] = target_outs

        return output

    def update_target_network(self):
        """Updates (framework-specifically) the weights of the target network."""
        self.target_encoder.set_weights(self.q_encoder.get_weigths())
        self.target.set_weights(self.q.get_weights())
