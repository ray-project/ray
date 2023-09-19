from typing import Any, Mapping

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
            tf.random.uniform(tf.stack([B]), minval=0.0, maxval=1.0, dtype=tf.float32) < epsilon
        )
        actions = tf.where(randomize, random_actions, exploitation_actions)
        output[SampleBatch.ACTIONS] = actions

        return output
    
    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # State encoding.
        encoder_outs = self.q_encoder(batch)

        # Q head.
        q_outs = self.q(encoder_outs[ENCODER_OUT])

        # TODO (simon): Either override the `Encoder` for the target
        # or override the `input_specs_train` for the `target_encoder`
        # to accept the `SampleBatch.NEXT_OBS` instead of the 
        # `SampleBatch.OBS`.


    # TODO (kourosh, avnish): I see that there is an 
    # 'RLModuleWIthTargetNetworksInterface`. That one calls in the `Learner`
    # the get_target_network_pairs and updates the weights of the target 
    # inside the learner. 
    # This here could be called in the learner and contains already the
    # framwork-specific logic how to do it. What's your opinion on it? 
    def update_target_network(self):
        """Updates (framework-specifically) the weights of the target network."""
        self.target_encoder.set_weights(self.q_encoder.get_weigths())
        self.target.set_weights(self.q.get_weights())
