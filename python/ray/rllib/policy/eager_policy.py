from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from ray.rllib.policy import policy
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class TFEagerPolicy(policy.Policy):

    @DeveloperAPI
    def __init__(self, optimizer, model, observation_space, action_space):
        self._optimizer = optimizer
        self._model = model
        self.observation_space = observation_space
        self.action_space = action_space

    @property
    def optimizer(self):
        return self._optimizer

    @property
    def model(self):
        return self._model

    @DeveloperAPI
    def loss(self, outputs, samples):
        raise NotImplementedError

    @DeveloperAPI
    def stats(self, outputs, samples):
        del outputs, samples
        return {}

    @DeveloperAPI
    def learn_on_batch(self, samples):
        """Fused compute gradients and apply gradients call.

        Either this or the combination of compute/apply grads must be
        implemented by subclasses.

        Returns:
            grad_info: dictionary of extra metadata from compute_gradients().

        Examples:
            >>> batch = ev.sample()
            >>> ev.learn_on_batch(samples)
        """

        samples = tf.nest.map_structure(tf.as_tensor, samples)
        with tf.gradient_tape() as tape:
            # Outputs of the model can be an arbitrary nested collection of
            # collection of tensors/arrays. The model packs what is
            # necessary.
            outputs = self.model(samples["obs"])
            loss = self.loss(outputs, samples)
            stats = self.stats(outputs, samples)
        grad = tape.gradient(loss)
        self.optimizer.apply(grad)
        return stats

    @DeveloperAPI
    def compute_gradients(self, postprocessed_batch):
        """Computes gradients against a batch of experiences.

        Either this or learn_on_batch() must be implemented by subclasses.

        Returns:
            grads (list): List of gradient output values
            info (dict): Extra policy-specific values
        """

        # Gradients for all trainable variables used when computing the loss
        # will be computed. Ideally, we would use variables returned by the
        # `self.model.trainable_variables()` but this might fail if variables
        # are created during the first call.
        grads, _ = zip(*self.optimizer.compute_gradients(
            loss=lambda: self.loss(postprocessed_batch),
        ))
        grads = tf.nest.map_structure(lambda grad: grad.numpy(), grads)

        return grads, {}

    @DeveloperAPI
    def apply_gradients(self, gradients):
        """Applies previously computed gradients.

        Either this or learn_on_batch() must be implemented by subclasses.
        """
        # TODO(gehring): figure out how to guarantee gradient values are
        #  applied to the correct variables.
        return NotImplemented

    @DeveloperAPI
    def get_weights(self):
        """Returns model weights.

        Returns:
            weights (obj): Serializable copy or view of model weights
        """
        # TODO(gehring): refactor `tf.nest` to `tf.contrib.framework.nest` if
        #  not `tf.nest` is not supported
        return tf.nest.map_structure(lambda var: var.numpy(),
                                     self.model.variables())

    @DeveloperAPI
    def set_weights(self, weights):
        """Sets model weights.

        Arguments:
            weights (obj): Serializable copy or view of model weights
        """
        tf.nest.map_structure(lambda var, value: var.assign(value),
                              self.model.variables(), weights)

    @DeveloperAPI
    def export_model(self, export_dir):
        """Export Policy to local directory for serving.

        Arguments:
            export_dir (str): Local writable directory.
        """
        # TODO(gehring): find the best way to export eager/TF2.0
        return NotImplemented

    @DeveloperAPI
    def export_checkpoint(self, export_dir):
        """Export Policy checkpoint to local directory.

        Argument:
            export_dir (str): Local writable directory.
        """
        # TODO(gehring): look into making ModelV2 trackable/checkpointable to
        #  facilitate this
        return NotImplemented


def build_tf_policy(name, postprocess_fn, loss_fn, make_model,
                    make_optimizer, get_default_config=None, stats_fn=None):

    class EagerPolicy(TFEagerPolicy):
        def __init__(self, observation_space, action_space, config):
            if get_default_config:
                config = dict(get_default_config(), **config)

            model = make_model(self, observation_space, action_space, config)
            optimizer = make_optimizer(self, observation_space, action_space,
                                       config)
            TFEagerPolicy.__init__(self, optimizer, model, observation_space,
                                   action_space)

        def postprocess_trajectory(self, samples):
            return postprocess_fn(samples)

        def compute_actions(self,
                            obs_batch,
                            state_batches,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            **kwargs):
            seq_len = tf.ones(len(obs_batch))
            actions = self.model(obs_batch, state_batches, seq_len)
            try:
                actions = actions.sample()
            except AttributeError:
                pass
            return actions

        def loss(self, outputs, samples):
            return loss_fn(outputs, samples)

        def stats(self, outputs, samples):
            if stats_fn is None:
                return {}
            return stats_fn(outputs, samples)

    EagerPolicy.__name__ = name
    return EagerPolicy
