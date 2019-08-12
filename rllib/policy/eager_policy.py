from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils import try_import_tf
from ray.rllib.policy import policy
from ray.rllib.models import catalog
from ray.rllib.models.tf import fcnet_v2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils.annotations import DeveloperAPI

tf = try_import_tf()


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

        samples = {
            SampleBatch.CUR_OBS: tf.convert_to_tensor(
                samples[SampleBatch.CUR_OBS], dtype=tf.float32),
            SampleBatch.ACTIONS: tf.convert_to_tensor(
                samples[SampleBatch.ACTIONS], dtype=tf.int32),
            Postprocessing.ADVANTAGES: tf.convert_to_tensor(
                samples[Postprocessing.ADVANTAGES], dtype=tf.float32),
        }

        with tf.GradientTape() as tape:
            seq_len = tf.ones(len(samples[SampleBatch.CUR_OBS]))
            model_out, states = self.model(samples, None, seq_len)
            self.action_dist = self.dist_class(model_out)
            loss = self.loss(self, samples)
            stats = self.stats(self, samples)

        vars = self.model.trainable_variables()
        grads = tape.gradient(loss, vars)
        self.optimizer.apply_gradients(zip(grads, vars))
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


def build_tf_policy(name, postprocess_fn, loss_fn,
                    make_optimizer, get_default_config=None, stats_fn=None):
    catalog.ModelCatalog.register_custom_model("keras_model",
                                               fcnet_v2.FullyConnectedNetwork)
    class EagerPolicy(TFEagerPolicy):
        def __init__(self, observation_space, action_space, config):
            if get_default_config:
                config = dict(get_default_config(), **config)

            self.dist_class, logit_dim = catalog.ModelCatalog.get_action_dist(
                action_space, config["model"])

            model = catalog.ModelCatalog.get_model_v2(
                observation_space,
                action_space,
                logit_dim,
                config["model"],
                framework="tf",
            )
            optimizer = make_optimizer(self, observation_space, action_space,
                                       config)
            self.config = config
            TFEagerPolicy.__init__(self, optimizer, model, observation_space,
                                   action_space)

        def postprocess_trajectory(self, samples, other_agent_batches=None,
                                   episode=None):
            return postprocess_fn(self, samples)

        def compute_actions(self,
                            obs_batch,
                            state_batches,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            **kwargs):
            seq_len = tf.ones(len(obs_batch))
            input_dict = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(
                    obs_batch, dtype=tf.float32),
                SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                    prev_action_batch, dtype=tf.int32),
                SampleBatch.PREV_REWARDS: tf.convert_to_tensor(
                    prev_reward_batch, dtype=tf.float32),
                "is_training": tf.convert_to_tensor(True),
            }
            model_out, states = self.model(input_dict, state_batches, seq_len)

            actions_dist = self.dist_class(model_out)
            return actions_dist.sample().numpy(), states, {}

        def loss(self, outputs, samples):
            return loss_fn(outputs, samples)

        def stats(self, outputs, samples):
            if stats_fn is None:
                return {}
            return stats_fn(outputs, samples)

    EagerPolicy.__name__ = name
    return EagerPolicy
