from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils import try_import_tf
from ray.rllib.policy import policy
from ray.rllib.models import catalog
from ray.rllib.models.tf import fcnet_v2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy.tf_policy import ACTION_PROB, ACTION_LOGP
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import DeveloperAPI

tf = try_import_tf()


@DeveloperAPI
class TFEagerPolicy(policy.Policy):

    @DeveloperAPI
    def __init__(self, optimizer, model, observation_space, action_space):
        self.optimizer = optimizer
        self.model = model
        self.observation_space = observation_space
        self.action_space = action_space
        self._sess = None

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


def build_tf_policy(name, loss_fn,
                    get_default_config=None,
                    postprocess_fn=None,
                    stats_fn=None,
                    optimizer_fn=None,
                    gradients_fn=None,
                    extra_action_fetches_fn=None,
                    before_init=None,
                    before_loss_init=None,
                    mixins=None,
                    obs_include_prev_action_reward=True):

    base = add_mixins(TFEagerPolicy, mixins)

    if gradients_fn:
        print("WARNING: NOT IMPLEMENTED: custom gradients fn")

    class policy_cls(base):
        def __init__(self, observation_space, action_space, config):
            assert tf.executing_eagerly()

            if get_default_config:
                config = dict(get_default_config(), **config)
            self.config = config

            self.dist_class, logit_dim = catalog.ModelCatalog.get_action_dist(
                action_space, config["model"])

            model = catalog.ModelCatalog.get_model_v2(
                observation_space,
                action_space,
                logit_dim,
                config["model"],
                framework="tf",
            )

            if optimizer_fn:
                optimizer = optimizer_fn(self, observation_space, action_space,
                                         config)
            else:
                optimizer = tf.train.AdamOptimizer(config["lr"])

            if before_init:
                before_init(self, observation_space, action_space, config)

            model({
                SampleBatch.CUR_OBS: tf.convert_to_tensor([observation_space.sample()]),
                SampleBatch.PREV_ACTIONS: tf.convert_to_tensor([action_space.sample()]),
                SampleBatch.PREV_REWARDS: tf.convert_to_tensor([0.]),
            }, [tf.convert_to_tensor([s]) for s in model.get_initial_state()],
            tf.convert_to_tensor([1]))

            TFEagerPolicy.__init__(self, optimizer, model, observation_space,
                                   action_space)
            if before_loss_init:
                before_loss_init(self, observation_space, action_space, config)

        def postprocess_trajectory(self, samples, other_agent_batches=None,
                                   episode=None):
            assert tf.executing_eagerly()
            return postprocess_fn(self, samples)

        def compute_actions(self,
                            obs_batch,
                            state_batches,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            **kwargs):
            assert tf.executing_eagerly()
            seq_len = tf.ones(len(obs_batch))
            input_dict = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(
                    obs_batch, dtype=tf.float32),
                "is_training": tf.convert_to_tensor(True),
            }
            if obs_include_prev_action_reward:
                input_dict.update({
                    SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                        prev_action_batch, dtype=tf.int32),
                    SampleBatch.PREV_REWARDS: tf.convert_to_tensor(
                        prev_reward_batch, dtype=tf.float32),
                })
            self.state_in = state_batches
            self.model_out, self.state_out = self.model(
                input_dict, state_batches, seq_len)

            action_dist = self.dist_class(self.model_out)
            fetches = {}
            action = action_dist.sample().numpy()
            logp = action_dist.sampled_action_logp()
            if logp is not None:
                fetches.update({
                    ACTION_PROB: tf.exp(logp).numpy(),
                    ACTION_LOGP: logp.numpy(),
                })
            if extra_action_fetches_fn:
                fetches.update(extra_action_fetches_fn(self))
            return action, self.state_out, fetches

        def loss(self, outputs, samples):
            assert tf.executing_eagerly()
            return loss_fn(outputs, samples)

        def stats(self, outputs, samples):
            assert tf.executing_eagerly()
            if stats_fn is None:
                return {}
            return stats_fn(outputs, samples)

    policy_cls.__name__ = name
    return policy_cls
