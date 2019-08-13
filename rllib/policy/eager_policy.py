from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np

from ray.rllib.evaluation.episode import _flatten_action
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import ACTION_PROB, ACTION_LOGP
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.debug import log_once
from ray.rllib.utils import try_import_tf

tf = try_import_tf()
logger = logging.getLogger(__name__)


@DeveloperAPI
class TFEagerPolicy(Policy):
    @DeveloperAPI
    def __init__(self, optimizer, model, observation_space, action_space,
                 gradients_fn):
        self.optimizer = optimizer
        self.model = model
        self.observation_space = observation_space
        self.action_space = action_space
        self._gradients_fn = gradients_fn
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
            k: tf.convert_to_tensor(v)
            for k, v in samples.items() if v.dtype != np.object
        }

        with tf.GradientTape() as tape:
            # TODO: set seq len and state in properly
            self.seq_lens = tf.ones(len(samples[SampleBatch.CUR_OBS]))
            self.state_in = []
            self.model_out, self.state_out = self.model(
                samples, self.state_in, self.seq_lens)
            self.action_dist = self.dist_class(self.model_out)
            loss = self.loss(self, samples)
            stats = self.stats(self, samples)

        variables = self.model.trainable_variables()

        if self._gradients_fn:

            class OptimizerWrapper(object):
                def __init__(self, tape):
                    self.tape = tape

                def compute_gradients(self, loss, var_list):
                    return zip(self.tape.gradient(loss, var_list), var_list)

            grads_and_vars = self._gradients_fn(self, OptimizerWrapper(tape),
                                                loss)
        else:
            grads_and_vars = zip(tape.gradient(loss, variables), variables)

        if log_once("grad_vars"):
            grads_and_vars = list(grads_and_vars)
            for _, v in grads_and_vars:
                logger.info("Optimizing variable {}".format(v.name))

        self.optimizer.apply_gradients(grads_and_vars)
        return {LEARNER_STATS_KEY: stats}

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
            loss=lambda: self.loss(postprocessed_batch), ))
        grads = tf.nest.map_structure(lambda grad: grad.numpy(), grads)

        return grads, {
            LEARNER_STATS_KEY: self.stats(self, postprocessed_batch)
        }

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

    def get_session(self):
        return None


def build_tf_policy(name,
                    loss_fn,
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

    class policy_cls(base):
        def __init__(self, observation_space, action_space, config):
            assert tf.executing_eagerly()

            if get_default_config:
                config = dict(get_default_config(), **config)

            if before_init:
                before_init(self, observation_space, action_space, config)

            self.config = config

            self.dist_class, logit_dim = ModelCatalog.get_action_dist(
                action_space, config["model"])

            model = ModelCatalog.get_model_v2(
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

            model({
                SampleBatch.CUR_OBS: tf.convert_to_tensor(
                    np.array([observation_space.sample()])),
                SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                    [_flatten_action(action_space.sample())]),
                SampleBatch.PREV_REWARDS: tf.convert_to_tensor([0.]),
            }, [tf.convert_to_tensor([s]) for s in model.get_initial_state()],
                  tf.convert_to_tensor([1]))

            TFEagerPolicy.__init__(self, optimizer, model, observation_space,
                                   action_space, gradients_fn)
            if before_loss_init:
                before_loss_init(self, observation_space, action_space, config)

        def postprocess_trajectory(self,
                                   samples,
                                   other_agent_batches=None,
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
            return {
                k: v.numpy()
                for k, v in stats_fn(outputs, samples).items()
            }

    policy_cls.__name__ = name
    return policy_cls
