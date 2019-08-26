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
from ray.rllib.utils.annotations import override
from ray.rllib.utils.debug import log_once
from ray.rllib.utils import try_import_tf

tf = try_import_tf()
logger = logging.getLogger(__name__)


def _disallow_var_creation(next_creator, **kw):
    v = next_creator(**kw)
    raise ValueError("Detected a variable being created during an eager "
                     "forward pass. Variables should only be created during "
                     "model initialization: {}".format(v.name))


def build_eager_tf_policy(name,
                          loss_fn,
                          get_default_config=None,
                          postprocess_fn=None,
                          stats_fn=None,
                          optimizer_fn=None,
                          gradients_fn=None,
                          apply_gradients_fn=None,
                          grad_stats_fn=None,
                          extra_learn_fetches_fn=None,
                          extra_action_fetches_fn=None,
                          before_init=None,
                          before_loss_init=None,
                          after_init=None,
                          make_model=None,
                          action_sampler_fn=None,
                          mixins=None,
                          obs_include_prev_action_reward=True,
                          get_batch_divisibility_req=None):
    """Build an eager TF policy.

    An eager policy runs all operations in eager mode, which makes debugging
    much simpler, but is lower performance.

    You shouldn't need to call this directly. Rather, prefer to build a TF
    graph policy and use set {"eager": true} in the trainer config to have
    it automatically be converted to an eager policy.

    This has the same signature as build_tf_policy()."""

    base = add_mixins(Policy, mixins)

    class eager_policy_cls(base):
        def __init__(self, observation_space, action_space, config):
            assert tf.executing_eagerly()
            Policy.__init__(self, observation_space, action_space, config)
            self._is_training = False
            self._loss_initialized = False
            self._sess = None

            if get_default_config:
                config = dict(get_default_config(), **config)

            if before_init:
                before_init(self, observation_space, action_space, config)

            self.config = config

            if action_sampler_fn:
                if not make_model:
                    raise ValueError(
                        "make_model is required if action_sampler_fn is given")
                self._dist_class = None
            else:
                self._dist_class, logit_dim = ModelCatalog.get_action_dist(
                    action_space, self.config["model"])

            if make_model:
                self.model = make_model(self, observation_space, action_space,
                                        config)
            else:
                self.model = ModelCatalog.get_model_v2(
                    observation_space,
                    action_space,
                    logit_dim,
                    config["model"],
                    framework="tf",
                )

            self.model({
                SampleBatch.CUR_OBS: tf.convert_to_tensor(
                    np.array([observation_space.sample()])),
                SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                    [_flatten_action(action_space.sample())]),
                SampleBatch.PREV_REWARDS: tf.convert_to_tensor([0.]),
            }, [
                tf.convert_to_tensor([s])
                for s in self.model.get_initial_state()
            ], tf.convert_to_tensor([1]))

            if before_loss_init:
                before_loss_init(self, observation_space, action_space, config)

            self._initialize_loss_with_dummy_batch()
            self._loss_initialized = True

            if optimizer_fn:
                self._optimizer = optimizer_fn(self, config)
            else:
                self._optimizer = tf.train.AdamOptimizer(config["lr"])

            if after_init:
                after_init(self, observation_space, action_space, config)

        @override(Policy)
        def postprocess_trajectory(self,
                                   samples,
                                   other_agent_batches=None,
                                   episode=None):
            assert tf.executing_eagerly()
            if postprocess_fn:
                return postprocess_fn(self, samples)
            else:
                return samples

        @override(Policy)
        def learn_on_batch(self, samples):
            with tf.variable_creator_scope(_disallow_var_creation):
                grads_and_vars, stats = self._compute_gradients(samples)
            self._apply_gradients(grads_and_vars)
            return stats

        @override(Policy)
        def compute_gradients(self, samples):
            with tf.variable_creator_scope(_disallow_var_creation):
                grads_and_vars, stats = self._compute_gradients(samples)
            grads = [g for g, v in grads_and_vars]
            grads = [(g.numpy() if g is not None else None) for g in grads]
            return grads, stats

        @override(Policy)
        def compute_actions(self,
                            obs_batch,
                            state_batches,
                            prev_action_batch=None,
                            prev_reward_batch=None,
                            info_batch=None,
                            episodes=None,
                            **kwargs):

            assert tf.executing_eagerly()
            self._is_training = False

            self._seq_lens = tf.ones(len(obs_batch))
            self._input_dict = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_batch),
                "is_training": tf.convert_to_tensor(False),
            }
            if obs_include_prev_action_reward:
                self._input_dict.update({
                    SampleBatch.PREV_ACTIONS: tf.convert_to_tensor(
                        prev_action_batch),
                    SampleBatch.PREV_REWARDS: tf.convert_to_tensor(
                        prev_reward_batch),
                })
            self._state_in = state_batches
            with tf.variable_creator_scope(_disallow_var_creation):
                model_out, state_out = self.model(
                    self._input_dict, state_batches, self._seq_lens)

            if self._dist_class:
                action_dist = self._dist_class(model_out, self.model)
                action = action_dist.sample().numpy()
                logp = action_dist.sampled_action_logp()
            else:
                action, logp = action_sampler_fn(
                    self, self.model, self._input_dict, self.observation_space,
                    self.action_space, self.config)
                action = action.numpy()

            fetches = {}
            if logp is not None:
                fetches.update({
                    ACTION_PROB: tf.exp(logp).numpy(),
                    ACTION_LOGP: logp.numpy(),
                })
            if extra_action_fetches_fn:
                fetches.update(extra_action_fetches_fn(self))
            return action, state_out, fetches

        @override(Policy)
        def apply_gradients(self, gradients):
            self._apply_gradients(
                zip([(tf.convert_to_tensor(g) if g is not None else None)
                     for g in gradients], self.model.trainable_variables()))

        @override(Policy)
        def get_weights(self):
            return tf.nest.map_structure(lambda var: var.numpy(),
                                         self.model.variables())

        @override(Policy)
        def set_weights(self, weights):
            tf.nest.map_structure(lambda var, value: var.assign(value),
                                  self.model.variables(), weights)

        def is_recurrent(self):
            return len(self._state_in) > 0

        def num_state_tensors(self):
            return len(self._state_in)

        def get_session(self):
            return None  # None implies eager

        def loss_initialized(self):
            return self._loss_initialized

        def _get_is_training_placeholder(self):
            return tf.convert_to_tensor(self._is_training)

        def _apply_gradients(self, grads_and_vars):
            if apply_gradients_fn:
                apply_gradients_fn(self, self._optimizer, grads_and_vars)
            else:
                self._optimizer.apply_gradients(grads_and_vars)

        def _compute_gradients(self, samples):
            """Computes and returns grads as eager tensors."""

            self._is_training = True

            samples = {
                k: tf.convert_to_tensor(v)
                for k, v in samples.items() if v.dtype != np.object
            }

            with tf.GradientTape(persistent=gradients_fn is not None) as tape:
                # TODO: set seq len and state in properly
                self._seq_lens = tf.ones(len(samples[SampleBatch.CUR_OBS]))
                self._state_in = []
                model_out, _ = self.model(samples, self._state_in,
                                          self._seq_lens)
                loss = loss_fn(self, self.model, self._dist_class, samples)

            variables = self.model.trainable_variables()

            if gradients_fn:

                class OptimizerWrapper(object):
                    def __init__(self, tape):
                        self.tape = tape

                    def compute_gradients(self, loss, var_list):
                        return list(
                            zip(self.tape.gradient(loss, var_list), var_list))

                grads_and_vars = gradients_fn(self, OptimizerWrapper(tape),
                                              loss)
            else:
                grads_and_vars = list(
                    zip(tape.gradient(loss, variables), variables))

            if log_once("grad_vars"):
                for _, v in grads_and_vars:
                    logger.info("Optimizing variable {}".format(v.name))

            grads = [g for g, v in grads_and_vars]
            stats = self._stats(self, samples, grads)
            return grads_and_vars, stats

        def _stats(self, outputs, samples, grads):
            assert tf.executing_eagerly()
            fetches = {}
            if stats_fn:
                fetches[LEARNER_STATS_KEY] = {
                    k: v.numpy()
                    for k, v in stats_fn(outputs, samples).items()
                }
            else:
                fetches[LEARNER_STATS_KEY] = {}
            if extra_learn_fetches_fn:
                fetches.update({
                    k: v.numpy()
                    for k, v in extra_learn_fetches_fn(self).items()
                })
            if grad_stats_fn:
                fetches.update({
                    k: v.numpy()
                    for k, v in grad_stats_fn(self, samples, grads).items()
                })
            return fetches

        def _initialize_loss_with_dummy_batch(self):
            # Dummy forward pass to initialize any policy attributes, etc.
            action_dtype, action_shape = ModelCatalog.get_action_shape(
                self.action_space)
            dummy_batch = {
                SampleBatch.CUR_OBS: tf.convert_to_tensor(
                    np.array([self.observation_space.sample()])),
                SampleBatch.NEXT_OBS: tf.convert_to_tensor(
                    np.array([self.observation_space.sample()])),
                SampleBatch.DONES: tf.convert_to_tensor(
                    np.array([False], dtype=np.bool)),
                SampleBatch.ACTIONS: tf.convert_to_tensor(
                    np.zeros(
                        (1, ) + action_shape[1:],
                        dtype=action_dtype.as_numpy_dtype())),
                SampleBatch.REWARDS: tf.convert_to_tensor(
                    np.array([0], dtype=np.float32)),
            }
            if obs_include_prev_action_reward:
                dummy_batch.update({
                    SampleBatch.PREV_ACTIONS: dummy_batch[SampleBatch.ACTIONS],
                    SampleBatch.PREV_REWARDS: dummy_batch[SampleBatch.REWARDS],
                })
            state_init = self.get_initial_state()
            state_batches = []
            for i, h in enumerate(state_init):
                dummy_batch["state_in_{}".format(i)] = tf.convert_to_tensor(
                    np.expand_dims(h, 0))
                dummy_batch["state_out_{}".format(i)] = tf.convert_to_tensor(
                    np.expand_dims(h, 0))
                state_batches.append(
                    tf.convert_to_tensor(np.expand_dims(h, 0)))
            if state_init:
                dummy_batch["seq_lens"] = tf.convert_to_tensor(
                    np.array([1], dtype=np.int32))

            # for IMPALA which expects a certain sample batch size
            def tile_to(tensor, n):
                return tf.tile(tensor,
                               [n] + [1 for _ in tensor.shape.as_list()[1:]])

            if get_batch_divisibility_req:
                dummy_batch = {
                    k: tile_to(v, get_batch_divisibility_req(self))
                    for k, v in dummy_batch.items()
                }

            # Execute a forward pass to get self.action_dist etc initialized,
            # and also obtain the extra action fetches
            _, _, fetches = self.compute_actions(
                dummy_batch[SampleBatch.CUR_OBS], state_batches,
                dummy_batch.get(SampleBatch.PREV_ACTIONS),
                dummy_batch.get(SampleBatch.PREV_REWARDS))
            dummy_batch.update(fetches)

            postprocessed_batch = self.postprocess_trajectory(
                SampleBatch(dummy_batch))

            # model forward pass for the loss (needed after postprocess to
            # overwrite any tensor state from that call)
            self.model.from_batch(dummy_batch)

            postprocessed_batch = {
                k: tf.convert_to_tensor(v)
                for k, v in postprocessed_batch.items()
            }

            loss_fn(self, self.model, self._dist_class, postprocessed_batch)
            if stats_fn:
                stats_fn(self, postprocessed_batch)

    eager_policy_cls.__name__ = name + "_eager"
    eager_policy_cls.__qualname__ = name + "_eager"
    return eager_policy_cls
