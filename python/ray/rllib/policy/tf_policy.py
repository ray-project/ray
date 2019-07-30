from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import errno
import logging
import os

import numpy as np
import ray
import ray.experimental.tf_utils
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.debug import log_once, summarize
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule
from ray.rllib.utils.tf_run_builder import TFRunBuilder
from ray.rllib.utils import try_import_tf

tf = try_import_tf()
logger = logging.getLogger(__name__)


@DeveloperAPI
class TFPolicy(Policy):
    """An agent policy and loss implemented in TensorFlow.

    Extending this class enables RLlib to perform TensorFlow specific
    optimizations on the policy, e.g., parallelization across gpus or
    fusing multiple graphs together in the multi-agent setting.

    Input tensors are typically shaped like [BATCH_SIZE, ...].

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        model (rllib.models.Model): RLlib model used for the policy.

    Examples:
        >>> policy = TFPolicySubclass(
            sess, obs_input, action_sampler, loss, loss_inputs)

        >>> print(policy.compute_actions([1, 0, 2]))
        (array([0, 1, 1]), [], {})

        >>> print(policy.postprocess_trajectory(SampleBatch({...})))
        SampleBatch({"action": ..., "advantages": ..., ...})
    """

    @DeveloperAPI
    def __init__(self,
                 observation_space,
                 action_space,
                 sess,
                 obs_input,
                 action_sampler,
                 loss,
                 loss_inputs,
                 model=None,
                 action_prob=None,
                 state_inputs=None,
                 state_outputs=None,
                 prev_action_input=None,
                 prev_reward_input=None,
                 seq_lens=None,
                 max_seq_len=20,
                 batch_divisibility_req=1,
                 update_ops=None):
        """Initialize the policy.

        Arguments:
            observation_space (gym.Space): Observation space of the env.
            action_space (gym.Space): Action space of the env.
            sess (Session): TensorFlow session to use.
            obs_input (Tensor): input placeholder for observations, of shape
                [BATCH_SIZE, obs...].
            action_sampler (Tensor): Tensor for sampling an action, of shape
                [BATCH_SIZE, action...]
            loss (Tensor): scalar policy loss output tensor.
            loss_inputs (list): a (name, placeholder) tuple for each loss
                input argument. Each placeholder name must correspond to a
                SampleBatch column key returned by postprocess_trajectory(),
                and has shape [BATCH_SIZE, data...]. These keys will be read
                from postprocessed sample batches and fed into the specified
                placeholders during loss computation.
            model (rllib.models.Model): used to integrate custom losses and
                stats from user-defined RLlib models.
            action_prob (Tensor): probability of the sampled action.
            state_inputs (list): list of RNN state input Tensors.
            state_outputs (list): list of RNN state output Tensors.
            prev_action_input (Tensor): placeholder for previous actions
            prev_reward_input (Tensor): placeholder for previous rewards
            seq_lens (Tensor): placeholder for RNN sequence lengths, of shape
                [NUM_SEQUENCES]. Note that NUM_SEQUENCES << BATCH_SIZE. See
                policy/rnn_sequencing.py for more information.
            max_seq_len (int): max sequence length for LSTM training.
            batch_divisibility_req (int): pad all agent experiences batches to
                multiples of this value. This only has an effect if not using
                a LSTM model.
            update_ops (list): override the batchnorm update ops to run when
                applying gradients. Otherwise we run all update ops found in
                the current variable scope.
        """

        self.observation_space = observation_space
        self.action_space = action_space
        self.model = model
        self._sess = sess
        self._obs_input = obs_input
        self._prev_action_input = prev_action_input
        self._prev_reward_input = prev_reward_input
        self._sampler = action_sampler
        self._is_training = self._get_is_training_placeholder()
        self._action_prob = action_prob
        self._state_inputs = state_inputs or []
        self._state_outputs = state_outputs or []
        self._seq_lens = seq_lens
        self._max_seq_len = max_seq_len
        self._batch_divisibility_req = batch_divisibility_req
        self._update_ops = update_ops
        self._stats_fetches = {}

        if loss is not None:
            self._initialize_loss(loss, loss_inputs)
        else:
            self._loss = None

        if len(self._state_inputs) != len(self._state_outputs):
            raise ValueError(
                "Number of state input and output tensors must match, got: "
                "{} vs {}".format(self._state_inputs, self._state_outputs))
        if len(self.get_initial_state()) != len(self._state_inputs):
            raise ValueError(
                "Length of initial state must match number of state inputs, "
                "got: {} vs {}".format(self.get_initial_state(),
                                       self._state_inputs))
        if self._state_inputs and self._seq_lens is None:
            raise ValueError(
                "seq_lens tensor must be given if state inputs are defined")

    def get_placeholder(self, name):
        """Returns the given action or loss input placeholder by name.

        If the loss has not been initialized and a loss input placeholder is
        requested, an error is raised.
        """

        obs_inputs = {
            SampleBatch.CUR_OBS: self._obs_input,
            SampleBatch.PREV_ACTIONS: self._prev_action_input,
            SampleBatch.PREV_REWARDS: self._prev_reward_input,
        }
        if name in obs_inputs:
            return obs_inputs[name]

        if not self.loss_initialized():
            raise RuntimeError(
                "You cannot call policy.get_placeholder() for non-obs inputs "
                "before the loss has been initialized. To avoid this, use "
                "policy.loss_initialized() to check whether this is the "
                "case, or move the call to later (e.g., from stats_fn to "
                "grad_stats_fn).")

        return self._loss_input_dict[name]

    def get_session(self):
        """Returns a reference to the TF session for this policy."""
        return self._sess

    def loss_initialized(self):
        """Returns whether the loss function has been initialized."""
        return self._loss is not None

    def _initialize_loss(self, loss, loss_inputs):
        self._loss_inputs = loss_inputs
        self._loss_input_dict = dict(self._loss_inputs)
        for i, ph in enumerate(self._state_inputs):
            self._loss_input_dict["state_in_{}".format(i)] = ph

        if self.model:
            self._loss = self.model.custom_loss(loss, self._loss_input_dict)
            self._stats_fetches.update({
                "model": self.model.metrics() if isinstance(
                    self.model, ModelV2) else self.model.custom_stats()
            })
        else:
            self._loss = loss

        self._optimizer = self.optimizer()
        self._grads_and_vars = [
            (g, v) for (g, v) in self.gradients(self._optimizer, self._loss)
            if g is not None
        ]
        self._grads = [g for (g, v) in self._grads_and_vars]
        self._variables = ray.experimental.tf_utils.TensorFlowVariables(
            self._loss, self._sess)

        # gather update ops for any batch norm layers
        if not self._update_ops:
            self._update_ops = tf.get_collection(
                tf.GraphKeys.UPDATE_OPS, scope=tf.get_variable_scope().name)
        if self._update_ops:
            logger.info("Update ops to run on apply gradient: {}".format(
                self._update_ops))
        with tf.control_dependencies(self._update_ops):
            self._apply_op = self.build_apply_op(self._optimizer,
                                                 self._grads_and_vars)

        if log_once("loss_used"):
            logger.debug(
                "These tensors were used in the loss_fn:\n\n{}\n".format(
                    summarize(self._loss_input_dict)))

        self._sess.run(tf.global_variables_initializer())

    @override(Policy)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        builder = TFRunBuilder(self._sess, "compute_actions")
        fetches = self._build_compute_actions(builder, obs_batch,
                                              state_batches, prev_action_batch,
                                              prev_reward_batch)
        return builder.get(fetches)

    @override(Policy)
    def compute_gradients(self, postprocessed_batch):
        assert self.loss_initialized()
        builder = TFRunBuilder(self._sess, "compute_gradients")
        fetches = self._build_compute_gradients(builder, postprocessed_batch)
        return builder.get(fetches)

    @override(Policy)
    def apply_gradients(self, gradients):
        assert self.loss_initialized()
        builder = TFRunBuilder(self._sess, "apply_gradients")
        fetches = self._build_apply_gradients(builder, gradients)
        builder.get(fetches)

    @override(Policy)
    def learn_on_batch(self, postprocessed_batch):
        assert self.loss_initialized()
        builder = TFRunBuilder(self._sess, "learn_on_batch")
        fetches = self._build_learn_on_batch(builder, postprocessed_batch)
        return builder.get(fetches)

    @override(Policy)
    def get_weights(self):
        return self._variables.get_flat()

    @override(Policy)
    def set_weights(self, weights):
        return self._variables.set_flat(weights)

    @override(Policy)
    def export_model(self, export_dir):
        """Export tensorflow graph to export_dir for serving."""
        with self._sess.graph.as_default():
            builder = tf.saved_model.builder.SavedModelBuilder(export_dir)
            signature_def_map = self._build_signature_def()
            builder.add_meta_graph_and_variables(
                self._sess, [tf.saved_model.tag_constants.SERVING],
                signature_def_map=signature_def_map)
            builder.save()

    @override(Policy)
    def export_checkpoint(self, export_dir, filename_prefix="model"):
        """Export tensorflow checkpoint to export_dir."""
        try:
            os.makedirs(export_dir)
        except OSError as e:
            # ignore error if export dir already exists
            if e.errno != errno.EEXIST:
                raise
        save_path = os.path.join(export_dir, filename_prefix)
        with self._sess.graph.as_default():
            saver = tf.train.Saver()
            saver.save(self._sess, save_path)

    @DeveloperAPI
    def copy(self, existing_inputs):
        """Creates a copy of self using existing input placeholders.

        Optional, only required to work with the multi-GPU optimizer."""
        raise NotImplementedError

    @DeveloperAPI
    def extra_compute_action_feed_dict(self):
        """Extra dict to pass to the compute actions session run."""
        return {}

    @DeveloperAPI
    def extra_compute_action_fetches(self):
        """Extra values to fetch and return from compute_actions().

        By default we only return action probability info (if present).
        """
        if self._action_prob is not None:
            return {"action_prob": self._action_prob}
        else:
            return {}

    @DeveloperAPI
    def extra_compute_grad_feed_dict(self):
        """Extra dict to pass to the compute gradients session run."""
        return {}  # e.g, kl_coeff

    @DeveloperAPI
    def extra_compute_grad_fetches(self):
        """Extra values to fetch and return from compute_gradients()."""
        return {LEARNER_STATS_KEY: {}}  # e.g, stats, td error, etc.

    @DeveloperAPI
    def optimizer(self):
        """TF optimizer to use for policy optimization."""
        if hasattr(self, "config"):
            return tf.train.AdamOptimizer(self.config["lr"])
        else:
            return tf.train.AdamOptimizer()

    @DeveloperAPI
    def gradients(self, optimizer, loss):
        """Override for custom gradient computation."""
        return optimizer.compute_gradients(loss)

    @DeveloperAPI
    def build_apply_op(self, optimizer, grads_and_vars):
        """Override for custom gradient apply computation."""

        # specify global_step for TD3 which needs to count the num updates
        return optimizer.apply_gradients(
            self._grads_and_vars,
            global_step=tf.train.get_or_create_global_step())

    @DeveloperAPI
    def _get_is_training_placeholder(self):
        """Get the placeholder for _is_training, i.e., for batch norm layers.

        This can be called safely before __init__ has run.
        """
        if not hasattr(self, "_is_training"):
            self._is_training = tf.placeholder_with_default(False, ())
        return self._is_training

    def _debug_vars(self):
        if log_once("grad_vars"):
            for _, v in self._grads_and_vars:
                logger.info("Optimizing variable {}".format(v))

    def _extra_input_signature_def(self):
        """Extra input signatures to add when exporting tf model.
        Inferred from extra_compute_action_feed_dict()
        """
        feed_dict = self.extra_compute_action_feed_dict()
        return {
            k.name: tf.saved_model.utils.build_tensor_info(k)
            for k in feed_dict.keys()
        }

    def _extra_output_signature_def(self):
        """Extra output signatures to add when exporting tf model.
        Inferred from extra_compute_action_fetches()
        """
        fetches = self.extra_compute_action_fetches()
        return {
            k: tf.saved_model.utils.build_tensor_info(fetches[k])
            for k in fetches.keys()
        }

    def _build_signature_def(self):
        """Build signature def map for tensorflow SavedModelBuilder.
        """
        # build input signatures
        input_signature = self._extra_input_signature_def()
        input_signature["observations"] = \
            tf.saved_model.utils.build_tensor_info(self._obs_input)

        if self._seq_lens is not None:
            input_signature["seq_lens"] = \
                tf.saved_model.utils.build_tensor_info(self._seq_lens)
        if self._prev_action_input is not None:
            input_signature["prev_action"] = \
                tf.saved_model.utils.build_tensor_info(self._prev_action_input)
        if self._prev_reward_input is not None:
            input_signature["prev_reward"] = \
                tf.saved_model.utils.build_tensor_info(self._prev_reward_input)
        input_signature["is_training"] = \
            tf.saved_model.utils.build_tensor_info(self._is_training)

        for state_input in self._state_inputs:
            input_signature[state_input.name] = \
                tf.saved_model.utils.build_tensor_info(state_input)

        # build output signatures
        output_signature = self._extra_output_signature_def()
        output_signature["actions"] = \
            tf.saved_model.utils.build_tensor_info(self._sampler)
        for state_output in self._state_outputs:
            output_signature[state_output.name] = \
                tf.saved_model.utils.build_tensor_info(state_output)
        signature_def = (
            tf.saved_model.signature_def_utils.build_signature_def(
                input_signature, output_signature,
                tf.saved_model.signature_constants.PREDICT_METHOD_NAME))
        signature_def_key = (tf.saved_model.signature_constants.
                             DEFAULT_SERVING_SIGNATURE_DEF_KEY)
        signature_def_map = {signature_def_key: signature_def}
        return signature_def_map

    def _build_compute_actions(self,
                               builder,
                               obs_batch,
                               state_batches=None,
                               prev_action_batch=None,
                               prev_reward_batch=None,
                               episodes=None):
        state_batches = state_batches or []
        if len(self._state_inputs) != len(state_batches):
            raise ValueError(
                "Must pass in RNN state batches for placeholders {}, got {}".
                format(self._state_inputs, state_batches))
        builder.add_feed_dict(self.extra_compute_action_feed_dict())
        builder.add_feed_dict({self._obs_input: obs_batch})
        if state_batches:
            builder.add_feed_dict({self._seq_lens: np.ones(len(obs_batch))})
        if self._prev_action_input is not None and prev_action_batch:
            builder.add_feed_dict({self._prev_action_input: prev_action_batch})
        if self._prev_reward_input is not None and prev_reward_batch:
            builder.add_feed_dict({self._prev_reward_input: prev_reward_batch})
        builder.add_feed_dict({self._is_training: False})
        builder.add_feed_dict(dict(zip(self._state_inputs, state_batches)))
        fetches = builder.add_fetches([self._sampler] + self._state_outputs +
                                      [self.extra_compute_action_fetches()])
        return fetches[0], fetches[1:-1], fetches[-1]

    def _build_compute_gradients(self, builder, postprocessed_batch):
        self._debug_vars()
        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict({self._is_training: True})
        builder.add_feed_dict(
            self._get_loss_inputs_dict(postprocessed_batch, shuffle=False))
        fetches = builder.add_fetches(
            [self._grads, self._get_grad_and_stats_fetches()])
        return fetches[0], fetches[1]

    def _build_apply_gradients(self, builder, gradients):
        if len(gradients) != len(self._grads):
            raise ValueError(
                "Unexpected number of gradients to apply, got {} for {}".
                format(gradients, self._grads))
        builder.add_feed_dict({self._is_training: True})
        builder.add_feed_dict(dict(zip(self._grads, gradients)))
        fetches = builder.add_fetches([self._apply_op])
        return fetches[0]

    def _build_learn_on_batch(self, builder, postprocessed_batch):
        self._debug_vars()
        builder.add_feed_dict(self.extra_compute_grad_feed_dict())
        builder.add_feed_dict(
            self._get_loss_inputs_dict(postprocessed_batch, shuffle=False))
        builder.add_feed_dict({self._is_training: True})
        fetches = builder.add_fetches([
            self._apply_op,
            self._get_grad_and_stats_fetches(),
        ])
        return fetches[1]

    def _get_grad_and_stats_fetches(self):
        fetches = self.extra_compute_grad_fetches()
        if LEARNER_STATS_KEY not in fetches:
            raise ValueError(
                "Grad fetches should contain 'stats': {...} entry")
        if self._stats_fetches:
            fetches[LEARNER_STATS_KEY] = dict(self._stats_fetches,
                                              **fetches[LEARNER_STATS_KEY])
        return fetches

    def _get_loss_inputs_dict(self, batch, shuffle):
        """Return a feed dict from a batch.

        Arguments:
            batch (SampleBatch): batch of data to derive inputs from
            shuffle (bool): whether to shuffle batch sequences. Shuffle may
                be done in-place. This only makes sense if you're further
                applying minibatch SGD after getting the outputs.

        Returns:
            feed dict of data
        """

        feed_dict = {}
        if self._batch_divisibility_req > 1:
            meets_divisibility_reqs = (
                len(batch[SampleBatch.CUR_OBS]) %
                self._batch_divisibility_req == 0
                and max(batch[SampleBatch.AGENT_INDEX]) == 0)  # not multiagent
        else:
            meets_divisibility_reqs = True

        # Simple case: not RNN nor do we need to pad
        if not self._state_inputs and meets_divisibility_reqs:
            if shuffle:
                batch.shuffle()
            for k, ph in self._loss_inputs:
                feed_dict[ph] = batch[k]
            return feed_dict

        if self._state_inputs:
            max_seq_len = self._max_seq_len
            dynamic_max = True
        else:
            max_seq_len = self._batch_divisibility_req
            dynamic_max = False

        # RNN or multi-agent case
        feature_keys = [k for k, v in self._loss_inputs]
        state_keys = [
            "state_in_{}".format(i) for i in range(len(self._state_inputs))
        ]
        feature_sequences, initial_states, seq_lens = chop_into_sequences(
            batch[SampleBatch.EPS_ID],
            batch[SampleBatch.UNROLL_ID],
            batch[SampleBatch.AGENT_INDEX], [batch[k] for k in feature_keys],
            [batch[k] for k in state_keys],
            max_seq_len,
            dynamic_max=dynamic_max,
            shuffle=shuffle)
        for k, v in zip(feature_keys, feature_sequences):
            feed_dict[self._loss_input_dict[k]] = v
        for k, v in zip(state_keys, initial_states):
            feed_dict[self._loss_input_dict[k]] = v
        feed_dict[self._seq_lens] = seq_lens

        if log_once("rnn_feed_dict"):
            logger.info("Padded input for RNN:\n\n{}\n".format(
                summarize({
                    "features": feature_sequences,
                    "initial_states": initial_states,
                    "seq_lens": seq_lens,
                    "max_seq_len": max_seq_len,
                })))
        return feed_dict


@DeveloperAPI
class LearningRateSchedule(object):
    """Mixin for TFPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self.cur_lr = tf.get_variable("lr", initializer=lr, trainable=False)
        if lr_schedule is None:
            self.lr_schedule = ConstantSchedule(lr)
        else:
            self.lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1])

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(LearningRateSchedule, self).on_global_var_update(global_vars)
        self.cur_lr.load(
            self.lr_schedule.value(global_vars["timestep"]),
            session=self._sess)

    @override(TFPolicy)
    def optimizer(self):
        return tf.train.AdamOptimizer(self.cur_lr)


@DeveloperAPI
class EntropyCoeffSchedule(object):
    """Mixin for TFPolicy that adds entropy coeff decay."""

    @DeveloperAPI
    def __init__(self, entropy_coeff, entropy_coeff_schedule):
        self.entropy_coeff = tf.get_variable(
            "entropy_coeff", initializer=entropy_coeff, trainable=False)
        self._entropy_schedule = entropy_coeff_schedule

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(EntropyCoeffSchedule, self).on_global_var_update(global_vars)
        if self._entropy_schedule is not None:
            self.entropy_coeff.load(
                self.entropy_coeff.eval(session=self._sess) *
                (1 - global_vars["timestep"] /
                 self.config["entropy_coeff_schedule"]),
                session=self._sess)
