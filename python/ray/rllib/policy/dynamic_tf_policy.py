from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
import logging
import numpy as np

from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.debug import log_once, summarize
from ray.rllib.utils.tracking_dict import UsageTrackingDict

tf = try_import_tf()

logger = logging.getLogger(__name__)


class DynamicTFPolicy(TFPolicy):
    """A TFPolicy that auto-defines placeholders dynamically at runtime.

    This class also supports eager execution if config["use_eager"] is True.
    Eager execution is implemented using a py_function op inside graph mode.

    Initialization of this class occurs in two phases.
      * Phase 1: the model is created and model variables are initialized.
      * Phase 2: a fake batch of data is created, sent to the trajectory
        postprocessor, and then used to create placeholders for the loss
        function. The loss and stats functions are initialized with these
        placeholders.

    Initialization defines the static graph. When using eager execution, a
    corresponding imperative py_function is also generated as an embedded op
    inside the static graph.
    """

    def __init__(self,
                 obs_space,
                 action_space,
                 config,
                 loss_fn,
                 stats_fn=None,
                 update_ops_fn=None,
                 grad_stats_fn=None,
                 before_loss_init=None,
                 make_model=None,
                 action_sampler_fn=None,
                 existing_inputs=None,
                 existing_model=None,
                 get_batch_divisibility_req=None,
                 obs_include_prev_action_reward=True):
        """Initialize a dynamic TF policy.

        Arguments:
            observation_space (gym.Space): Observation space of the policy.
            action_space (gym.Space): Action space of the policy.
            config (dict): Policy-specific configuration data.
            loss_fn (func): function that returns a loss tensor the policy
                graph, and dict of experience tensor placeholders
            stats_fn (func): optional function that returns a dict of
                TF fetches given the policy and batch input tensors
            grad_stats_fn (func): optional function that returns a dict of
                TF fetches given the policy and loss gradient tensors
            update_ops_fn (func): optional function that returns a list
                overriding the update ops to run when applying gradients
            before_loss_init (func): optional function to run prior to loss
                init that takes the same arguments as __init__
            make_model (func): optional function that returns a ModelV2 object
                given (policy, obs_space, action_space, config).
                All policy variables should be created in this function. If not
                specified, a default model will be created.
            action_sampler_fn (func): optional function that returns a
                tuple of action and action prob tensors given
                (policy, model, input_dict, obs_space, action_space, config).
                If not specified, a default action distribution will be used.
            existing_inputs (OrderedDict): when copying a policy, this
                specifies an existing dict of placeholders to use instead of
                defining new ones
            existing_model (ModelV2): when copying a policy, this specifies
                an existing model to clone and share weights with
            get_batch_divisibility_req (func): optional function that returns
                the divisibility requirement for sample batches
            obs_include_prev_action_reward (bool): whether to include the
                previous action and reward in the model input

        Attributes:
            config: config of the policy
            model: model instance, if any
            model_out: output tensors of the model
            action_dist: action distribution of the model, if any
            state_in: state input tensors, if any
            state_out: state output tensors, if any
            seq_lens: tensor of sequence lengths
        """
        self.config = config
        self._loss_fn = loss_fn
        self._stats_fn = stats_fn
        self._grad_stats_fn = grad_stats_fn
        self._update_ops_fn = update_ops_fn
        self._obs_include_prev_action_reward = obs_include_prev_action_reward

        # Setup standard placeholders
        prev_actions = None
        prev_rewards = None
        if existing_inputs is not None:
            obs = existing_inputs[SampleBatch.CUR_OBS]
            if self._obs_include_prev_action_reward:
                prev_actions = existing_inputs[SampleBatch.PREV_ACTIONS]
                prev_rewards = existing_inputs[SampleBatch.PREV_REWARDS]
        else:
            obs = tf.placeholder(
                tf.float32,
                shape=[None] + list(obs_space.shape),
                name="observation")
            if self._obs_include_prev_action_reward:
                prev_actions = ModelCatalog.get_action_placeholder(
                    action_space)
                prev_rewards = tf.placeholder(
                    tf.float32, [None], name="prev_reward")

        self.input_dict = {
            SampleBatch.CUR_OBS: obs,
            SampleBatch.PREV_ACTIONS: prev_actions,
            SampleBatch.PREV_REWARDS: prev_rewards,
            "is_training": self._get_is_training_placeholder(),
        }
        self.seq_lens = tf.placeholder(
            dtype=tf.int32, shape=[None], name="seq_lens")

        # Setup model
        self.dist_class, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        if existing_model:
            self.model = existing_model
        elif make_model:
            self.model = make_model(self, obs_space, action_space, config)
        else:
            self.model = ModelCatalog.get_model_v2(
                obs_space,
                action_space,
                logit_dim,
                self.config["model"],
                framework="tf")
        if existing_inputs:
            self.state_in = [
                v for k, v in existing_inputs.items()
                if k.startswith("state_in_")
            ]
            if self.state_in:
                self.seq_lens = existing_inputs["seq_lens"]
        else:
            self.state_in = [
                tf.placeholder(shape=(None, ) + s.shape, dtype=s.dtype)
                for s in self.model.get_initial_state()
            ]
        self.model_out, self.state_out = self.model(
            self.input_dict, self.state_in, self.seq_lens)

        # Setup action sampler
        if action_sampler_fn:
            self.action_dist = None
            self.dist_class = None
            action_sampler, action_prob = action_sampler_fn(
                self, self.model, self.input_dict, obs_space, action_space,
                config)
        else:
            self.action_dist = self.dist_class(self.model_out)
            action_sampler = self.action_dist.sample()
            action_prob = self.action_dist.sampled_action_prob()

        # Phase 1 init
        sess = tf.get_default_session() or tf.Session()
        if get_batch_divisibility_req:
            batch_divisibility_req = get_batch_divisibility_req(self)
        else:
            batch_divisibility_req = 1
        TFPolicy.__init__(
            self,
            obs_space,
            action_space,
            sess,
            obs_input=obs,
            action_sampler=action_sampler,
            action_prob=action_prob,
            loss=None,  # dynamically initialized on run
            loss_inputs=[],
            model=self.model,
            state_inputs=self.state_in,
            state_outputs=self.state_out,
            prev_action_input=prev_actions,
            prev_reward_input=prev_rewards,
            seq_lens=self.seq_lens,
            max_seq_len=config["model"]["max_seq_len"],
            batch_divisibility_req=batch_divisibility_req)

        # Phase 2 init
        self._needs_eager_conversion = set()
        self._eager_tensors = {}
        before_loss_init(self, obs_space, action_space, config)
        if not existing_inputs:
            self._initialize_loss()

    def get_obs_input_dict(self):
        """Returns the obs input dict used to build policy models.

        This dict includes the obs, prev actions, prev rewards, etc. tensors.
        """
        return self.input_dict

    def convert_to_eager(self, tensor):
        """Convert a graph tensor accessed in the loss to an eager tensor.

        Experimental.
        """
        if tf.executing_eagerly():
            return self._eager_tensors[tensor]
        else:
            self._needs_eager_conversion.add(tensor)
            return tensor

    @override(TFPolicy)
    def copy(self, existing_inputs):
        """Creates a copy of self using existing input placeholders."""

        # Note that there might be RNN state inputs at the end of the list
        if self._state_inputs:
            num_state_inputs = len(self._state_inputs) + 1
        else:
            num_state_inputs = 0
        if len(self._loss_inputs) + num_state_inputs != len(existing_inputs):
            raise ValueError("Tensor list mismatch", self._loss_inputs,
                             self._state_inputs, existing_inputs)
        for i, (k, v) in enumerate(self._loss_inputs):
            if v.shape.as_list() != existing_inputs[i].shape.as_list():
                raise ValueError("Tensor shape mismatch", i, k, v.shape,
                                 existing_inputs[i].shape)
        # By convention, the loss inputs are followed by state inputs and then
        # the seq len tensor
        rnn_inputs = []
        for i in range(len(self._state_inputs)):
            rnn_inputs.append(("state_in_{}".format(i),
                               existing_inputs[len(self._loss_inputs) + i]))
        if rnn_inputs:
            rnn_inputs.append(("seq_lens", existing_inputs[-1]))
        input_dict = OrderedDict(
            [(k, existing_inputs[i])
             for i, (k, _) in enumerate(self._loss_inputs)] + rnn_inputs)
        instance = self.__class__(
            self.observation_space,
            self.action_space,
            self.config,
            existing_inputs=input_dict,
            existing_model=self.model)

        loss = instance._do_loss_init(input_dict)
        loss_inputs = [(k, existing_inputs[i])
                       for i, (k, _) in enumerate(self._loss_inputs)]

        if self.config["use_eager"]:
            loss, new_stats = instance._gen_eager_loss_op(loss_inputs)
            instance._stats_fetches = new_stats

        TFPolicy._initialize_loss(instance, loss, loss_inputs)
        if instance._grad_stats_fn:
            instance._stats_fetches.update(
                instance._grad_stats_fn(instance, instance._grads))
        return instance

    @override(Policy)
    def get_initial_state(self):
        if self.model:
            return self.model.get_initial_state()
        else:
            return []

    def _initialize_loss(self):
        def fake_array(tensor):
            shape = tensor.shape.as_list()
            shape[0] = 1
            return np.zeros(shape, dtype=tensor.dtype.as_numpy_dtype)

        dummy_batch = {
            SampleBatch.CUR_OBS: fake_array(self._obs_input),
            SampleBatch.NEXT_OBS: fake_array(self._obs_input),
            SampleBatch.DONES: np.array([False], dtype=np.bool),
            SampleBatch.ACTIONS: fake_array(
                ModelCatalog.get_action_placeholder(self.action_space)),
            SampleBatch.REWARDS: np.array([0], dtype=np.float32),
        }
        if self._obs_include_prev_action_reward:
            dummy_batch.update({
                SampleBatch.PREV_ACTIONS: fake_array(self._prev_action_input),
                SampleBatch.PREV_REWARDS: fake_array(self._prev_reward_input),
            })
        state_init = self.get_initial_state()
        for i, h in enumerate(state_init):
            dummy_batch["state_in_{}".format(i)] = np.expand_dims(h, 0)
            dummy_batch["state_out_{}".format(i)] = np.expand_dims(h, 0)
        if state_init:
            dummy_batch["seq_lens"] = np.array([1], dtype=np.int32)
        for k, v in self.extra_compute_action_fetches().items():
            dummy_batch[k] = fake_array(v)

        # postprocessing might depend on variable init, so run it first here
        self._sess.run(tf.global_variables_initializer())
        postprocessed_batch = self.postprocess_trajectory(
            SampleBatch(dummy_batch))

        if self._obs_include_prev_action_reward:
            batch_tensors = UsageTrackingDict({
                SampleBatch.PREV_ACTIONS: self._prev_action_input,
                SampleBatch.PREV_REWARDS: self._prev_reward_input,
                SampleBatch.CUR_OBS: self._obs_input,
            })
            loss_inputs = [
                (SampleBatch.PREV_ACTIONS, self._prev_action_input),
                (SampleBatch.PREV_REWARDS, self._prev_reward_input),
                (SampleBatch.CUR_OBS, self._obs_input),
            ]
        else:
            batch_tensors = UsageTrackingDict({
                SampleBatch.CUR_OBS: self._obs_input,
            })
            loss_inputs = [
                (SampleBatch.CUR_OBS, self._obs_input),
            ]

        for k, v in postprocessed_batch.items():
            if k in batch_tensors:
                continue
            elif v.dtype == np.object:
                continue  # can't handle arbitrary objects in TF
            shape = (None, ) + v.shape[1:]
            dtype = np.float32 if v.dtype == np.float64 else v.dtype
            placeholder = tf.placeholder(dtype, shape=shape, name=k)
            batch_tensors[k] = placeholder

        if log_once("loss_init"):
            logger.info(
                "Initializing loss function with dummy input:\n\n{}\n".format(
                    summarize(batch_tensors)))

        loss = self._do_loss_init(batch_tensors)
        for k in sorted(batch_tensors.accessed_keys):
            loss_inputs.append((k, batch_tensors[k]))

        # XXX experimental support for automatically eagerifying the loss.
        # The main limitation right now is that TF doesn't support mixing eager
        # and non-eager tensors, so losses that read non-eager tensors through
        # `policy` need to use `policy.convert_to_eager(tensor)`.
        if self.config["use_eager"]:
            loss, new_stats = self._gen_eager_loss_op(loss_inputs)
            self._stats_fetches = new_stats

        TFPolicy._initialize_loss(self, loss, loss_inputs)
        if self._grad_stats_fn:
            self._stats_fetches.update(self._grad_stats_fn(self, self._grads))
        self._sess.run(tf.global_variables_initializer())

    def _do_loss_init(self, batch_tensors):
        loss = self._loss_fn(self, batch_tensors)
        if self._stats_fn:
            self._stats_fetches.update(self._stats_fn(self, batch_tensors))
        if self._update_ops_fn:
            self._update_ops = self._update_ops_fn(self)
        return loss

    def _gen_eager_loss_op(self, loss_inputs):
        graph_tensors = list(self._needs_eager_conversion)
        stat_items = list(self._stats_fetches.items())

        def gen_loss(model_outputs, *args):
            # fill in the batch tensor dict with eager ensors
            eager_inputs = dict(
                zip([k for (k, v) in loss_inputs], args[:len(loss_inputs)]))
            # fill in the eager versions of all accessed graph tensors
            self._eager_tensors = dict(
                zip(graph_tensors, args[len(loss_inputs):]))
            # patch the action dist to use eager mode tensors
            self.action_dist.inputs = model_outputs
            loss = self._loss_fn(self, eager_inputs)
            if self._stats_fn:
                stats = self._stats_fn(self, eager_inputs)
            return [loss] + [stats[k] for (k, v) in stat_items]

        eager_out = tf.py_function(
            gen_loss,
            # cast works around TypeError: Cannot convert provided value
            # to EagerTensor. Provided value: 0.0 Requested dtype: int64
            [self.model_out] + [
                tf.cast(v, tf.float32) for (k, v) in loss_inputs
            ] + [tf.cast(t, tf.float32) for t in graph_tensors],
            Tout=[tf.float32] + [v.dtype for (k, v) in stat_items])
        stats = {
            k: stat_tensor
            for (stat_tensor, (k, v)) in zip(eager_out[1:], stat_items)
        }

        return eager_out[0], stats
