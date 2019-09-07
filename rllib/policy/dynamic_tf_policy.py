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

    Initialization of this class occurs in two phases.
      * Phase 1: the model is created and model variables are initialized.
      * Phase 2: a fake batch of data is created, sent to the trajectory
        postprocessor, and then used to create placeholders for the loss
        function. The loss and stats functions are initialized with these
        placeholders.

    Initialization defines the static graph.
    """

    def __init__(self,
                 obs_space,
                 action_space,
                 config,
                 loss_fn,
                 stats_fn=None,
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
            before_loss_init (func): optional function to run prior to loss
                init that takes the same arguments as __init__
            make_model (func): optional function that returns a ModelV2 object
                given (policy, obs_space, action_space, config).
                All policy variables should be created in this function. If not
                specified, a default model will be created.
            action_sampler_fn (func): optional function that returns a
                tuple of action and action logp tensors given
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
        """
        self.config = config
        self._loss_fn = loss_fn
        self._stats_fn = stats_fn
        self._grad_stats_fn = grad_stats_fn
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

        self._input_dict = {
            SampleBatch.CUR_OBS: obs,
            SampleBatch.PREV_ACTIONS: prev_actions,
            SampleBatch.PREV_REWARDS: prev_rewards,
            "is_training": self._get_is_training_placeholder(),
        }
        self._seq_lens = tf.placeholder(
            dtype=tf.int32, shape=[None], name="seq_lens")

        # Setup model
        if action_sampler_fn:
            if not make_model:
                raise ValueError(
                    "make_model is required if action_sampler_fn is given")
            self._dist_class = None
        else:
            self._dist_class, logit_dim = ModelCatalog.get_action_dist(
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
            self._state_in = [
                v for k, v in existing_inputs.items()
                if k.startswith("state_in_")
            ]
            if self._state_in:
                self._seq_lens = existing_inputs["seq_lens"]
        else:
            self._state_in = [
                tf.placeholder(shape=(None, ) + s.shape, dtype=s.dtype)
                for s in self.model.get_initial_state()
            ]

        model_out, self._state_out = self.model(self._input_dict,
                                                self._state_in, self._seq_lens)

        # Setup action sampler
        if action_sampler_fn:
            action_sampler, action_logp = action_sampler_fn(
                self, self.model, self._input_dict, obs_space, action_space,
                config)
        else:
            action_dist = self._dist_class(model_out, self.model)
            action_sampler = action_dist.sample()
            action_logp = action_dist.sampled_action_logp()

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
            action_logp=action_logp,
            loss=None,  # dynamically initialized on run
            loss_inputs=[],
            model=self.model,
            state_inputs=self._state_in,
            state_outputs=self._state_out,
            prev_action_input=prev_actions,
            prev_reward_input=prev_rewards,
            seq_lens=self._seq_lens,
            max_seq_len=config["model"]["max_seq_len"],
            batch_divisibility_req=batch_divisibility_req)

        # Phase 2 init
        before_loss_init(self, obs_space, action_space, config)
        if not existing_inputs:
            self._initialize_loss()

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

        instance._loss_input_dict = input_dict
        loss = instance._do_loss_init(input_dict)
        loss_inputs = [(k, existing_inputs[i])
                       for i, (k, _) in enumerate(self._loss_inputs)]

        TFPolicy._initialize_loss(instance, loss, loss_inputs)
        if instance._grad_stats_fn:
            instance._stats_fetches.update(
                instance._grad_stats_fn(instance, input_dict, instance._grads))
        return instance

    @override(Policy)
    def get_initial_state(self):
        if self.model:
            return self.model.get_initial_state()
        else:
            return []

    def is_recurrent(self):
        return len(self._state_in) > 0

    def num_state_tensors(self):
        return len(self._state_in)

    def _initialize_loss(self):
        def fake_array(tensor):
            shape = tensor.shape.as_list()
            shape = [s if s is not None else 1 for s in shape]
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
        state_batches = []
        for i, h in enumerate(state_init):
            dummy_batch["state_in_{}".format(i)] = np.expand_dims(h, 0)
            dummy_batch["state_out_{}".format(i)] = np.expand_dims(h, 0)
            state_batches.append(np.expand_dims(h, 0))
        if state_init:
            dummy_batch["seq_lens"] = np.array([1], dtype=np.int32)
        for k, v in self.extra_compute_action_fetches().items():
            dummy_batch[k] = fake_array(v)

        # postprocessing might depend on variable init, so run it first here
        self._sess.run(tf.global_variables_initializer())

        postprocessed_batch = self.postprocess_trajectory(
            SampleBatch(dummy_batch))

        # model forward pass for the loss (needed after postprocess to
        # overwrite any tensor state from that call)
        self.model(self._input_dict, self._state_in, self._seq_lens)

        if self._obs_include_prev_action_reward:
            train_batch = UsageTrackingDict({
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
            train_batch = UsageTrackingDict({
                SampleBatch.CUR_OBS: self._obs_input,
            })
            loss_inputs = [
                (SampleBatch.CUR_OBS, self._obs_input),
            ]

        for k, v in postprocessed_batch.items():
            if k in train_batch:
                continue
            elif v.dtype == np.object:
                continue  # can't handle arbitrary objects in TF
            elif k == "seq_lens" or k.startswith("state_in_"):
                continue
            shape = (None, ) + v.shape[1:]
            dtype = np.float32 if v.dtype == np.float64 else v.dtype
            placeholder = tf.placeholder(dtype, shape=shape, name=k)
            train_batch[k] = placeholder

        for i, si in enumerate(self._state_in):
            train_batch["state_in_{}".format(i)] = si
        train_batch["seq_lens"] = self._seq_lens

        if log_once("loss_init"):
            logger.debug(
                "Initializing loss function with dummy input:\n\n{}\n".format(
                    summarize(train_batch)))

        self._loss_input_dict = train_batch
        loss = self._do_loss_init(train_batch)
        for k in sorted(train_batch.accessed_keys):
            if k != "seq_lens" and not k.startswith("state_in_"):
                loss_inputs.append((k, train_batch[k]))

        TFPolicy._initialize_loss(self, loss, loss_inputs)
        if self._grad_stats_fn:
            self._stats_fetches.update(
                self._grad_stats_fn(self, train_batch, self._grads))
        self._sess.run(tf.global_variables_initializer())

    def _do_loss_init(self, train_batch):
        loss = self._loss_fn(self, self.model, self._dist_class, train_batch)
        if self._stats_fn:
            self._stats_fetches.update(self._stats_fn(self, train_batch))
        # override the update ops to be those of the model
        self._update_ops = self.model.update_ops()
        return loss
