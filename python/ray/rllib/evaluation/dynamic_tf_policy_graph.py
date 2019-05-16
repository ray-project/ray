from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
import logging
import numpy as np

from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.debug import log_once, summarize
from ray.rllib.utils.tracking_dict import UsageTrackingDict

tf = try_import_tf()

logger = logging.getLogger(__name__)


@DeveloperAPI
def build_tf_graph(name,
                   get_default_config,
                   loss_fn,
                   postprocess_fn=None,
                   make_optimizer=None,
                   make_extra_action_fetches=None):
    """Helper function for creating a dynamic tf policy graph at runtime.

    Arguments:
        name (str): name of the graph (e.g., "PPOPolicyGraph")
        get_default_config (func): function that returns the default config
            to merge with any overrides
        loss_fn (func): function that returns a loss tensor the policy graph,
            and dict of experience tensor placeholders
        postprocess_fn (func): optional experience postprocessing function
            that takes the same args as PolicyGraph.postprocess_trajectory()
        make_optimizer (func): optional function that returns a tf.Optimizer
            given the policy graph object
        make_extra_action_fetches (func): optional function that returns
            a dict of TF fetches given the policy graph object

    Returns:
        a DynamicTFPolicyGraph instance that uses the specified args
    """

    class graph_cls(DynamicTFPolicyGraph):
        def __init__(self, obs_space, action_space, config):
            config = dict(get_default_config(), **config)
            if make_extra_action_fetches is None:
                self._extra_action_fetches = {}
            else:
                self._extra_action_fetches = make_extra_action_fetches(self)
            DynamicTFPolicyGraph.__init__(self, obs_space, action_space,
                                          config, loss_fn)

        @override(PolicyGraph)
        def postprocess_trajectory(self,
                                   sample_batch,
                                   other_agent_batches=None,
                                   episode=None):
            if not postprocess_fn:
                return sample_batch
            return postprocess_fn(self, sample_batch, other_agent_batches,
                                  episode)

        @override(TFPolicyGraph)
        def optimizer(self):
            if make_optimizer:
                return make_optimizer(self)
            else:
                return TFPolicyGraph.optimizer(self)

        @override(TFPolicyGraph)
        def extra_compute_action_fetches(self):
            return dict(
                TFPolicyGraph.extra_compute_action_fetches(self),
                **self._extra_action_fetches)

    graph_cls.__name__ = name
    return graph_cls


class DynamicTFPolicyGraph(TFPolicyGraph):
    """A TFPolicyGraph that auto-defines placeholders dynamically at runtime.

    Initialization of this class occurs in two phases.
      * Phase 1: the model is created and model variables are initialized.
      * Phase 2: a fake batch of data is created, sent to the trajectory
        postprocessor, and then used to create placeholders for the loss
        function. The loss function is initialized with these placeholders.
    """

    def __init__(self,
                 obs_space,
                 action_space,
                 config,
                 loss_fn,
                 autosetup_model=True,
                 autoinit_loss=True,
                 action_sampler=None,
                 action_prob=None,
                 existing_inputs=None):
        self.config = config
        self.autosetup_model = autosetup_model
        self._build_loss = loss_fn

        # Setup standard placeholders
        if existing_inputs is not None:
            obs = existing_inputs[SampleBatch.CUR_OBS]
            prev_actions = existing_inputs[SampleBatch.PREV_ACTIONS]
            prev_rewards = existing_inputs[SampleBatch.PREV_REWARDS]
        else:
            obs = tf.placeholder(
                tf.float32,
                shape=[None] + list(obs_space.shape),
                name="observation")
            prev_actions = ModelCatalog.get_action_placeholder(action_space)
            prev_rewards = tf.placeholder(
                tf.float32, [None], name="prev_reward")

        # Create the model network and action outputs
        if autosetup_model:
            dist_class, self.logit_dim = ModelCatalog.get_action_dist(
                action_space, self.config["model"])
            if existing_inputs:
                existing_state_in = [
                    v for k, v in existing_inputs.items()
                    if k.startswith("state_in_")
                ]
                if existing_state_in:
                    existing_seq_lens = existing_inputs["seq_lens"]
                else:
                    existing_seq_lens = None
            else:
                existing_state_in = []
                existing_seq_lens = None
            self.model = ModelCatalog.get_model(
                {
                    "obs": obs,
                    "prev_actions": prev_actions,
                    "prev_rewards": prev_rewards,
                    "is_training": self._get_is_training_placeholder(),
                },
                obs_space,
                action_space,
                self.logit_dim,
                self.config["model"],
                state_in=existing_state_in,
                seq_lens=existing_seq_lens)
            self.action_dist = dist_class(self.model.outputs)
            action_sampler = self.action_dist.sample()
            action_prob = self.action_dist.sampled_action_prob()
        else:
            self.logit_dim = None
            self.model = None
            self.action_dist = None
            if not action_sampler:
                raise ValueError(
                    "When autosetup_model=False, action_sampler must be "
                    "passed in to the constructor.")

        # Phase 1 init
        sess = tf.get_default_session()
        TFPolicyGraph.__init__(
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
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            prev_action_input=prev_actions,
            prev_reward_input=prev_rewards,
            seq_lens=self.model.seq_lens,
            max_seq_len=config["model"]["max_seq_len"])

        if autoinit_loss:
            self._initialize_loss()

    @override(TFPolicyGraph)
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
        # by convention, the loss inputs are followed by state inputs and then
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
            existing_inputs=input_dict)
        loss = instance._build_loss(instance, input_dict)
        TFPolicyGraph._initialize_loss(
            instance, loss, [(k, existing_inputs[i])
                             for i, (k, _) in enumerate(self._loss_inputs)])
        return instance

    @override(PolicyGraph)
    def get_initial_state(self):
        if self.model:
            return self.model.state_init
        else:
            return []

    def _initialize_loss(self):
        def fake_array(tensor):
            shape = tensor.shape.as_list()
            shape[0] = 1
            return np.zeros(shape, dtype=tensor.dtype.as_numpy_dtype)

        dummy_batch = {
            SampleBatch.PREV_ACTIONS: fake_array(self._prev_action_input),
            SampleBatch.PREV_REWARDS: fake_array(self._prev_reward_input),
            SampleBatch.CUR_OBS: fake_array(self._obs_input),
            SampleBatch.NEXT_OBS: fake_array(self._obs_input),
            SampleBatch.ACTIONS: fake_array(self._sampler),
            SampleBatch.REWARDS: np.array([0], dtype=np.int32),
            SampleBatch.DONES: np.array([False], dtype=np.bool),
        }
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

        loss_input = UsageTrackingDict({
            SampleBatch.PREV_ACTIONS: self._prev_action_input,
            SampleBatch.PREV_REWARDS: self._prev_reward_input,
            SampleBatch.CUR_OBS: self._obs_input,
        })
        loss_inputs = [
            (SampleBatch.PREV_ACTIONS, self._prev_action_input),
            (SampleBatch.PREV_REWARDS, self._prev_reward_input),
            (SampleBatch.CUR_OBS, self._obs_input),
        ]

        for k, v in postprocessed_batch.items():
            if k in loss_input:
                continue
            elif v.dtype == np.object:
                continue  # can't handle arbitrary objects in TF
            shape = (None, ) + v.shape[1:]
            placeholder = tf.placeholder(v.dtype, shape=shape, name=k)
            loss_input[k] = placeholder

        if log_once("loss_init"):
            logger.info(
                "Initializing loss function with dummy input:\n\n{}\n".format(
                    summarize(loss_input)))

        loss = self._build_loss(self, loss_input)
        for k in sorted(loss_input.accessed_keys):
            loss_inputs.append((k, loss_input[k]))

        TFPolicyGraph._initialize_loss(self, loss, loss_inputs)
        self._sess.run(tf.global_variables_initializer())
