from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
                   make_optimizer=None):
    """Helper function for creating a dynamic tf policy graph at runtime.

    Arguments:
        name (str): name of the graph (e.g., "PGPolicyGraph")
        get_default_config (func): function that returns the default config
            to merge with any overrides
        loss_fn (func): function that returns a loss tensor the policy graph,
            and dict of experience tensor placeholders
        postprocess_fn (func): optional experience postprocessing function
            that takes the same args as PolicyGraph.postprocess_trajectory()
        make_optimizer (func): optional function that returns a tf.Optimizer
            given the policy graph object

    Returns:
        a DynamicTFPolicyGraph instance that uses the specified args
    """

    class graph_cls(DynamicTFPolicyGraph):
        def __init__(self, obs_space, action_space, config):
            config = dict(get_default_config(), **config)
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

    graph_cls.__name__ = name
    return graph_cls


class DynamicTFPolicyGraph(TFPolicyGraph):
    """A TFPolicyGraph that auto-defines placeholders dynamically at runtime.

    The loss function of this class is not initialized until the first batch
    of experiences is collected from the environment. At that point we
    dynamically generate TF placeholders based on the batch keys and values.
    which are passed into the user-defined loss function.
    """

    def __init__(self,
                 obs_space,
                 action_space,
                 config,
                 loss_fn,
                 autosetup_model=True,
                 action_sampler=None,
                 action_prob=None):
        self.config = config
        self._build_loss = loss_fn

        # Setup standard placeholders
        obs = tf.placeholder(tf.float32, shape=[None] + list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        prev_actions = ModelCatalog.get_action_placeholder(action_space)
        prev_rewards = tf.placeholder(tf.float32, [None], name="prev_reward")

        # Create the model network and action outputs
        if autosetup_model:
            self.model = ModelCatalog.get_model({
                "obs": obs,
                "prev_actions": prev_actions,
                "prev_rewards": prev_rewards,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, action_space, self.logit_dim, self.config["model"])
            self.action_dist = dist_class(self.model.outputs)
            action_sampler = self.action_dist.sample()
            action_prob = self.action_dist.sampled_action_prob()
        else:
            self.model = None
            self.action_dist = None
            if not action_sampler:
                raise ValueError(
                    "When autosetup_model=False, action_sampler must be "
                    "passed in to the constructor.")

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
        sess.run(tf.global_variables_initializer())

    @override(PolicyGraph)
    def get_initial_state(self):
        if self.model:
            return self.model.state_init
        else:
            return []

    def _initialize_loss_if_needed(self, postprocessed_batch):
        if self._loss is not None:
            return  # already created

        with self._sess.graph.as_default():
            unroll_tensors = UsageTrackingDict({
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
                if k in unroll_tensors:
                    continue
                elif v.dtype == np.object:
                    continue  # can't handle arbitrary objects in TF
                shape = (None, ) + v.shape[1:]
                placeholder = tf.placeholder(v.dtype, shape=shape, name=k)
                unroll_tensors[k] = placeholder

            if log_once("loss_init"):
                logger.info(
                    "Initializing loss function with inputs:\n\n{}\n".format(
                        summarize(unroll_tensors)))

            loss = self._build_loss(self, unroll_tensors)
            for k in unroll_tensors.accessed_keys:
                loss_inputs.append((k, unroll_tensors[k]))

            TFPolicyGraph._initialize_loss(self, loss, loss_inputs)
            self._sess.run(tf.global_variables_initializer())

    @override(PolicyGraph)
    def compute_gradients(self, postprocessed_batch):
        self._initialize_loss_if_needed(postprocessed_batch)
        return TFPolicyGraph.compute_gradients(self, postprocessed_batch)

    @override(PolicyGraph)
    def learn_on_batch(self, postprocessed_batch):
        self._initialize_loss_if_needed(postprocessed_batch)
        return TFPolicyGraph.learn_on_batch(self, postprocessed_batch)
