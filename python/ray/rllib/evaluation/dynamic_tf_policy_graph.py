from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def build_tf_graph(name,
                   default_config,
                   postprocess_fn,
                   loss_fn,
                   make_optimizer=None):
    class graph_cls(DynamicTFPolicyGraph):
        def __init__(self, obs_space, action_space, config):
            config = dict(default_config, **config)
            DynamicTFPolicyGraph.__init__(self, obs_space, action_space,
                                          config, loss_fn)

        @override(PolicyGraph)
        def postprocess_trajectory(self,
                                   sample_batch,
                                   other_agent_batches=None,
                                   episode=None):
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
    def __init__(self, obs_space, action_space, config, loss_fn):
        self.config = config
        self._build_loss = loss_fn

        # Setup standard placeholders
        obs = tf.placeholder(tf.float32, shape=[None] + list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        prev_actions = ModelCatalog.get_action_placeholder(action_space)
        prev_rewards = tf.placeholder(tf.float32, [None], name="prev_reward")

        # Create the model network and action outputs
        self.model = ModelCatalog.get_model({
            "obs": obs,
            "prev_actions": prev_actions,
            "prev_rewards": prev_rewards,
            "is_training": self._get_is_training_placeholder(),
        }, obs_space, action_space, self.logit_dim, self.config["model"])
        self.action_dist = dist_class(self.model.outputs)

        sess = tf.get_default_session()
        TFPolicyGraph.__init__(
            self,
            obs_space,
            action_space,
            sess,
            obs_input=obs,
            action_sampler=self.action_dist.sample(),
            action_prob=self.action_dist.sampled_action_prob(),
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
        return self.model.state_init

    def _initialize_loss_if_needed(self, postprocessed_batch):
        if self._loss is not None:
            return  # already created

        with self._sess.graph.as_default():
            unroll_tensors = {
                SampleBatch.PREV_ACTIONS: self._prev_action_input,
                SampleBatch.PREV_REWARDS: self._prev_reward_input,
                SampleBatch.CUR_OBS: self._obs_input,
            }
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
                loss_inputs.append((k,
                                    placeholder))  # TODO: prune to used only

            loss = self._build_loss(unroll_tensors, self.action_dist)
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
