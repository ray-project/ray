from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.evaluation.dynamic_tf_policy_graph import DynamicTFPolicyGraph
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.utils.annotations import override, DeveloperAPI


@DeveloperAPI
def build_tf_graph(name,
                   get_default_config,
                   loss_fn,
                   stats_fn=None,
                   extra_action_fetches_fn=None,
                   postprocess_fn=None,
                   optimizer_fn=None,
                   gradients_fn=None,
                   before_init=None,
                   before_loss_init=None,
                   after_init=None,
                   mixins=None):
    """Helper function for creating a dynamic tf policy graph at runtime.

    Arguments:
        name (str): name of the graph (e.g., "PPOPolicyGraph")
        get_default_config (func): function that returns the default config
            to merge with any overrides
        loss_fn (func): function that returns a loss tensor the policy graph,
            and dict of experience tensor placeholders
        stats_fn (func): optional function that returns a dict of
            TF fetches given the policy graph and batch input tensors
        extra_action_fetches_fn (func): optional function that returns
            a dict of TF fetches given the policy graph object
        postprocess_fn (func): optional experience postprocessing function
            that takes the same args as PolicyGraph.postprocess_trajectory()
        optimizer_fn (func): optional function that returns a tf.Optimizer
            given the policy graph object
        gradients_fn (func): optional function that returns a list of gradients
            given a tf optimizer and loss tensor. If not specified, this
            defaults to optimizer.compute_gradients(loss)
        before_init (func): optional function to run at the beginning of
            __init__ that takes the same arguments as __init__
        before_loss_init (func): optional function to run prior to loss
            init that takes the same arguments as __init__
        after_init (func): optional function to run at the end of __init__
            that takes the same arguments as __init__
        mixins (list): list of any class mixins for the returned policy class

    Returns:
        a DynamicTFPolicyGraph instance that uses the specified args
    """

    if mixins is None:
        mixins = []

    class graph_cls(*mixins, DynamicTFPolicyGraph):
        def __init__(self,
                     obs_space,
                     action_space,
                     config,
                     existing_inputs=None):
            config = dict(get_default_config(), **config)

            if before_init:
                before_init(self, obs_space, action_space, config)

            def before_loss_init_wrapper(policy, obs_space, action_space,
                                         config):
                if before_loss_init:
                    before_loss_init(policy, obs_space, action_space, config)
                if extra_action_fetches_fn is None:
                    self._extra_action_fetches = {}
                else:
                    self._extra_action_fetches = extra_action_fetches_fn(self)

            DynamicTFPolicyGraph.__init__(
                self,
                obs_space,
                action_space,
                config,
                loss_fn,
                stats_fn,
                before_loss_init=before_loss_init_wrapper,
                existing_inputs=existing_inputs)

            if after_init:
                after_init(self, obs_space, action_space, config)

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
            if optimizer_fn:
                return optimizer_fn(self)
            else:
                return TFPolicyGraph.optimizer(self)

        @override(TFPolicyGraph)
        def gradients(self, optimizer, loss):
            if gradients_fn:
                return gradients_fn(self, optimizer, loss)
            else:
                return TFPolicyGraph.gradients(self, optimizer, loss)

        @override(TFPolicyGraph)
        def extra_compute_action_fetches(self):
            return dict(
                TFPolicyGraph.extra_compute_action_fetches(self),
                **self._extra_action_fetches)

    graph_cls.__name__ = name
    return graph_cls
