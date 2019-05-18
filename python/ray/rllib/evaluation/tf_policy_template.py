from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils.annotations import override, DeveloperAPI


@DeveloperAPI
def build_tf_policy(name,
                    loss_fn,
                    get_default_config=None,
                    stats_fn=None,
                    grad_stats_fn=None,
                    extra_action_fetches_fn=None,
                    postprocess_fn=None,
                    optimizer_fn=None,
                    gradients_fn=None,
                    before_init=None,
                    before_loss_init=None,
                    after_init=None,
                    make_action_sampler=None,
                    mixins=None,
                    get_batch_divisibility_req=None):
    """Helper function for creating a dynamic tf policy at runtime.

    Arguments:
        name (str): name of the policy (e.g., "PPOTFPolicy")
        loss_fn (func): function that returns a loss tensor the policy,
            and dict of experience tensor placeholders
        get_default_config (func): optional function that returns the default
            config to merge with any overrides
        stats_fn (func): optional function that returns a dict of
            TF fetches given the policy and batch input tensors
        grad_stats_fn (func): optional function that returns a dict of
            TF fetches given the policy and loss gradient tensors
        extra_action_fetches_fn (func): optional function that returns
            a dict of TF fetches given the policy object
        postprocess_fn (func): optional experience postprocessing function
            that takes the same args as Policy.postprocess_trajectory()
        optimizer_fn (func): optional function that returns a tf.Optimizer
            given the policy and config
        gradients_fn (func): optional function that returns a list of gradients
            given a tf optimizer and loss tensor. If not specified, this
            defaults to optimizer.compute_gradients(loss)
        before_init (func): optional function to run at the beginning of
            policy init that takes the same arguments as the policy constructor
        before_loss_init (func): optional function to run prior to loss
            init that takes the same arguments as the policy constructor
        after_init (func): optional function to run at the end of policy init
            that takes the same arguments as the policy constructor
        make_action_sampler (func): optional function that returns a
            tuple of action and action prob tensors. The function takes
            (policy, input_dict, obs_space, action_space, config) as its
            arguments
        mixins (list): list of any class mixins for the returned policy class.
            These mixins will be applied in order and will have higher
            precedence than the DynamicTFPolicy class
        get_batch_divisibility_req (func): optional function that returns
            the divisibility requirement for sample batches

    Returns:
        a DynamicTFPolicy instance that uses the specified args
    """

    if not name.endswith("TFPolicy"):
        raise ValueError("Name should match *TFPolicy", name)

    base = DynamicTFPolicy
    while mixins:

        class new_base(mixins.pop(), base):
            pass

        base = new_base

    class policy_cls(base):
        def __init__(self,
                     obs_space,
                     action_space,
                     config,
                     existing_inputs=None):
            if get_default_config:
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

            DynamicTFPolicy.__init__(
                self,
                obs_space,
                action_space,
                config,
                loss_fn,
                stats_fn=stats_fn,
                grad_stats_fn=grad_stats_fn,
                before_loss_init=before_loss_init_wrapper,
                existing_inputs=existing_inputs)

            if after_init:
                after_init(self, obs_space, action_space, config)

        @override(Policy)
        def postprocess_trajectory(self,
                                   sample_batch,
                                   other_agent_batches=None,
                                   episode=None):
            if not postprocess_fn:
                return sample_batch
            return postprocess_fn(self, sample_batch, other_agent_batches,
                                  episode)

        @override(TFPolicy)
        def optimizer(self):
            if optimizer_fn:
                return optimizer_fn(self, self.config)
            else:
                return TFPolicy.optimizer(self)

        @override(TFPolicy)
        def gradients(self, optimizer, loss):
            if gradients_fn:
                return gradients_fn(self, optimizer, loss)
            else:
                return TFPolicy.gradients(self, optimizer, loss)

        @override(TFPolicy)
        def extra_compute_action_fetches(self):
            return dict(
                TFPolicy.extra_compute_action_fetches(self),
                **self._extra_action_fetches)

    policy_cls.__name__ = name
    policy_cls.__qualname__ = name
    return policy_cls
