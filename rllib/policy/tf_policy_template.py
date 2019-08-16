from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy import eager_tf_policy
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


@DeveloperAPI
def build_tf_policy(name,
                    loss_fn,
                    get_default_config=None,
                    postprocess_fn=None,
                    stats_fn=None,
                    optimizer_fn=None,
                    gradients_fn=None,
                    apply_gradients_fn=None,
                    grad_stats_fn=None,
                    extra_action_fetches_fn=None,
                    extra_learn_fetches_fn=None,
                    before_init=None,
                    before_loss_init=None,
                    after_init=None,
                    make_model=None,
                    action_sampler_fn=None,
                    mixins=None,
                    get_batch_divisibility_req=None,
                    obs_include_prev_action_reward=True):
    """Helper function for creating a dynamic tf policy at runtime.

    Functions will be run in this order to initialize the policy:
        1. Placeholder setup: postprocess_fn
        2. Loss init: loss_fn, stats_fn
        3. Optimizer init: optimizer_fn, gradients_fn, apply_gradients_fn,
                           grad_stats_fn

    This means that you can e.g., depend on any policy attributes created in
    the running of `loss_fn` in later functions such as `stats_fn`.

    In eager mode, the following functions will be run repeatedly on each
    eager execution: loss_fn, stats_fn, gradients_fn, apply_gradients_fn,
    and grad_stats_fn.

    This means that these functions should not define any variables internally,
    otherwise they will fail in eager mode execution. Variable should only
    be created in make_model (if defined).

    Arguments:
        name (str): name of the policy (e.g., "PPOTFPolicy")
        loss_fn (func): function that returns a loss tensor as arguments
            (policy, model, dist_class, train_batch)
        get_default_config (func): optional function that returns the default
            config to merge with any overrides
        postprocess_fn (func): optional experience postprocessing function
            that takes the same args as Policy.postprocess_trajectory()
        stats_fn (func): optional function that returns a dict of
            TF fetches given the policy and batch input tensors
        optimizer_fn (func): optional function that returns a tf.Optimizer
            given the policy and config
        gradients_fn (func): optional function that returns a list of gradients
            given (policy, optimizer, loss). If not specified, this
            defaults to optimizer.compute_gradients(loss)
        apply_gradients_fn (func): optional function that returns an apply
            gradients op given (policy, optimizer, grads_and_vars)
        grad_stats_fn (func): optional function that returns a dict of
            TF fetches given the policy, batch input, and gradient tensors
        extra_action_fetches_fn (func): optional function that returns
            a dict of TF fetches given the policy object
        extra_learn_fetches_fn (func): optional function that returns a dict of
            extra values to fetch and return when learning on a batch
        before_init (func): optional function to run at the beginning of
            policy init that takes the same arguments as the policy constructor
        before_loss_init (func): optional function to run prior to loss
            init that takes the same arguments as the policy constructor
        after_init (func): optional function to run at the end of policy init
            that takes the same arguments as the policy constructor
        make_model (func): optional function that returns a ModelV2 object
            given (policy, obs_space, action_space, config).
            All policy variables should be created in this function. If not
            specified, a default model will be created.
        action_sampler_fn (func): optional function that returns a
            tuple of action and action prob tensors given
            (policy, model, input_dict, obs_space, action_space, config).
            If not specified, a default action distribution will be used.
        mixins (list): list of any class mixins for the returned policy class.
            These mixins will be applied in order and will have higher
            precedence than the DynamicTFPolicy class
        get_batch_divisibility_req (func): optional function that returns
            the divisibility requirement for sample batches
        obs_include_prev_action_reward (bool): whether to include the
            previous action and reward in the model input

    Returns:
        a DynamicTFPolicy instance that uses the specified args
    """

    original_kwargs = locals().copy()
    base = add_mixins(DynamicTFPolicy, mixins)

    class policy_cls(base):
        def __init__(self,
                     obs_space,
                     action_space,
                     config,
                     existing_model=None,
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
                make_model=make_model,
                action_sampler_fn=action_sampler_fn,
                existing_model=existing_model,
                existing_inputs=existing_inputs,
                get_batch_divisibility_req=get_batch_divisibility_req,
                obs_include_prev_action_reward=obs_include_prev_action_reward)

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
        def build_apply_op(self, optimizer, grads_and_vars):
            if apply_gradients_fn:
                return apply_gradients_fn(self, optimizer, grads_and_vars)
            else:
                return TFPolicy.build_apply_op(self, optimizer, grads_and_vars)

        @override(TFPolicy)
        def extra_compute_action_fetches(self):
            return dict(
                TFPolicy.extra_compute_action_fetches(self),
                **self._extra_action_fetches)

        @override(TFPolicy)
        def extra_compute_grad_fetches(self):
            if extra_learn_fetches_fn:
                # auto-add empty learner stats dict if needed
                return dict({
                    LEARNER_STATS_KEY: {}
                }, **extra_learn_fetches_fn(self))
            else:
                return TFPolicy.extra_compute_grad_fetches(self)

    @staticmethod
    def with_updates(**overrides):
        return build_tf_policy(**dict(original_kwargs, **overrides))

    @staticmethod
    def as_eager():
        return eager_tf_policy.build_eager_tf_policy(**original_kwargs)

    policy_cls.with_updates = with_updates
    policy_cls.as_eager = as_eager
    policy_cls.__name__ = name
    policy_cls.__qualname__ = name
    return policy_cls
