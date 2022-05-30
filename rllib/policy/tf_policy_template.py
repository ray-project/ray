from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy import eager_tf_policy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import add_mixins, force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    deprecation_warning,
    Deprecated,
    DEPRECATED_VALUE,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY

tf1, tf, tfv = try_import_tf()


@Deprecated(
    new="create sub-classes from ray.rllib.policy.dynamic_tf_policy_v2::"
    "DynamicTFPolicyV2 and ray.rllib.policy.eager_tf_policy_v2::EagerTFPolicyV2 "
    "(see ray.rllib.agents.ppo.ppo_tf_policy.py for an example) and override "
    "needed methods",
    error=False,
)
def build_tf_policy(
    name: str,
    *,
    loss_fn=None,
    get_default_config=None,
    postprocess_fn=None,
    stats_fn=None,
    optimizer_fn=None,
    compute_gradients_fn=None,
    apply_gradients_fn=None,
    grad_stats_fn=None,
    extra_action_out_fn=None,
    extra_learn_fetches_fn=None,
    validate_spaces=None,
    before_init=None,
    before_loss_init=None,
    after_init=None,
    make_model=None,
    action_sampler_fn=None,
    action_distribution_fn=None,
    mixins=None,
    get_batch_divisibility_req=None,
    # Deprecated args.
    obs_include_prev_action_reward=DEPRECATED_VALUE,
    extra_action_fetches_fn=None,  # Use `extra_action_out_fn`.
    gradients_fn=None,  # Use `compute_gradients_fn`.
):
    original_kwargs = locals().copy()
    base = add_mixins(DynamicTFPolicy, mixins)

    if obs_include_prev_action_reward != DEPRECATED_VALUE:
        deprecation_warning(old="obs_include_prev_action_reward", error=False)

    if extra_action_fetches_fn is not None:
        deprecation_warning(
            old="extra_action_fetches_fn", new="extra_action_out_fn", error=False
        )
        extra_action_out_fn = extra_action_fetches_fn

    if gradients_fn is not None:
        deprecation_warning(old="gradients_fn", new="compute_gradients_fn", error=False)
        compute_gradients_fn = gradients_fn

    class policy_cls(base):
        def __init__(
            self,
            obs_space,
            action_space,
            config,
            existing_model=None,
            existing_inputs=None,
        ):
            if get_default_config:
                config = dict(get_default_config(), **config)

            if validate_spaces:
                validate_spaces(self, obs_space, action_space, config)

            if before_init:
                before_init(self, obs_space, action_space, config)

            def before_loss_init_wrapper(policy, obs_space, action_space, config):
                if before_loss_init:
                    before_loss_init(policy, obs_space, action_space, config)

                if extra_action_out_fn is None or policy._is_tower:
                    extra_action_fetches = {}
                else:
                    extra_action_fetches = extra_action_out_fn(policy)

                if hasattr(policy, "_extra_action_fetches"):
                    policy._extra_action_fetches.update(extra_action_fetches)
                else:
                    policy._extra_action_fetches = extra_action_fetches

            DynamicTFPolicy.__init__(
                self,
                obs_space=obs_space,
                action_space=action_space,
                config=config,
                loss_fn=loss_fn,
                stats_fn=stats_fn,
                grad_stats_fn=grad_stats_fn,
                before_loss_init=before_loss_init_wrapper,
                make_model=make_model,
                action_sampler_fn=action_sampler_fn,
                action_distribution_fn=action_distribution_fn,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
                get_batch_divisibility_req=get_batch_divisibility_req,
            )

            if after_init:
                after_init(self, obs_space, action_space, config)

            # Got to reset global_timestep again after this fake run-through.
            self.global_timestep = 0

        @override(Policy)
        def postprocess_trajectory(
            self, sample_batch, other_agent_batches=None, episode=None
        ):
            # Call super's postprocess_trajectory first.
            sample_batch = Policy.postprocess_trajectory(self, sample_batch)
            if postprocess_fn:
                return postprocess_fn(self, sample_batch, other_agent_batches, episode)
            return sample_batch

        @override(TFPolicy)
        def optimizer(self):
            if optimizer_fn:
                optimizers = optimizer_fn(self, self.config)
            else:
                optimizers = base.optimizer(self)
            optimizers = force_list(optimizers)
            if getattr(self, "exploration", None):
                optimizers = self.exploration.get_exploration_optimizer(optimizers)

            # No optimizers produced -> Return None.
            if not optimizers:
                return None
            # New API: Allow more than one optimizer to be returned.
            # -> Return list.
            elif self.config["_tf_policy_handles_more_than_one_loss"]:
                return optimizers
            # Old API: Return a single LocalOptimizer.
            else:
                return optimizers[0]

        @override(TFPolicy)
        def gradients(self, optimizer, loss):
            optimizers = force_list(optimizer)
            losses = force_list(loss)

            if compute_gradients_fn:
                # New API: Allow more than one optimizer -> Return a list of
                # lists of gradients.
                if self.config["_tf_policy_handles_more_than_one_loss"]:
                    return compute_gradients_fn(self, optimizers, losses)
                # Old API: Return a single List of gradients.
                else:
                    return compute_gradients_fn(self, optimizers[0], losses[0])
            else:
                return base.gradients(self, optimizers, losses)

        @override(TFPolicy)
        def build_apply_op(self, optimizer, grads_and_vars):
            if apply_gradients_fn:
                return apply_gradients_fn(self, optimizer, grads_and_vars)
            else:
                return base.build_apply_op(self, optimizer, grads_and_vars)

        @override(TFPolicy)
        def extra_compute_action_fetches(self):
            return dict(
                base.extra_compute_action_fetches(self), **self._extra_action_fetches
            )

        @override(TFPolicy)
        def extra_compute_grad_fetches(self):
            if extra_learn_fetches_fn:
                # TODO: (sven) in torch, extra_learn_fetches do not exist.
                #  Hence, things like td_error are returned by the stats_fn
                #  and end up under the LEARNER_STATS_KEY. We should
                #  change tf to do this as well. However, this will confilct
                #  the handling of LEARNER_STATS_KEY inside the multi-GPU
                #  train op.
                # Auto-add empty learner stats dict if needed.
                return dict({LEARNER_STATS_KEY: {}}, **extra_learn_fetches_fn(self))
            else:
                return base.extra_compute_grad_fetches(self)

    def with_updates(**overrides):
        """Allows creating a TFPolicy cls based on settings of another one.

        Keyword Args:
            **overrides: The settings (passed into `build_tf_policy`) that
                should be different from the class that this method is called
                on.

        Returns:
            type: A new TFPolicy sub-class.

        Examples:
        >> MySpecialDQNPolicyClass = DQNTFPolicy.with_updates(
        ..    name="MySpecialDQNPolicyClass",
        ..    loss_function=[some_new_loss_function],
        .. )
        """
        return build_tf_policy(**dict(original_kwargs, **overrides))

    def as_eager():
        return eager_tf_policy._build_eager_tf_policy(**original_kwargs)

    policy_cls.with_updates = staticmethod(with_updates)
    policy_cls.as_eager = staticmethod(as_eager)
    policy_cls.__name__ = name
    policy_cls.__qualname__ = name
    return policy_cls
