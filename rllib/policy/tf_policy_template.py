import gym
from typing import Callable, Dict, List, Optional, Tuple, Type, Union

from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy import eager_tf_policy
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import add_mixins, force_list
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import AgentID, ModelGradients, TensorType, \
    TrainerConfigDict

tf1, tf, tfv = try_import_tf()


@DeveloperAPI
def build_tf_policy(
        name: str,
        *,
        loss_fn: Callable[[
            Policy, ModelV2, Type[TFActionDistribution], SampleBatch
        ], Union[TensorType, List[TensorType]]],
        get_default_config: Optional[Callable[[None],
                                              TrainerConfigDict]] = None,
        postprocess_fn: Optional[Callable[[
            Policy, SampleBatch, Optional[Dict[AgentID, SampleBatch]],
            Optional["MultiAgentEpisode"]
        ], SampleBatch]] = None,
        stats_fn: Optional[Callable[[Policy, SampleBatch], Dict[
            str, TensorType]]] = None,
        optimizer_fn: Optional[Callable[[
            Policy, TrainerConfigDict
        ], "tf.keras.optimizers.Optimizer"]] = None,
        gradients_fn: Optional[Callable[[
            Policy, "tf.keras.optimizers.Optimizer", TensorType
        ], ModelGradients]] = None,
        apply_gradients_fn: Optional[Callable[[
            Policy, "tf.keras.optimizers.Optimizer", ModelGradients
        ], "tf.Operation"]] = None,
        grad_stats_fn: Optional[Callable[[Policy, SampleBatch, ModelGradients],
                                         Dict[str, TensorType]]] = None,
        extra_action_out_fn: Optional[Callable[[Policy], Dict[
            str, TensorType]]] = None,
        extra_learn_fetches_fn: Optional[Callable[[Policy], Dict[
            str, TensorType]]] = None,
        validate_spaces: Optional[Callable[
            [Policy, gym.Space, gym.Space, TrainerConfigDict], None]] = None,
        before_init: Optional[Callable[
            [Policy, gym.Space, gym.Space, TrainerConfigDict], None]] = None,
        before_loss_init: Optional[Callable[[
            Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
        ], None]] = None,
        after_init: Optional[Callable[
            [Policy, gym.Space, gym.Space, TrainerConfigDict], None]] = None,
        make_model: Optional[Callable[[
            Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
        ], ModelV2]] = None,
        action_sampler_fn: Optional[Callable[[TensorType, List[
            TensorType]], Tuple[TensorType, TensorType]]] = None,
        action_distribution_fn: Optional[Callable[[
            Policy, ModelV2, TensorType, TensorType, TensorType
        ], Tuple[TensorType, type, List[TensorType]]]] = None,
        mixins: Optional[List[type]] = None,
        get_batch_divisibility_req: Optional[Callable[[Policy], int]] = None,
        # Deprecated args.
        obs_include_prev_action_reward=DEPRECATED_VALUE,
        extra_action_fetches_fn: Optional[Callable[[Policy], Dict[
            str, TensorType]]] = None,
) -> Type[DynamicTFPolicy]:
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

    Args:
        name (str): Name of the policy (e.g., "PPOTFPolicy").
        loss_fn (Callable[[
            Policy, ModelV2, Type[TFActionDistribution], SampleBatch],
            Union[TensorType, List[TensorType]]]): Callable for calculating a
            loss tensor.
        get_default_config (Optional[Callable[[None], TrainerConfigDict]]):
            Optional callable that returns the default config to merge with any
            overrides. If None, uses only(!) the user-provided
            PartialTrainerConfigDict as dict for this Policy.
        postprocess_fn (Optional[Callable[[Policy, SampleBatch,
            Optional[Dict[AgentID, SampleBatch]], MultiAgentEpisode], None]]):
            Optional callable for post-processing experience batches (called
            after the parent class' `postprocess_trajectory` method).
        stats_fn (Optional[Callable[[Policy, SampleBatch],
            Dict[str, TensorType]]]): Optional callable that returns a dict of
            TF tensors to fetch given the policy and batch input tensors. If
            None, will not compute any stats.
        optimizer_fn (Optional[Callable[[Policy, TrainerConfigDict],
            "tf.keras.optimizers.Optimizer"]]): Optional callable that returns
            a tf.Optimizer given the policy and config. If None, will call
            the base class' `optimizer()` method instead (which returns a
            tf1.train.AdamOptimizer).
        gradients_fn (Optional[Callable[[Policy,
            "tf.keras.optimizers.Optimizer", TensorType], ModelGradients]]):
            Optional callable that returns a list of gradients. If None,
            this defaults to optimizer.compute_gradients([loss]).
        apply_gradients_fn (Optional[Callable[[Policy,
            "tf.keras.optimizers.Optimizer", ModelGradients],
            "tf.Operation"]]): Optional callable that returns an apply
            gradients op given policy, tf-optimizer, and grads_and_vars. If
            None, will call the base class' `build_apply_op()` method instead.
        grad_stats_fn (Optional[Callable[[Policy, SampleBatch, ModelGradients],
            Dict[str, TensorType]]]): Optional callable that returns a dict of
            TF fetches given the policy, batch input, and gradient tensors. If
            None, will not collect any gradient stats.
        extra_action_out_fn (Optional[Callable[[Policy],
            Dict[str, TensorType]]]): Optional callable that returns
            a dict of TF fetches given the policy object. If None, will not
            perform any extra fetches.
        extra_learn_fetches_fn (Optional[Callable[[Policy],
            Dict[str, TensorType]]]): Optional callable that returns a dict of
            extra values to fetch and return when learning on a batch. If None,
            will call the base class' `extra_compute_grad_fetches()` method
            instead.
        validate_spaces (Optional[Callable[[Policy, gym.Space, gym.Space,
            TrainerConfigDict], None]]): Optional callable that takes the
            Policy, observation_space, action_space, and config to check
            the spaces for correctness. If None, no spaces checking will be
            done.
        before_init (Optional[Callable[[Policy, gym.Space, gym.Space,
            TrainerConfigDict], None]]): Optional callable to run at the
            beginning of policy init that takes the same arguments as the
            policy constructor. If None, this step will be skipped.
        before_loss_init (Optional[Callable[[Policy, gym.spaces.Space,
            gym.spaces.Space, TrainerConfigDict], None]]): Optional callable to
            run prior to loss init. If None, this step will be skipped.
        after_init (Optional[Callable[[Policy, gym.Space, gym.Space,
            TrainerConfigDict], None]]): Optional callable to run at the end of
            policy init. If None, this step will be skipped.
        make_model (Optional[Callable[[Policy, gym.spaces.Space,
            gym.spaces.Space, TrainerConfigDict], ModelV2]]): Optional callable
            that returns a ModelV2 object.
            All policy variables should be created in this function. If None,
            a default ModelV2 object will be created.
        action_sampler_fn (Optional[Callable[[TensorType, List[TensorType]],
            Tuple[TensorType, TensorType]]]): A callable returning a sampled
            action and its log-likelihood given observation and state inputs.
            If None, will either use `action_distribution_fn` or
            compute actions by calling self.model, then sampling from the
            so parameterized action distribution.
        action_distribution_fn (Optional[Callable[[Policy, ModelV2, TensorType,
            TensorType, TensorType],
            Tuple[TensorType, type, List[TensorType]]]]): Optional callable
            returning distribution inputs (parameters), a dist-class to
            generate an action distribution object from, and internal-state
            outputs (or an empty list if not applicable). If None, will either
            use `action_sampler_fn` or compute actions by calling self.model,
            then sampling from the so parameterized action distribution.
        mixins (Optional[List[type]]): Optional list of any class mixins for
            the returned policy class. These mixins will be applied in order
            and will have higher precedence than the DynamicTFPolicy class.
        get_batch_divisibility_req (Optional[Callable[[Policy], int]]):
            Optional callable that returns the divisibility requirement for
            sample batches. If None, will assume a value of 1.

    Returns:
        Type[DynamicTFPolicy]: A child class of DynamicTFPolicy based on the
            specified args.
    """
    original_kwargs = locals().copy()
    base = add_mixins(DynamicTFPolicy, mixins)

    if extra_action_fetches_fn is not None:
        deprecation_warning(
            old="extra_action_fetches_fn",
            new="extra_action_out_fn",
            error=False)
        extra_action_out_fn = extra_action_fetches_fn

    if obs_include_prev_action_reward != DEPRECATED_VALUE:
        deprecation_warning(old="obs_include_prev_action_reward", error=False)

    class policy_cls(base):
        def __init__(self,
                     obs_space,
                     action_space,
                     config,
                     existing_model=None,
                     existing_inputs=None):
            if get_default_config:
                config = dict(get_default_config(), **config)

            if validate_spaces:
                validate_spaces(self, obs_space, action_space, config)

            if before_init:
                before_init(self, obs_space, action_space, config)

            def before_loss_init_wrapper(policy, obs_space, action_space,
                                         config):
                if before_loss_init:
                    before_loss_init(policy, obs_space, action_space, config)
                if extra_action_out_fn is None:
                    policy._extra_action_fetches = {}
                else:
                    policy._extra_action_fetches = extra_action_out_fn(policy)
                    policy._extra_action_fetches = extra_action_out_fn(policy)

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
        def postprocess_trajectory(self,
                                   sample_batch,
                                   other_agent_batches=None,
                                   episode=None):
            # Call super's postprocess_trajectory first.
            sample_batch = Policy.postprocess_trajectory(self, sample_batch)
            if postprocess_fn:
                return postprocess_fn(self, sample_batch, other_agent_batches,
                                      episode)
            return sample_batch

        @override(TFPolicy)
        def optimizer(self):
            if optimizer_fn:
                optimizers = optimizer_fn(self, self.config)
            else:
                optimizers = base.optimizer(self)
            optimizers = force_list(optimizers)
            if getattr(self, "exploration", None):
                optimizers = self.exploration.get_exploration_optimizer(
                    optimizers)
            # TODO: (sven) Allow tf-eager policy to have more than 1 optimizer.
            #  Just like torch Policy does.
            return optimizers[0] if optimizers else None

        @override(TFPolicy)
        def gradients(self, optimizer, loss):
            if gradients_fn:
                return gradients_fn(self, optimizer, loss)
            else:
                return base.gradients(self, optimizer, loss)

        @override(TFPolicy)
        def build_apply_op(self, optimizer, grads_and_vars):
            if apply_gradients_fn:
                return apply_gradients_fn(self, optimizer, grads_and_vars)
            else:
                return base.build_apply_op(self, optimizer, grads_and_vars)

        @override(TFPolicy)
        def extra_compute_action_fetches(self):
            return dict(
                base.extra_compute_action_fetches(self),
                **self._extra_action_fetches)

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
                return dict({
                    LEARNER_STATS_KEY: {}
                }, **extra_learn_fetches_fn(self))
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
        return eager_tf_policy.build_eager_tf_policy(**original_kwargs)

    policy_cls.with_updates = staticmethod(with_updates)
    policy_cls.as_eager = staticmethod(as_eager)
    policy_cls.__name__ = name
    policy_cls.__qualname__ = name
    return policy_cls
