import gym
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils import add_mixins, force_list
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import convert_to_non_torch_type
from ray.rllib.utils.typing import TensorType, TrainerConfigDict

torch, _ = try_import_torch()


@DeveloperAPI
def build_torch_policy(
        name: str,
        *,
        loss_fn: Optional[Callable[[
            Policy, ModelV2, Type[TorchDistributionWrapper], SampleBatch
        ], Union[TensorType, List[TensorType]]]],
        get_default_config: Optional[Callable[[], TrainerConfigDict]] = None,
        stats_fn: Optional[Callable[[Policy, SampleBatch], Dict[
            str, TensorType]]] = None,
        postprocess_fn: Optional[Callable[[
            Policy, SampleBatch, Optional[Dict[Any, SampleBatch]], Optional[
                "MultiAgentEpisode"]
        ], SampleBatch]] = None,
        extra_action_out_fn: Optional[Callable[[
            Policy, Dict[str, TensorType], List[TensorType], ModelV2,
            TorchDistributionWrapper
        ], Dict[str, TensorType]]] = None,
        extra_grad_process_fn: Optional[Callable[[
            Policy, "torch.optim.Optimizer", TensorType
        ], Dict[str, TensorType]]] = None,
        # TODO: (sven) Replace "fetches" with "process".
        extra_learn_fetches_fn: Optional[Callable[[Policy], Dict[
            str, TensorType]]] = None,
        optimizer_fn: Optional[Callable[[Policy, TrainerConfigDict],
                                        "torch.optim.Optimizer"]] = None,
        validate_spaces: Optional[Callable[
            [Policy, gym.Space, gym.Space, TrainerConfigDict], None]] = None,
        before_init: Optional[Callable[
            [Policy, gym.Space, gym.Space, TrainerConfigDict], None]] = None,
        before_loss_init: Optional[Callable[[
            Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
        ], None]] = None,
        after_init: Optional[Callable[
            [Policy, gym.Space, gym.Space, TrainerConfigDict], None]] = None,
        _after_loss_init: Optional[Callable[[
            Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
        ], None]] = None,
        action_sampler_fn: Optional[Callable[[TensorType, List[
            TensorType]], Tuple[TensorType, TensorType]]] = None,
        action_distribution_fn: Optional[Callable[[
            Policy, ModelV2, TensorType, TensorType, TensorType
        ], Tuple[TensorType, type, List[TensorType]]]] = None,
        make_model: Optional[Callable[[
            Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
        ], ModelV2]] = None,
        make_model_and_action_dist: Optional[Callable[[
            Policy, gym.spaces.Space, gym.spaces.Space, TrainerConfigDict
        ], Tuple[ModelV2, Type[TorchDistributionWrapper]]]] = None,
        apply_gradients_fn: Optional[Callable[
            [Policy, "torch.optim.Optimizer"], None]] = None,
        mixins: Optional[List[type]] = None,
        get_batch_divisibility_req: Optional[Callable[[Policy], int]] = None
) -> Type[TorchPolicy]:
    """Helper function for creating a torch policy class at runtime.

    Args:
        name (str): name of the policy (e.g., "PPOTorchPolicy")
        loss_fn (Optional[Callable[[Policy, ModelV2,
            Type[TorchDistributionWrapper], SampleBatch], Union[TensorType,
            List[TensorType]]]]): Callable that returns a loss tensor.
        get_default_config (Optional[Callable[[None], TrainerConfigDict]]):
            Optional callable that returns the default config to merge with any
            overrides. If None, uses only(!) the user-provided
            PartialTrainerConfigDict as dict for this Policy.
        postprocess_fn (Optional[Callable[[Policy, SampleBatch,
            Optional[Dict[Any, SampleBatch]], Optional["MultiAgentEpisode"]],
            SampleBatch]]): Optional callable for post-processing experience
            batches (called after the super's `postprocess_trajectory` method).
        stats_fn (Optional[Callable[[Policy, SampleBatch],
            Dict[str, TensorType]]]): Optional callable that returns a dict of
            values given the policy and training batch. If None,
            will use `TorchPolicy.extra_grad_info()` instead. The stats dict is
            used for logging (e.g. in TensorBoard).
        extra_action_out_fn (Optional[Callable[[Policy, Dict[str, TensorType],
            List[TensorType], ModelV2, TorchDistributionWrapper]], Dict[str,
            TensorType]]]): Optional callable that returns a dict of extra
            values to include in experiences. If None, no extra computations
            will be performed.
        extra_grad_process_fn (Optional[Callable[[Policy,
            "torch.optim.Optimizer", TensorType], Dict[str, TensorType]]]):
            Optional callable that is called after gradients are computed and
            returns a processing info dict. If None, will call the
            `TorchPolicy.extra_grad_process()` method instead.
        # TODO: (sven) dissolve naming mismatch between "learn" and "compute.."
        extra_learn_fetches_fn (Optional[Callable[[Policy],
            Dict[str, TensorType]]]): Optional callable that returns a dict of
            extra tensors from the policy after loss evaluation. If None,
            will call the `TorchPolicy.extra_compute_grad_fetches()` method
            instead.
        optimizer_fn (Optional[Callable[[Policy, TrainerConfigDict],
            "torch.optim.Optimizer"]]): Optional callable that returns a
            torch optimizer given the policy and config. If None, will call
            the `TorchPolicy.optimizer()` method instead (which returns a
            torch Adam optimizer).
        validate_spaces (Optional[Callable[[Policy, gym.Space, gym.Space,
            TrainerConfigDict], None]]): Optional callable that takes the
            Policy, observation_space, action_space, and config to check for
            correctness. If None, no spaces checking will be done.
        before_init (Optional[Callable[[Policy, gym.Space, gym.Space,
            TrainerConfigDict], None]]): Optional callable to run at the
            beginning of `Policy.__init__` that takes the same arguments as
            the Policy constructor. If None, this step will be skipped.
        before_loss_init (Optional[Callable[[Policy, gym.spaces.Space,
            gym.spaces.Space, TrainerConfigDict], None]]): Optional callable to
            run prior to loss init. If None, this step will be skipped.
        after_init (Optional[Callable[[Policy, gym.Space, gym.Space,
            TrainerConfigDict], None]]): DEPRECATED: Use `before_loss_init`
            instead.
        _after_loss_init (Optional[Callable[[Policy, gym.spaces.Space,
            gym.spaces.Space, TrainerConfigDict], None]]): Optional callable to
            run after the loss init. If None, this step will be skipped.
            This will be deprecated at some point and renamed into `after_init`
            to match `build_tf_policy()` behavior.
        action_sampler_fn (Optional[Callable[[TensorType, List[TensorType]],
            Tuple[TensorType, TensorType]]]): Optional callable returning a
            sampled action and its log-likelihood given some (obs and state)
            inputs. If None, will either use `action_distribution_fn` or
            compute actions by calling self.model, then sampling from the
            so parameterized action distribution.
        action_distribution_fn (Optional[Callable[[Policy, ModelV2, TensorType,
            TensorType, TensorType], Tuple[TensorType,
            Type[TorchDistributionWrapper], List[TensorType]]]]): A callable
            that takes the Policy, Model, the observation batch, an
            explore-flag, a timestep, and an is_training flag and returns a
            tuple of a) distribution inputs (parameters), b) a dist-class to
            generate an action distribution object from, and c) internal-state
            outputs (empty list if not applicable). If None, will either use
            `action_sampler_fn` or compute actions by calling self.model,
            then sampling from the parameterized action distribution.
        make_model (Optional[Callable[[Policy, gym.spaces.Space,
            gym.spaces.Space, TrainerConfigDict], ModelV2]]): Optional callable
            that takes the same arguments as Policy.__init__ and returns a
            model instance. The distribution class will be determined
            automatically. Note: Only one of `make_model` or
            `make_model_and_action_dist` should be provided. If both are None,
            a default Model will be created.
        make_model_and_action_dist (Optional[Callable[[Policy,
            gym.spaces.Space, gym.spaces.Space, TrainerConfigDict],
            Tuple[ModelV2, Type[TorchDistributionWrapper]]]]): Optional
            callable that takes the same arguments as Policy.__init__ and
            returns a tuple of model instance and torch action distribution
            class.
            Note: Only one of `make_model` or `make_model_and_action_dist`
            should be provided. If both are None, a default Model will be
            created.
        apply_gradients_fn (Optional[Callable[[Policy,
            "torch.optim.Optimizer"], None]]): Optional callable that
            takes a grads list and applies these to the Model's parameters.
            If None, will call the `TorchPolicy.apply_gradients()` method
            instead.
        mixins (Optional[List[type]]): Optional list of any class mixins for
            the returned policy class. These mixins will be applied in order
            and will have higher precedence than the TorchPolicy class.
        get_batch_divisibility_req (Optional[Callable[[Policy], int]]):
            Optional callable that returns the divisibility requirement for
            sample batches. If None, will assume a value of 1.

    Returns:
        Type[TorchPolicy]: TorchPolicy child class constructed from the
            specified args.
    """

    original_kwargs = locals().copy()
    base = add_mixins(TorchPolicy, mixins)

    class policy_cls(base):
        def __init__(self, obs_space, action_space, config):
            if get_default_config:
                config = dict(get_default_config(), **config)
            self.config = config

            if validate_spaces:
                validate_spaces(self, obs_space, action_space, self.config)

            if before_init:
                before_init(self, obs_space, action_space, self.config)

            # Model is customized (use default action dist class).
            if make_model:
                assert make_model_and_action_dist is None, \
                    "Either `make_model` or `make_model_and_action_dist`" \
                    " must be None!"
                self.model = make_model(self, obs_space, action_space, config)
                dist_class, _ = ModelCatalog.get_action_dist(
                    action_space, self.config["model"], framework="torch")
            # Model and action dist class are customized.
            elif make_model_and_action_dist:
                self.model, dist_class = make_model_and_action_dist(
                    self, obs_space, action_space, config)
            # Use default model and default action dist.
            else:
                dist_class, logit_dim = ModelCatalog.get_action_dist(
                    action_space, self.config["model"], framework="torch")
                self.model = ModelCatalog.get_model_v2(
                    obs_space=obs_space,
                    action_space=action_space,
                    num_outputs=logit_dim,
                    model_config=self.config["model"],
                    framework="torch")

            # Make sure, we passed in a correct Model factory.
            assert isinstance(self.model, TorchModelV2), \
                "ERROR: Generated Model must be a TorchModelV2 object!"

            TorchPolicy.__init__(
                self,
                observation_space=obs_space,
                action_space=action_space,
                config=config,
                model=self.model,
                loss=loss_fn,
                action_distribution_class=dist_class,
                action_sampler_fn=action_sampler_fn,
                action_distribution_fn=action_distribution_fn,
                max_seq_len=config["model"]["max_seq_len"],
                get_batch_divisibility_req=get_batch_divisibility_req,
            )

            self.view_requirements.update(
                self.model.inference_view_requirements)

            _before_loss_init = before_loss_init or after_init
            if _before_loss_init:
                _before_loss_init(self, self.observation_space,
                                  self.action_space, config)

            self._initialize_loss_from_dummy_batch(
                auto_remove_unneeded_view_reqs=True,
                stats_fn=stats_fn,
            )

            if _after_loss_init:
                _after_loss_init(self, obs_space, action_space, config)

            # Got to reset global_timestep again after this fake run-through.
            self.global_timestep = 0

        @override(Policy)
        def postprocess_trajectory(self,
                                   sample_batch,
                                   other_agent_batches=None,
                                   episode=None):
            # Do all post-processing always with no_grad().
            # Not using this here will introduce a memory leak (issue #6962).
            with torch.no_grad():
                # Call super's postprocess_trajectory first.
                sample_batch = super().postprocess_trajectory(
                    sample_batch, other_agent_batches, episode)
                if postprocess_fn:
                    return postprocess_fn(self, sample_batch,
                                          other_agent_batches, episode)

                return sample_batch

        @override(TorchPolicy)
        def extra_grad_process(self, optimizer, loss):
            """Called after optimizer.zero_grad() and loss.backward() calls.

            Allows for gradient processing before optimizer.step() is called.
            E.g. for gradient clipping.
            """
            if extra_grad_process_fn:
                return extra_grad_process_fn(self, optimizer, loss)
            else:
                return TorchPolicy.extra_grad_process(self, optimizer, loss)

        @override(TorchPolicy)
        def extra_compute_grad_fetches(self):
            if extra_learn_fetches_fn:
                fetches = convert_to_non_torch_type(
                    extra_learn_fetches_fn(self))
                # Auto-add empty learner stats dict if needed.
                return dict({LEARNER_STATS_KEY: {}}, **fetches)
            else:
                return TorchPolicy.extra_compute_grad_fetches(self)

        @override(TorchPolicy)
        def apply_gradients(self, gradients):
            if apply_gradients_fn:
                apply_gradients_fn(self, gradients)
            else:
                TorchPolicy.apply_gradients(self, gradients)

        @override(TorchPolicy)
        def extra_action_out(self, input_dict, state_batches, model,
                             action_dist):
            with torch.no_grad():
                if extra_action_out_fn:
                    stats_dict = extra_action_out_fn(
                        self, input_dict, state_batches, model, action_dist)
                else:
                    stats_dict = TorchPolicy.extra_action_out(
                        self, input_dict, state_batches, model, action_dist)
                return convert_to_non_torch_type(stats_dict)

        @override(TorchPolicy)
        def optimizer(self):
            if optimizer_fn:
                optimizers = optimizer_fn(self, self.config)
            else:
                optimizers = TorchPolicy.optimizer(self)
            optimizers = force_list(optimizers)
            if getattr(self, "exploration", None):
                optimizers = self.exploration.get_exploration_optimizer(
                    optimizers)
            return optimizers

        @override(TorchPolicy)
        def extra_grad_info(self, train_batch):
            with torch.no_grad():
                if stats_fn:
                    stats_dict = stats_fn(self, train_batch)
                else:
                    stats_dict = TorchPolicy.extra_grad_info(self, train_batch)
                return convert_to_non_torch_type(stats_dict)

    def with_updates(**overrides):
        """Allows creating a TorchPolicy cls based on settings of another one.

        Keyword Args:
            **overrides: The settings (passed into `build_torch_policy`) that
                should be different from the class that this method is called
                on.

        Returns:
            type: A new TorchPolicy sub-class.

        Examples:
        >> MySpecialDQNPolicyClass = DQNTorchPolicy.with_updates(
        ..    name="MySpecialDQNPolicyClass",
        ..    loss_function=[some_new_loss_function],
        .. )
        """
        return build_torch_policy(**dict(original_kwargs, **overrides))

    policy_cls.with_updates = staticmethod(with_updates)
    policy_cls.__name__ = name
    policy_cls.__qualname__ = name
    return policy_cls
