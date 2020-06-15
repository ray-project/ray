from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils import add_mixins
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import convert_to_non_torch_type

torch, _ = try_import_torch()


@DeveloperAPI
def build_torch_policy(name,
                       *,
                       loss_fn,
                       get_default_config=None,
                       stats_fn=None,
                       postprocess_fn=None,
                       extra_action_out_fn=None,
                       extra_grad_process_fn=None,
                       optimizer_fn=None,
                       before_init=None,
                       after_init=None,
                       action_sampler_fn=None,
                       action_distribution_fn=None,
                       make_model=None,
                       make_model_and_action_dist=None,
                       apply_gradients_fn=None,
                       mixins=None,
                       get_batch_divisibility_req=None):
    """Helper function for creating a torch policy class at runtime.

    Arguments:
        name (str): name of the policy (e.g., "PPOTorchPolicy")
        loss_fn (callable): Callable that returns a loss tensor as arguments
            given (policy, model, dist_class, train_batch).
        get_default_config (Optional[callable]): Optional callable that returns
            the default config to merge with any overrides.
        stats_fn (Optional[callable]): Optional callable that returns a dict of
            values given the policy and batch input tensors.
        postprocess_fn (Optional[callable]): Optional experience postprocessing
            function that takes the same args as
            Policy.postprocess_trajectory().
        extra_action_out_fn (Optional[callable]): Optional callable that
            returns a dict of extra values to include in experiences.
        extra_grad_process_fn (Optional[callable]): Optional callable that is
            called after gradients are computed and returns processing info.
        optimizer_fn (Optional[callable]): Optional callable that returns a
            torch optimizer given the policy and config.
        before_init (Optional[callable]): Optional callable to run at the
            beginning of `Policy.__init__` that takes the same arguments as
            the Policy constructor.
        after_init (Optional[callable]): Optional callable to run at the end of
            policy init that takes the same arguments as the policy
            constructor.
        action_sampler_fn (Optional[callable]): Optional callable returning a
            sampled action and its log-likelihood given some (obs and state)
            inputs.
        action_distribution_fn (Optional[callable]): A callable that takes
            the Policy, Model, the observation batch, an explore-flag, a
            timestep, and an is_training flag and returns a tuple of
            a) distribution inputs (parameters), b) a dist-class to generate
            an action distribution object from, and c) internal-state outputs
            (empty list if not applicable).
        make_model (Optional[callable]): Optional func that
            takes the same arguments as Policy.__init__ and returns a model
            instance. The distribution class will be determined automatically.
            Note: Only one of `make_model` or `make_model_and_action_dist`
            should be provided.
        make_model_and_action_dist (Optional[callable]): Optional func that
            takes the same arguments as Policy.__init__ and returns a tuple
            of model instance and torch action distribution class.
            Note: Only one of `make_model` or `make_model_and_action_dist`
            should be provided.
        apply_gradients_fn (Optional[callable]): Optional callable that
            takes a grads list and applies these to the Model's parameters.
        mixins (list): list of any class mixins for the returned policy class.
            These mixins will be applied in order and will have higher
            precedence than the TorchPolicy class.
        get_batch_divisibility_req (Optional[callable]): Optional callable that
            returns the divisibility requirement for sample batches.

    Returns:
        type: TorchPolicy child class constructed from the specified args.
    """

    original_kwargs = locals().copy()
    base = add_mixins(TorchPolicy, mixins)

    class policy_cls(base):
        def __init__(self, obs_space, action_space, config):
            if get_default_config:
                config = dict(get_default_config(), **config)
            self.config = config

            if before_init:
                before_init(self, obs_space, action_space, config)

            # Model is customized (use default action dist class).
            if make_model:
                assert make_model_and_action_dist is None
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
                    framework="torch",
                    **self.config["model"].get("custom_model_config", {}))

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

            if after_init:
                after_init(self, obs_space, action_space, config)

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
                    convert_to_non_torch_type(sample_batch),
                    convert_to_non_torch_type(other_agent_batches), episode)
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
                return optimizer_fn(self, self.config)
            else:
                return TorchPolicy.optimizer(self)

        @override(TorchPolicy)
        def extra_grad_info(self, train_batch):
            with torch.no_grad():
                if stats_fn:
                    stats_dict = stats_fn(self, train_batch)
                else:
                    stats_dict = TorchPolicy.extra_grad_info(self, train_batch)
                return convert_to_non_torch_type(stats_dict)

    def with_updates(**overrides):
        return build_torch_policy(**dict(original_kwargs, **overrides))

    policy_cls.with_updates = staticmethod(with_updates)
    policy_cls.__name__ = name
    policy_cls.__qualname__ = name
    return policy_cls
